import json
from typing import Any, Callable, Dict, Iterator, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from spark_expectations import _log
from spark_expectations.config.rest_streaming_defaults import (
    DEFAULT_REST_API_VERSION,
    DEFAULT_REST_BACKOFF_FACTOR,
    DEFAULT_REST_EMBEDDED_FORMAT,
    DEFAULT_REST_MAX_RETRIES,
    DEFAULT_REST_POOL_CONNECTIONS,
    DEFAULT_REST_POOL_MAXSIZE,
    DEFAULT_REST_TIMEOUT_SEC,
)
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.base_writer import (
    SparkExpectationsSinkWriter,
    spark_expectations_writer_impl,
)

_RETRYABLE_STATUS = (408, 429, 500, 502, 503, 504)

# Upper bound on how long a single ``Retry-After`` header can make the driver sleep.
# Without a cap, a proxy (or anything in front of it) can stall the job for as long as
# it likes. Override with ``rest_write_options["max_retry_after_sec"]``.
DEFAULT_REST_MAX_RETRY_AFTER_SEC = 30.0

# Upper bound on the size of a single produce request body. Each stats row is sent as
# one request, so this is also the per-record limit. 1 MiB matches the Kafka broker
# default ``message.max.bytes``. Override with ``rest_write_options["max_record_bytes"]``.
DEFAULT_REST_MAX_RECORD_BYTES = 1024 * 1024

# Only v2 + json are supported today. When v3 (or another format) support is
# introduced, extend these sets AND add the corresponding branch in the helpers
# below along with the required configuration (e.g. cluster_id for v3).
_SUPPORTED_API_VERSIONS = frozenset({"2"})
_SUPPORTED_EMBEDDED_FORMATS = frozenset({"json"})


def _normalize_api_version(api_version: Any) -> str:
    """Validate + normalize the REST proxy API version.

    Only ``v2`` is accepted. Any other value (including ``v3``) raises
    :class:`SparkExpectationsMiscException`.
    """
    version = str(api_version).strip().lower()
    if version.startswith("v"):
        version = version[1:]
    if version not in _SUPPORTED_API_VERSIONS:
        raise SparkExpectationsMiscException(
            f"unsupported kafka REST api_version '{api_version}'; only 'v2' is supported"
        )
    return f"v{version}"


def _normalize_embedded_format(embedded_format: Any) -> str:
    """Validate + normalize the REST proxy embedded format.

    Only ``json`` is accepted today. Any other value raises
    :class:`SparkExpectationsMiscException`.
    """
    fmt = str(embedded_format).strip().lower()
    if fmt not in _SUPPORTED_EMBEDDED_FORMATS:
        raise SparkExpectationsMiscException(
            f"unsupported kafka REST embedded_format '{embedded_format}'; only 'json' is supported"
        )
    return fmt


def _build_rest_headers(api_version: str, embedded_format: str) -> Dict[str, str]:
    """Build Confluent Kafka REST v2 Accept / Content-Type headers for JSON payloads."""
    version = _normalize_api_version(api_version)
    _normalize_embedded_format(embedded_format)
    return {
        "Content-Type": f"application/vnd.kafka.json.{version}+json",
        "Accept": f"application/vnd.kafka.{version}+json",
    }


def _build_topic_url(base_url: str, topic: str, api_version: str) -> str:
    """Build the v2 topic-produce URL: ``{base}/topics/{topic}``."""
    _normalize_api_version(api_version)
    base = str(base_url).rstrip("/")
    return f"{base}/topics/{topic}"


def _build_record_payload(raw_json: str, api_version: str, embedded_format: str) -> bytes:
    """Wrap a single JSON row as a Kafka REST v2 records payload."""
    _normalize_api_version(api_version)
    _normalize_embedded_format(embedded_format)
    body: Dict[str, Any] = {"records": [{"value": json.loads(raw_json)}]}
    return json.dumps(body).encode("utf-8")


def _resolve_publish_context(rest_options: Dict[str, Any]) -> Tuple[str, Dict[str, str], Callable[[str], bytes]]:
    api_version = rest_options.get("api_version", DEFAULT_REST_API_VERSION)
    embedded_format = rest_options.get("embedded_format", DEFAULT_REST_EMBEDDED_FORMAT)
    base_url = rest_options["base_url"]
    topic = rest_options["topic"]

    url = _build_topic_url(base_url, topic, api_version)
    headers = _build_rest_headers(api_version, embedded_format)

    def build_payload(raw_json: str) -> bytes:
        return _build_record_payload(raw_json, api_version, embedded_format)

    return url, headers, build_payload


class _BoundedRetryAfterRetry(Retry):
    """``Retry`` that honours ``Retry-After`` but never sleeps longer than ``max_retry_after``."""

    def __init__(self, *args: Any, max_retry_after: float = DEFAULT_REST_MAX_RETRY_AFTER_SEC, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.max_retry_after = float(max_retry_after)

    def new(self, **kw: Any) -> "_BoundedRetryAfterRetry":
        kw.setdefault("max_retry_after", self.max_retry_after)
        return super().new(**kw)  # type: ignore[return-value]

    def get_retry_after(self, response: Any) -> Optional[float]:
        retry_after = super().get_retry_after(response)
        if retry_after is None:
            return None
        return max(0.0, min(float(retry_after), self.max_retry_after))


def _positive_number_option(rest_options: Dict[str, Any], key: str, default: float, allow_zero: bool) -> float:
    raw = rest_options.get(key, default)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = -1.0
    if value < 0 or (value == 0 and not allow_zero) or value != value:  # value != value catches NaN
        raise SparkExpectationsMiscException(
            f"invalid kafka REST option '{key}': expected a {'non-negative' if allow_zero else 'positive'} number"
        )
    return value


def _resolve_max_retry_after(rest_options: Dict[str, Any]) -> float:
    return _positive_number_option(rest_options, "max_retry_after_sec", DEFAULT_REST_MAX_RETRY_AFTER_SEC, True)


def _resolve_max_record_bytes(rest_options: Dict[str, Any]) -> int:
    return int(_positive_number_option(rest_options, "max_record_bytes", DEFAULT_REST_MAX_RECORD_BYTES, False))


def _build_session(rest_options: Dict[str, Any]) -> requests.Session:
    """Build a ``requests.Session`` with retry + connection pooling for the Kafka REST proxy.

    Retries cover network errors AND retryable HTTP statuses (408/429/5xx). ``POST`` is included
    in ``allowed_methods`` explicitly because urllib3 does not retry non-idempotent verbs by default.

    Delivery is therefore **at-least-once**: if the proxy commits a record and the response is
    lost (read timeout, connection reset, or a 5xx emitted after the write), the retry produces
    the record again and consumers see a duplicate. Set ``max_retries`` to ``0`` to disable
    retries; a failure is then reported to the caller instead of being retried.

    ``Retry-After`` is honoured but capped at ``max_retry_after_sec`` per attempt.
    """
    max_retries = int(rest_options.get("max_retries", DEFAULT_REST_MAX_RETRIES))
    retry = _BoundedRetryAfterRetry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=float(rest_options.get("backoff_factor", DEFAULT_REST_BACKOFF_FACTOR)),
        status_forcelist=_RETRYABLE_STATUS,
        allowed_methods=frozenset(["POST"]),
        respect_retry_after_header=True,
        raise_on_status=False,
        max_retry_after=_resolve_max_retry_after(rest_options),
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=int(rest_options.get("pool_connections", DEFAULT_REST_POOL_CONNECTIONS)),
        pool_maxsize=int(rest_options.get("pool_maxsize", DEFAULT_REST_POOL_MAXSIZE)),
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _resolve_timeout(rest_options: Dict[str, Any]) -> Tuple[float, float]:
    """Resolve request timeout as a ``(connect, read)`` tuple.

    Accepts either the legacy scalar ``timeout_sec`` (applied to both) or explicit
    ``connect_timeout_sec`` / ``read_timeout_sec`` overrides.
    """
    scalar = float(rest_options.get("timeout_sec", DEFAULT_REST_TIMEOUT_SEC))
    connect_timeout = float(rest_options.get("connect_timeout_sec", scalar))
    read_timeout = float(rest_options.get("read_timeout_sec", scalar))
    return (connect_timeout, read_timeout)


def _retry_count_from_response(response: requests.Response) -> int:
    """Best-effort extraction of the number of retries urllib3 performed for this request."""
    raw = getattr(response, "raw", None)
    retries = getattr(raw, "retries", None)
    if retries is None:
        return 0
    history = getattr(retries, "history", ()) or ()
    return len(history)


def _iter_stats_json(stats_df: Any) -> Iterator[str]:
    """Yield one JSON string per stats row without collecting the whole DataFrame.

    ``toLocalIterator`` pulls one partition at a time to the driver, so driver memory is
    bounded by the largest partition rather than by the full result. It does not bound
    the size of an individual partition.
    """
    for row in stats_df.selectExpr("to_json(struct(*)) AS value").toLocalIterator():
        yield row["value"]


def _delivery_failure(
    reason: str, record_number: int, acknowledged: int, attempted: bool = True
) -> SparkExpectationsMiscException:
    """Build a failure message that states what is known about delivery, without echoing
    the endpoint URL, response body, payload or raw exception text."""
    if attempted:
        state = (
            f"stats record #{record_number} was not acknowledged and may still have been written "
            f"if an earlier attempt's response was lost"
        )
    else:
        state = f"stats record #{record_number} was not sent"
    return SparkExpectationsMiscException(
        f"{reason}; {state}. {acknowledged} earlier record(s) were acknowledged and later records "
        f"were not sent. Delivery is at-least-once: re-running the job re-sends acknowledged records."
    )


class SparkExpectationsKafkaRestWritePluginImpl(SparkExpectationsSinkWriter):
    @spark_expectations_writer_impl
    def writer(self, _write_args: Dict[str, Any]) -> None:
        transport = _write_args.get("transport")
        if transport != "kafka_rest":
            return

        if not _write_args.get("enable_se_streaming"):
            return

        rest_options: Dict[str, Any] = _write_args.get("rest_write_options") or {}
        base_url = rest_options.get("base_url")
        topic = rest_options.get("topic")
        if not base_url or not topic:
            raise SparkExpectationsMiscException(
                "error occurred while saving data into kafka (REST): "
                "'base_url' and 'topic' are required in rest_write_options"
            )

        timeout = _resolve_timeout(rest_options)
        verify = bool(rest_options.get("verify_ssl", True))
        max_record_bytes = _resolve_max_record_bytes(rest_options)
        url, headers, build_payload = _resolve_publish_context(rest_options)

        stats_df = _write_args.get("stats_df")
        session = _build_session(rest_options)
        acknowledged = 0
        total_bytes_sent = 0
        total_retries = 0
        api_version = rest_options.get("api_version", DEFAULT_REST_API_VERSION)
        embedded_format = rest_options.get("embedded_format", DEFAULT_REST_EMBEDDED_FORMAT)
        _log.info(
            f"started write stats data into kafka REST proxy topic: {topic} "
            f"(api_version={api_version}, embedded_format={embedded_format}, "
            f"connect_timeout={timeout[0]}s, read_timeout={timeout[1]}s, max_record_bytes={max_record_bytes})"
        )
        try:
            for record_number, raw_json in enumerate(_iter_stats_json(stats_df), start=1):
                payload_bytes = build_payload(raw_json)
                if len(payload_bytes) > max_record_bytes:
                    raise _delivery_failure(
                        f"kafka REST request body for topic '{topic}' is {len(payload_bytes)} bytes, "
                        f"over max_record_bytes={max_record_bytes}",
                        record_number,
                        acknowledged,
                        attempted=False,
                    )
                try:
                    response = session.post(
                        url,
                        data=payload_bytes,
                        headers=headers,
                        timeout=timeout,
                        verify=verify,
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    # Raw exception text from requests/urllib3 can carry the proxy URL; report the
                    # type only and do not chain the original exception into the traceback.
                    raise _delivery_failure(
                        f"error occurred while saving data into kafka (REST) topic '{topic}': "
                        f"{type(exc).__name__}",
                        record_number,
                        acknowledged,
                    ) from None

                row_retries = _retry_count_from_response(response)
                total_retries += row_retries
                if row_retries:
                    _log.warning(
                        f"kafka REST proxy request to topic '{topic}' retried {row_retries} "
                        f"time(s) before status {response.status_code}"
                    )

                reason = self._record_error_reason(response, topic)
                if reason is not None:
                    raise _delivery_failure(reason, record_number, acknowledged)
                acknowledged += 1
                total_bytes_sent += len(payload_bytes)
        finally:
            session.close()
            _log.info(
                f"ended writing stats data into kafka REST proxy topic: {topic} "
                f"(acknowledged_rows={acknowledged}, bytes_acknowledged={total_bytes_sent}, "
                f"total_retries={total_retries})"
            )

    @staticmethod
    def _raise_for_record_errors(response: requests.Response, topic: str, base_url: str = "") -> None:
        """Raise if ``response`` reports a failure. ``base_url`` is accepted for compatibility and
        is intentionally not included in the message."""
        del base_url
        reason = SparkExpectationsKafkaRestWritePluginImpl._record_error_reason(response, topic)
        if reason is not None:
            raise SparkExpectationsMiscException(reason)

    @staticmethod
    def _record_error_reason(response: requests.Response, topic: str) -> Optional[str]:
        """Return a safe failure reason for a non-successful produce response, else ``None``.

        Only the HTTP status and integer ``error_code`` values are reported. The response body
        and proxy-supplied ``message`` fields are not echoed.
        """
        if response.status_code >= 400:
            return f"REST proxy HTTP {response.status_code} for topic '{topic}'"
        payload: Any = {}
        try:
            if response.content:
                payload = response.json()
        except ValueError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        offsets = payload.get("offsets") or []
        codes = [o.get("error_code") for o in offsets if isinstance(o, dict) and o.get("error_code") is not None]
        if payload.get("error_code") is not None:
            codes.insert(0, payload.get("error_code"))
        if codes:
            safe_codes = ", ".join(str(c) if isinstance(c, int) else "non-integer" for c in codes)
            return f"REST proxy record error for topic '{topic}' (error_code={safe_codes})"
        return None
