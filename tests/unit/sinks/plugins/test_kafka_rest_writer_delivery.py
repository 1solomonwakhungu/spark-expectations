"""Delivery-semantics and diagnostics regressions for the Kafka REST writer.

All HTTP traffic goes to a ``ThreadingHTTPServer`` bound to 127.0.0.1 or to mocks.
Credentials are synthetic.
"""

import json
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch

import pytest

from spark_expectations import _log
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.kafka_rest_writer import (
    DEFAULT_REST_MAX_RECORD_BYTES,
    DEFAULT_REST_MAX_RETRY_AFTER_SEC,
    SparkExpectationsKafkaRestWritePluginImpl,
    _BoundedRetryAfterRetry,
    _build_session,
)

SYNTHETIC_SECRET = "synthetic-secret-7f3a"
BODY_MARKER = "proxy-body-marker-91c2"
OK_BODY = json.dumps({"offsets": [{"partition": 0, "offset": 1, "error_code": None}]})


@pytest.fixture(autouse=True)
def capture_package_logs(caplog):
    # Suite fixtures can disable propagation; capture the actual package logger.
    with patch.object(_log, "handlers", [*_log.handlers, caplog.handler]):
        with caplog.at_level("DEBUG", logger=_log.name):
            yield


class _Proxy:
    """Loopback stand-in for a REST proxy. ``plan`` entries are consumed one per request."""

    def __init__(self, plan):
        self.plan = list(plan)
        self.bodies = []
        self._lock = threading.Lock()
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):  # silence stderr
                pass

            def do_POST(self):  # noqa: N802
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                with proxy._lock:
                    proxy.bodies.append(json.loads(body))
                    action = proxy.plan.pop(0) if proxy.plan else {}
                if action.get("drop"):
                    # Request was received ("committed") but the response never arrives.
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True
                    return
                out = action.get("body", OK_BODY).encode("utf-8")
                self.send_response(action.get("status", 200))
                for key, value in action.get("headers", {}).items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(out)))
                self.end_headers()
                self.wfile.write(out)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def port(self):
        return self.server.server_address[1]

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture(autouse=True)
def _no_proxy_env(monkeypatch):
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1")


def _stats_df(rows):
    df = MagicMock()
    projected = MagicMock()
    df.selectExpr.return_value = projected
    projected.toLocalIterator.side_effect = lambda *a, **k: iter([{"value": json.dumps(r)} for r in rows])
    projected.collect.side_effect = AssertionError("collect() must not be used")
    return df, projected


def _args(port, rows, **options):
    df, projected = _stats_df(rows)
    rest = {
        "base_url": f"http://svc:{SYNTHETIC_SECRET}@127.0.0.1:{port}",
        "topic": "dq-stats",
        "backoff_factor": 0,
        "timeout_sec": 5,
    }
    rest.update(options)
    args = {"enable_se_streaming": True, "transport": "kafka_rest", "stats_df": df, "rest_write_options": rest}
    return args, projected


def _plugin_log_text(caplog):
    """Text logged by this package (urllib3's own connection DEBUG lines are out of scope)."""
    return "\n".join(r.getMessage() for r in caplog.records if r.name.startswith("spark_expectations"))


def _write(args):
    SparkExpectationsKafkaRestWritePluginImpl().writer(_write_args=args)


# ---------------------------------------------------------------------------
# Bounded driver iteration
# ---------------------------------------------------------------------------


def test_rows_are_streamed_with_tolocaliterator_not_collect():
    with _Proxy([]) as proxy:
        args, projected = _args(proxy.port, [{"i": i} for i in range(4)])
        _write(args)
    projected.toLocalIterator.assert_called_once_with()
    projected.collect.assert_not_called()
    assert [b["records"][0]["value"]["i"] for b in proxy.bodies] == [0, 1, 2, 3]


def test_rows_are_consumed_lazily_and_stop_at_first_failure():
    consumed = []

    def rows():
        for i in range(1000):
            consumed.append(i)
            yield {"value": json.dumps({"i": i})}

    df = MagicMock()
    df.selectExpr.return_value.toLocalIterator.return_value = rows()
    with _Proxy([{}, {"status": 400, "body": BODY_MARKER}]) as proxy:
        args, _ = _args(proxy.port, [], max_retries=0)
        args["stats_df"] = df
        with pytest.raises(SparkExpectationsMiscException):
            _write(args)
    # Only the rows needed so far were pulled from the iterator, not all 1000.
    assert consumed == [0, 1]
    assert len(proxy.bodies) == 2


def test_oversized_record_is_rejected_before_sending():
    with _Proxy([]) as proxy:
        args, _ = _args(proxy.port, [{"i": 0}, {"blob": "x" * 500}, {"i": 2}], max_record_bytes=200)
        with pytest.raises(SparkExpectationsMiscException) as excinfo:
            _write(args)
    msg = str(excinfo.value)
    assert "over max_record_bytes=200" in msg
    assert "stats record #2 was not sent" in msg
    assert "1 earlier record(s) were acknowledged" in msg
    assert len(proxy.bodies) == 1


def test_default_max_record_bytes_is_one_mib():
    assert DEFAULT_REST_MAX_RECORD_BYTES == 1024 * 1024


@pytest.mark.parametrize(
    "key,value",
    [
        ("max_record_bytes", 0),
        ("max_record_bytes", "abc"),
        ("max_retry_after_sec", -1),
        ("max_retry_after_sec", float("nan")),
    ],
)
def test_invalid_limit_options_are_rejected(key, value):
    args, _ = _args(1, [{"i": 0}], **{key: value})
    with pytest.raises(SparkExpectationsMiscException, match=f"invalid kafka REST option '{key}'"):
        _write(args)


# ---------------------------------------------------------------------------
# At-least-once retries
# ---------------------------------------------------------------------------


def test_lost_response_is_retried_and_produces_duplicate():
    """Documents the at-least-once contract: the proxy saw the record twice."""
    with _Proxy([{"drop": True}]) as proxy:
        args, _ = _args(proxy.port, [{"id": 1}], max_retries=1)
        _write(args)
    assert len(proxy.bodies) == 2
    assert proxy.bodies[0] == proxy.bodies[1]


def test_max_retries_zero_sends_once_and_reports_unknown_state():
    with _Proxy([{"drop": True}]) as proxy:
        args, _ = _args(proxy.port, [{"id": 1}], max_retries=0)
        with pytest.raises(SparkExpectationsMiscException) as excinfo:
            _write(args)
    assert len(proxy.bodies) == 1
    msg = str(excinfo.value)
    assert "ConnectionError" in msg
    assert "may still have been written" in msg
    assert "at-least-once" in msg


def test_retryable_status_then_success_sends_each_record_once_more():
    with _Proxy([{"status": 503}, {}, {}]) as proxy:
        args, _ = _args(proxy.port, [{"id": 1}, {"id": 2}], max_retries=2)
        _write(args)
    assert [b["records"][0]["value"]["id"] for b in proxy.bodies] == [1, 1, 2]


# ---------------------------------------------------------------------------
# Partial failure and safe diagnostics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "action,expected",
    [
        ({"status": 500, "body": BODY_MARKER}, "REST proxy HTTP 500 for topic 'dq-stats'"),
        ({"status": 422, "body": BODY_MARKER}, "REST proxy HTTP 422 for topic 'dq-stats'"),
        (
            {"body": json.dumps({"offsets": [{"error_code": 40403, "message": BODY_MARKER}]})},
            "REST proxy record error for topic 'dq-stats' (error_code=40403)",
        ),
        (
            {"body": json.dumps({"error_code": 50002, "message": BODY_MARKER})},
            "REST proxy record error for topic 'dq-stats' (error_code=50002)",
        ),
        (
            {"body": json.dumps({"offsets": [{"error_code": BODY_MARKER}]})},
            "REST proxy record error for topic 'dq-stats' (error_code=non-integer)",
        ),
    ],
)
def test_partial_failure_reports_progress_without_url_or_body(action, expected, caplog):
    with _Proxy([{}, action]) as proxy:
        args, _ = _args(proxy.port, [{"id": 1}, {"id": 2}, {"id": 3}], max_retries=0)
        with caplog.at_level("DEBUG"):
            with pytest.raises(SparkExpectationsMiscException) as excinfo:
                _write(args)
    msg = str(excinfo.value)
    assert expected in msg
    assert "stats record #2 was not acknowledged" in msg
    assert "1 earlier record(s) were acknowledged and later records were not sent" in msg
    assert len(proxy.bodies) == 2  # record #3 never sent
    assert "started write stats data" in _plugin_log_text(caplog)  # logs were captured
    for text in (msg, _plugin_log_text(caplog)):
        assert SYNTHETIC_SECRET not in text
        assert "127.0.0.1" not in text
        assert BODY_MARKER not in text
        assert '"id"' not in text  # payload is not echoed
    assert excinfo.value.__cause__ is None


def test_connection_refused_does_not_leak_url_or_chain(caplog):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    args, _ = _args(port, [{"id": 1}], max_retries=0)
    with caplog.at_level("DEBUG"):
        with pytest.raises(SparkExpectationsMiscException) as excinfo:
            _write(args)
    assert "ConnectionError" in str(excinfo.value)
    assert excinfo.value.__cause__ is None and excinfo.value.__suppress_context__
    for text in (str(excinfo.value), _plugin_log_text(caplog)):
        assert SYNTHETIC_SECRET not in text
        assert str(port) not in text


def test_raise_for_record_errors_compat_omits_base_url():
    resp = MagicMock(status_code=502, text=BODY_MARKER, content=b"")
    with pytest.raises(SparkExpectationsMiscException) as excinfo:
        SparkExpectationsKafkaRestWritePluginImpl._raise_for_record_errors(
            resp, "dq-stats", f"https://u:{SYNTHETIC_SECRET}@proxy.invalid"
        )
    assert str(excinfo.value) == "REST proxy HTTP 502 for topic 'dq-stats'"


# ---------------------------------------------------------------------------
# Retry-After
# ---------------------------------------------------------------------------


def test_retry_after_is_capped_by_max_retry_after_sec():
    plan = [{"status": 429, "headers": {"Retry-After": "3600"}}, {}]
    with _Proxy(plan) as proxy:
        args, _ = _args(proxy.port, [{"id": 1}], max_retries=1, max_retry_after_sec=0.05)
        started = time.monotonic()
        _write(args)
        elapsed = time.monotonic() - started
    assert len(proxy.bodies) == 2
    assert elapsed < 3.0


def test_retry_after_short_value_is_honoured():
    plan = [{"status": 503, "headers": {"Retry-After": "1"}}, {}]
    with _Proxy(plan) as proxy:
        args, _ = _args(proxy.port, [{"id": 1}], max_retries=1, max_retry_after_sec=5)
        started = time.monotonic()
        _write(args)
        elapsed = time.monotonic() - started
    assert len(proxy.bodies) == 2
    assert 0.9 <= elapsed < 4.0


def test_retry_after_exhausted_reports_status():
    plan = [{"status": 429, "headers": {"Retry-After": "3600"}}] * 2
    with _Proxy(plan) as proxy:
        args, _ = _args(proxy.port, [{"id": 1}], max_retries=1, max_retry_after_sec=0)
        with pytest.raises(SparkExpectationsMiscException, match="REST proxy HTTP 429"):
            _write(args)
    assert len(proxy.bodies) == 2


@pytest.mark.parametrize("header,cap,expected", [("3600", 2.0, 2.0), ("1", 2.0, 1.0), ("0", 2.0, 0.0)])
def test_bounded_retry_get_retry_after(header, cap, expected):
    retry = _BoundedRetryAfterRetry(total=1, max_retry_after=cap)
    response = MagicMock()
    response.headers = {"Retry-After": header}
    assert retry.get_retry_after(response) == expected


def test_bounded_retry_cap_survives_increment():
    retry = _BoundedRetryAfterRetry(total=3, max_retry_after=1.5)
    assert retry.new(total=2).max_retry_after == 1.5


def test_build_session_uses_bounded_retry_with_default_cap():
    session = _build_session({})
    retry = session.get_adapter("https://proxy.invalid").max_retries
    assert isinstance(retry, _BoundedRetryAfterRetry)
    assert retry.max_retry_after == DEFAULT_REST_MAX_RETRY_AFTER_SEC
    assert "POST" in retry.allowed_methods
    session.close()
