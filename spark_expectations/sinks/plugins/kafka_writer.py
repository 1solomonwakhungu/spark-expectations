from typing import Any, Dict, Mapping, Optional, Set, Union
import hashlib
import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, lit, schema_of_json

from spark_expectations import _log
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks.plugins.base_writer import (
    SparkExpectationsSinkWriter,
    spark_expectations_writer_impl,
)

# Only values from these closed sets are ever written to logs or exception text.
# Every other Kafka option (JAAS config, endpoints, bootstrap servers, topic,
# custom user options) may carry credentials and is never rendered verbatim.
_SAFE_SECURITY_PROTOCOLS = frozenset({"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"})
_SAFE_SASL_MECHANISMS = frozenset({"PLAIN", "GSSAPI", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"})
_SAFE_CONSTANT_VALUES = frozenset({"true", "false"}) | _SAFE_SECURITY_PROTOCOLS | _SAFE_SASL_MECHANISMS

_REDACTED = "***REDACTED***"
# Values shorter than this are not scrubbed from error text: they cannot be
# meaningful credentials and replacing them would mangle unrelated text.
_MIN_REDACT_LENGTH = 4
# Matches `key="value"`, `key='value'` and `key=value` fragments, as used in JAAS
# configuration strings, so that embedded credentials are scrubbed even when an
# error message echoes only part of the option value.
_EMBEDDED_VALUE_PATTERN = re.compile(r"""=\s*(?:"([^"]*)"|'([^']*)'|([^\s;"']+))""")


def _allowlisted(value: Any, allowed: frozenset) -> str:
    if value is None:
        return "unset"
    return value if isinstance(value, str) and value in allowed else "other"


def _topic_fingerprint(topic: Any) -> str:
    """Return a short, non-reversible identifier for correlating a topic in logs."""
    if not topic:
        return "unset"
    return "sha256:" + hashlib.sha256(str(topic).encode("utf-8")).hexdigest()[:12]


def _describe_kafka_options(kafka_options: Optional[Mapping[str, Any]]) -> str:
    """Build a log-safe description of Kafka options from allowlisted metadata only."""
    options = kafka_options if isinstance(kafka_options, Mapping) else {}
    return (
        f"topic_fingerprint={_topic_fingerprint(options.get('topic'))}, "
        f"security_protocol={_allowlisted(options.get('kafka.security.protocol'), _SAFE_SECURITY_PROTOCOLS)}, "
        f"sasl_mechanism={_allowlisted(options.get('kafka.sasl.mechanism'), _SAFE_SASL_MECHANISMS)}, "
        f"bootstrap_servers_configured={bool(options.get('kafka.bootstrap.servers'))}, "
        f"option_count={len(options)}"
    )


def _sensitive_fragments(kafka_options: Optional[Mapping[str, Any]]) -> Set[str]:
    """Collect every option value, and every value embedded inside one, that must not be rendered."""
    fragments: Set[str] = set()
    if not isinstance(kafka_options, Mapping):
        return fragments
    for value in kafka_options.values():
        if value is None:
            continue
        text = str(value)
        candidates = [text]
        for match in _EMBEDDED_VALUE_PATTERN.finditer(text):
            candidates.extend(group for group in match.groups() if group)
        for candidate in candidates:
            candidate = candidate.strip()
            if len(candidate) >= _MIN_REDACT_LENGTH and candidate not in _SAFE_CONSTANT_VALUES:
                fragments.add(candidate)
    return fragments


def _redact(text: str, kafka_options: Optional[Mapping[str, Any]]) -> str:
    # Replace longest fragments first so a full option value is redacted before
    # any of its substrings, leaving no partial secret behind.
    for fragment in sorted(_sensitive_fragments(kafka_options), key=len, reverse=True):
        text = text.replace(fragment, _REDACTED)
    return text


class SparkExpectationsKafkaWritePluginImpl(SparkExpectationsSinkWriter):
    """
    class helps to write the stats data into the NSP
    """

    @spark_expectations_writer_impl
    def writer(self, _write_args: Dict[Union[str], Union[str, bool, Dict[str, str], DataFrame]]) -> None:
        """
        The functions helps to write data into the kafka topic
        Args:
            _write_args:

        Returns:

        """

        transport = _write_args.get("transport")
        if transport not in (None, "kafka_native"):
            return

        failure_message: Optional[str] = None
        try:
            if _write_args.pop("enable_se_streaming"):
                _log.info("started write stats data into kafka stats topic")

                df: DataFrame = _write_args.get("stats_df")
                kafka_options = _write_args.get("kafka_write_options")

                _log.info(f"Writing to Kafka ({_describe_kafka_options(kafka_options)})")

                # Convert se_job_metadata from JSON string to a proper struct
                # so it appears as a nested object in the Kafka JSON output
                # instead of a double-escaped string
                if "se_job_metadata" in df.columns:
                    metadata_sample = df.select("se_job_metadata").first()[0]
                    if metadata_sample:
                        metadata_schema = schema_of_json(lit(metadata_sample))
                        df = df.withColumn(
                            "se_job_metadata",
                            from_json(col("se_job_metadata"), metadata_schema),
                        )

                df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").mode("append").options(
                    **kafka_options
                ).save()

                _log.info("ended writing stats data into kafka stats topic")

        except Exception as e:  # pylint: disable=broad-except
            kafka_options = _write_args.get("kafka_write_options")
            failure_message = (
                f"Failed to write to Kafka ({_describe_kafka_options(kafka_options)}). "
                f"Error: {type(e).__name__}: {_redact(str(e), kafka_options)}"
            )
            _log.error(f"Kafka write failure: {failure_message}")

        if failure_message is not None:
            # Raised outside the except block so the original exception, whose text
            # and traceback may contain Kafka option values, is not attached as
            # __context__ or __cause__ and cannot leak through chained tracebacks.
            raise SparkExpectationsMiscException(f"error occurred while saving data into kafka: {failure_message}")
