from typing import Any, Dict, Mapping, Optional, Union
import hashlib

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

def _allowlisted(value: Any, allowed: frozenset) -> str:
    if value is None:
        return "unset"
    return value if isinstance(value, str) and value in allowed else "other"


def _topic_fingerprint(topic: Any) -> str:
    """Return a short identifier for correlating a topic in logs."""
    if not topic:
        return "unset"
    return "sha256:" + hashlib.sha256(str(topic).encode("utf-8")).hexdigest()[:12]


def _describe_kafka_options(kafka_options: Any) -> str:
    """Build a log-safe description of Kafka options from allowlisted metadata only."""
    options = kafka_options if isinstance(kafka_options, Mapping) else {}
    return (
        f"topic_fingerprint={_topic_fingerprint(options.get('topic'))}, "
        f"security_protocol={_allowlisted(options.get('kafka.security.protocol'), _SAFE_SECURITY_PROTOCOLS)}, "
        f"sasl_mechanism={_allowlisted(options.get('kafka.sasl.mechanism'), _SAFE_SASL_MECHANISMS)}, "
        f"bootstrap_servers_configured={bool(options.get('kafka.bootstrap.servers'))}, "
        f"option_count={len(options)}"
    )


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

        except Exception:  # pylint: disable=broad-except
            # Downstream errors may echo transformed credentials or row contents.
            # Do not render them or attach their traceback to the public error.
            failure_message = "Failed to write to Kafka. Downstream error details withheld."
            _log.error(f"Kafka write failure: {failure_message}")

        if failure_message is not None:
            # Raised outside the except block so the original exception, whose text
            # and traceback may contain Kafka option values, is not attached as
            # ordinary __context__ or __cause__. Suppress any ambient caller context
            # in default traceback formatting as well.
            raise SparkExpectationsMiscException(f"error occurred while saving data into kafka: {failure_message}") from None
