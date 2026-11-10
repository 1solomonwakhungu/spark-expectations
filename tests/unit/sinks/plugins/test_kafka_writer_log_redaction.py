"""Regression tests: Kafka write options must never reach logs or exception text.

Every credential-bearing value is a unique synthetic sentinel. No Kafka broker,
Spark session, identity provider or real secret backend is contacted.
"""

from typing import Any, Dict, List, Optional
import io
import logging
import traceback
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark_expectations import _log
from spark_expectations.core.exceptions import SparkExpectationsMiscException
from spark_expectations.sinks import _sink_hook
from spark_expectations.sinks.plugins.kafka_writer import SparkExpectationsKafkaWritePluginImpl
from spark_expectations.sinks.utils import writer as writer_module
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter

CLIENT_ID = "SENTINEL_CLIENT_ID_7f3a"
CLIENT_SECRET = "SENTINEL_CLIENT_SECRET_91bd"
TOKEN_ENDPOINT = "https://SENTINEL_TOKEN_ENDPOINT_44c2.invalid/oauth2/token"
BOOTSTRAP = "SENTINEL_BOOTSTRAP_USER_0e5d:SENTINEL_BOOTSTRAP_PASS_a8f1@broker.invalid:9093"
TOPIC = "SENTINEL_TOPIC_63e0"
CUSTOM_OPTION_VALUE = "SENTINEL_CUSTOM_OPTION_d27b"

SENTINELS = [
    CLIENT_ID,
    CLIENT_SECRET,
    "SENTINEL_TOKEN_ENDPOINT_44c2",
    "SENTINEL_BOOTSTRAP_USER_0e5d",
    "SENTINEL_BOOTSTRAP_PASS_a8f1",
    TOPIC,
    CUSTOM_OPTION_VALUE,
]


def _sensitive_options() -> Dict[str, str]:
    return {
        "kafka.bootstrap.servers": BOOTSTRAP,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "OAUTHBEARER",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required "
            f'clientId="{CLIENT_ID}" clientSecret="{CLIENT_SECRET}";'
        ),
        "kafka.sasl.oauthbearer.token.endpoint.url": TOKEN_ENDPOINT,
        "kafka.custom.unknown.option": CUSTOM_OPTION_VALUE,
        "topic": TOPIC,
    }


class _RecordingWriteBuilder:
    def __init__(self, error: Optional[Exception] = None) -> None:
        self.error = error
        self.received_options: Optional[Dict[str, Any]] = None
        self.format_name: Optional[str] = None
        self.mode_name: Optional[str] = None

    def format(self, value: str) -> "_RecordingWriteBuilder":
        self.format_name = value
        return self

    def mode(self, value: str) -> "_RecordingWriteBuilder":
        self.mode_name = value
        return self

    def options(self, **values: Any) -> "_RecordingWriteBuilder":
        self.received_options = dict(values)
        return self

    def save(self) -> None:
        if self.error is not None:
            raise self.error


class _FakeDataFrame:
    columns: List[str] = []

    def __init__(self, builder: _RecordingWriteBuilder) -> None:
        self.write = builder

    def selectExpr(self, _expression: str) -> "_FakeDataFrame":
        return self

    def __repr__(self) -> str:
        return f"_FakeDataFrame(secret={CLIENT_SECRET})"


def _echoing_error(options: Dict[str, str]) -> Exception:
    """Simulate a sink error whose text echoes option values and a bare JAAS fragment."""
    endpoint = options.get("kafka.sasl.oauthbearer.token.endpoint.url", "")
    return RuntimeError(
        f"Kafka producer failed for options {options!r}; "
        f"login failed for clientSecret {CLIENT_SECRET} at {endpoint}; "
        f"topic {options['topic']} not present in metadata after 60000 ms"
    )


@pytest.fixture(name="captured_log", params=[logging.INFO, logging.DEBUG], ids=["INFO", "DEBUG"])
def fixture_captured_log(request):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    old_handlers = list(_log.handlers)
    old_level = _log.level
    _log.handlers[:] = [handler]
    _log.setLevel(request.param)
    try:
        yield stream
    finally:
        _log.handlers[:] = old_handlers
        _log.setLevel(old_level)


def _assert_no_sentinels(text: str) -> None:
    for sentinel in SENTINELS:
        assert sentinel not in text, f"sentinel {sentinel!r} leaked into: {text}"


def _full_exception_text(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


def test_success_logs_only_allowlisted_metadata_and_passes_options_unchanged(captured_log):
    options = _sensitive_options()
    expected = dict(options)
    builder = _RecordingWriteBuilder()

    SparkExpectationsKafkaWritePluginImpl().writer(
        _write_args={
            "enable_se_streaming": True,
            "stats_df": _FakeDataFrame(builder),
            "kafka_write_options": options,
        }
    )

    assert builder.received_options == expected
    assert builder.format_name == "kafka"
    assert builder.mode_name == "append"
    rendered = captured_log.getvalue()
    _assert_no_sentinels(rendered)
    assert "jaas" not in rendered.lower()
    assert "kafka.custom.unknown.option" not in rendered
    assert "security_protocol=SASL_SSL" in rendered
    assert "sasl_mechanism=OAUTHBEARER" in rendered
    assert "topic_fingerprint=sha256:" in rendered


def test_failure_scrubs_logs_exception_text_and_chain(captured_log):
    options = _sensitive_options()
    expected = dict(options)
    builder = _RecordingWriteBuilder(error=_echoing_error(options))

    with pytest.raises(SparkExpectationsMiscException) as exc_info:
        SparkExpectationsKafkaWritePluginImpl().writer(
            _write_args={
                "enable_se_streaming": True,
                "stats_df": _FakeDataFrame(builder),
                "kafka_write_options": options,
            }
        )

    assert builder.received_options == expected
    exc = exc_info.value
    assert exc.__cause__ is None
    assert exc.__context__ is None
    message = str(exc)
    assert message.startswith("error occurred while saving data into kafka: Failed to write to Kafka")
    assert "RuntimeError" not in message
    assert "not present in metadata after 60000 ms" not in message
    _assert_no_sentinels(message)
    _assert_no_sentinels(_full_exception_text(exc))
    _assert_no_sentinels(captured_log.getvalue())


def test_failure_through_plugin_hook_scrubs_exception(captured_log):
    options = _sensitive_options()
    builder = _RecordingWriteBuilder(error=_echoing_error(options))

    with pytest.raises(SparkExpectationsMiscException) as exc_info:
        _sink_hook.writer(
            _write_args={
                "enable_se_streaming": True,
                "stats_df": _FakeDataFrame(builder),
                "kafka_write_options": options,
            }
        )

    _assert_no_sentinels(_full_exception_text(exc_info.value))
    _assert_no_sentinels(captured_log.getvalue())


def test_unknown_security_values_are_not_echoed(captured_log):
    options = {
        "kafka.security.protocol": CUSTOM_OPTION_VALUE,
        "kafka.sasl.mechanism": CLIENT_SECRET,
        "topic": TOPIC,
    }
    SparkExpectationsKafkaWritePluginImpl().writer(
        _write_args={
            "enable_se_streaming": True,
            "stats_df": _FakeDataFrame(_RecordingWriteBuilder()),
            "kafka_write_options": options,
        }
    )
    rendered = captured_log.getvalue()
    _assert_no_sentinels(rendered)
    assert "security_protocol=other" in rendered
    assert "sasl_mechanism=other" in rendered


class _FakeSecretsBackend:
    """Stands in for the real secret backend; returns sentinels keyed by the requested name."""

    SECRETS = {
        "client_id_key": CLIENT_ID,
        "token_key": CLIENT_SECRET,
        "token_endpoint_key": TOKEN_ENDPOINT,
        "server_url_key": BOOTSTRAP,
        "topic_key": TOPIC,
    }

    def __init__(self, _se_stats_dict: Dict[str, Any]) -> None:
        pass

    def get_secret(self, key: str) -> str:
        return self.SECRETS[key]


def _writer_with_context(custom_config: bool, dbr_version: Optional[str]) -> SparkExpectationsWriter:
    writer = SparkExpectationsWriter.__new__(SparkExpectationsWriter)
    writer._context = SimpleNamespace(
        get_env="prod",
        get_dbr_version=dbr_version,
        get_se_streaming_stats_kafka_custom_config_enable=custom_config,
        get_se_streaming_stats_kafka_bootstrap_server=BOOTSTRAP,
        get_client_id="client_id_key",
        get_token="token_key",
        get_token_endpoint_url="token_endpoint_key",
        get_server_url_key="server_url_key",
        get_topic_name="topic_key",
    )
    return writer


@pytest.mark.parametrize(
    "custom_config, dbr_version",
    [(True, None), (False, "12.2"), (False, "14.3")],
    ids=["custom", "legacy", "modern"],
)
@pytest.mark.parametrize("fail", [False, True], ids=["success", "failure"])
def test_secret_backend_options_never_logged(monkeypatch, captured_log, custom_config, dbr_version, fail):
    monkeypatch.setattr(writer_module, "SparkExpectationsSecretsBackend", _FakeSecretsBackend)
    options = _writer_with_context(custom_config, dbr_version).get_kafka_write_options({})
    assert CLIENT_SECRET in options["kafka.sasl.jaas.config"]
    expected = dict(options)
    builder = _RecordingWriteBuilder(error=_echoing_error(options) if fail else None)

    write_args = {"enable_se_streaming": True, "stats_df": _FakeDataFrame(builder), "kafka_write_options": options}
    if fail:
        with pytest.raises(SparkExpectationsMiscException) as exc_info:
            SparkExpectationsKafkaWritePluginImpl().writer(_write_args=write_args)
        _assert_no_sentinels(_full_exception_text(exc_info.value))
    else:
        SparkExpectationsKafkaWritePluginImpl().writer(_write_args=write_args)

    assert builder.received_options == expected
    _assert_no_sentinels(captured_log.getvalue())


@pytest.mark.parametrize("detail", ["x7", "c2VjcmV0", "credential-prefix", "row contents unrelated to options"])
def test_failure_never_renders_downstream_details(captured_log, detail):
    builder = _RecordingWriteBuilder(error=RuntimeError(detail))
    with pytest.raises(SparkExpectationsMiscException) as error:
        SparkExpectationsKafkaWritePluginImpl().writer(
            {"enable_se_streaming": True, "stats_df": _FakeDataFrame(builder), "kafka_write_options": {}}
        )
    assert detail not in _full_exception_text(error.value)
    assert detail not in captured_log.getvalue()
    assert error.value.__context__ is None
    assert error.value.__cause__ is None


def test_ambient_caller_exception_is_suppressed(captured_log):
    try:
        raise RuntimeError("AMBIENT_PRIVATE_SENTINEL")
    except RuntimeError:
        with pytest.raises(SparkExpectationsMiscException) as error:
            SparkExpectationsKafkaWritePluginImpl().writer(
                {"enable_se_streaming": True, "stats_df": _FakeDataFrame(_RecordingWriteBuilder(RuntimeError("private"))), "kafka_write_options": {}}
            )
        assert "AMBIENT_PRIVATE_SENTINEL" not in _full_exception_text(error.value)
        assert error.value.__suppress_context__ is True


def make_writer(monkeypatch, custom, version):
    context=MagicMock()
    context.get_run_date='2026-11-09 12:00:00'
    context.get_run_id_name='run_id'; context.get_run_date_name='run_date'; context.get_run_date_time_name='run_time'
    context.get_dq_rules_params={}; context.get_se_job_metadata={}
    context.get_stats_table_writer_config={'format':'delta'}
    context.get_se_streaming_stats_dict={writer_module.user_config.se_enable_streaming:True}
    context.get_env='prod'; context.get_dbr_version=version
    context.get_se_streaming_stats_kafka_custom_config_enable=custom
    for attr in ['client_id','token','token_endpoint_url','server_url_key','topic_name']:
        setattr(context,'get_'+attr,attr)
    writer=writer_module.SparkExpectationsWriter(context)
    df=MagicMock(); df.withColumn.return_value=df
    writer.spark.createDataFrame.return_value=df
    writer.save_df_as_table=MagicMock()
    monkeypatch.setattr(writer_module,'sql_round',lambda *a:None); monkeypatch.setattr(writer_module,'lit',lambda *a:None)
    return writer, context


@pytest.mark.parametrize("custom,version", [(True, None), (False, "12.2"), (False, "14.3")])
def test_secret_fetch_failure_does_not_reach_diagnostics(monkeypatch, captured_log, custom, version):
    writer, context = make_writer(monkeypatch, custom, version)
    class FailingBackend:
        def __init__(self, config):
            pass
        def get_secret(self, key):
            raise RuntimeError("BACKEND_PRIVATE_SENTINEL")
    monkeypatch.setattr(writer_module, "SparkExpectationsSecretsBackend", FailingBackend)
    with pytest.raises(SparkExpectationsMiscException) as error:
        writer.write_error_stats()
    notification = context.set_kafka_write_error_message.call_args.args[0]
    assert "BACKEND_PRIVATE_SENTINEL" not in notification + captured_log.getvalue() + _full_exception_text(error.value)
    context.set_kafka_write_status.assert_called_once_with("Failed")
