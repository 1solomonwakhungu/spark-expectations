"""Runtime tests for spark.expectations.rules.validation.policy.

These run SparkExpectations.with_expectations on a local Spark session with
in-memory DataFrames. Every table write goes to a mock, Kafka stats streaming is
disabled, the sink hook is replaced by a mock that fails the test if called,
and notifications are off.
"""
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.exceptions import (
    SparkExpectationsMiscException,
    SparkExpectationsUserInputOrConfigInvalidException,
)
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from spark_expectations.sinks.utils.writer import SparkExpectationsWriter

PRODUCT = "policy_product"
TARGET = "policy_db.policy_target"

RULES_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("table_name", StringType(), True),
        StructField("rule_type", StringType(), True),
        StructField("rule", StringType(), True),
        StructField("column_name", StringType(), True),
        StructField("expectation", StringType(), True),
        StructField("action_if_failed", StringType(), True),
        StructField("tag", StringType(), True),
        StructField("description", StringType(), True),
        StructField("enable_for_source_dq_validation", BooleanType(), True),
        StructField("enable_for_target_dq_validation", BooleanType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("enable_error_drop_alert", BooleanType(), True),
        StructField("error_drop_threshold", StringType(), True),
        StructField("priority", StringType(), True),
    ]
)


def _rule(
    name: str,
    expectation: str,
    rule_type: str = "row_dq",
    source: bool = True,
    target: bool = False,
    active: bool = True,
    table: str = TARGET,
    product: str = PRODUCT,
) -> Dict[str, Any]:
    return {
        "product_id": product,
        "table_name": table,
        "rule_type": rule_type,
        "rule": name,
        "column_name": "col1",
        "expectation": expectation,
        "action_if_failed": "ignore",
        "tag": "validity",
        "description": name,
        "enable_for_source_dq_validation": source,
        "enable_for_target_dq_validation": target,
        "is_active": active,
        "enable_error_drop_alert": False,
        "error_drop_threshold": "0",
        "priority": "medium",
    }


VALID_ROW = _rule("row_ok", "col1 > 0")
INVALID_ROW = _rule("row_bad", "sum(col1) > 0")
VALID_AGG = _rule("agg_ok", "sum(col1) > 0", "agg_dq", source=True, target=False)
INVALID_AGG = _rule("agg_bad", "col1 > 0", "agg_dq", source=True, target=False)
VALID_QUERY = _rule("query_ok", "(select count(*) from policy_src_view) > 0", "query_dq", source=True, target=False)
INVALID_QUERY = _rule("query_bad", "SELEC FROM", "query_dq", source=True, target=False)


@pytest.fixture(name="spark_session")
def fixture_spark_session(spark):
    if spark is None:
        pytest.skip("requires a local Spark session (UNIT_TESTING_ENV)")
    return spark


@pytest.fixture(name="input_df")
def fixture_input_df(spark_session):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "col1 int, col2 string")
    df.createOrReplaceTempView("policy_src_view")
    return df


@pytest.fixture(name="sinks")
def fixture_sinks():
    """Mock every sink. ``stats`` records the context state at each stats write."""
    stats: List[Dict[str, Any]] = []

    def _capture(writer_self):
        ctx = writer_self._context
        stats.append(
            {
                "run_status": ctx.get_dq_run_status,
                "input_count": ctx.get_input_count,
                "invalid_rule_stats": ctx.get_invalid_rule_stats,
                "invalid_rule_ids": ctx.get_invalid_rule_ids,
                "skipped_rule_ids": ctx.get_skipped_rule_ids,
                "num_row_dq_rules": ctx.get_num_row_dq_rules,
                "num_dq_rules": ctx.get_num_dq_rules,
                "num_agg_dq_rules": dict(ctx.get_num_agg_dq_rules),
                "num_query_dq_rules": dict(ctx.get_num_query_dq_rules),
                "row_dq_status": ctx.get_row_dq_status,
                "source_agg_dq_status": ctx.get_source_agg_dq_status,
            }
        )

    sink_hook = MagicMock(side_effect=AssertionError("stats streaming sink must not be called"))
    with patch.object(SparkExpectationsWriter, "write_error_stats", autospec=True, side_effect=_capture), patch.object(
        SparkExpectationsWriter, "save_df_as_table", autospec=True, return_value=None
    ) as save, patch("spark_expectations.sinks.utils.writer._sink_hook.writer", sink_hook):
        yield {"stats": stats, "save": save, "sink_hook": sink_hook}


def _se(spark_session, rules: List[Dict[str, Any]]) -> SparkExpectations:
    writer = WrappedDataFrameWriter().mode("append").format("parquet")
    return SparkExpectations(
        product_id=PRODUCT,
        rules_df=spark_session.createDataFrame(rules, schema=RULES_SCHEMA),
        stats_table="policy_db.policy_stats",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        stats_streaming_options={user_config.se_enable_streaming: False},
    )


def _conf(policy=None) -> Dict[str, Any]:
    conf: Dict[str, Any] = {
        user_config.se_enable_streaming: False,
        user_config.se_notifications_on_start: False,
        user_config.se_notifications_on_completion: False,
        user_config.se_notifications_on_fail: False,
    }
    if policy is not None:
        conf[user_config.se_rules_validation_policy] = policy
    return conf


def _decorate(se: SparkExpectations, df_supplier, policy=None):
    return se.with_expectations(TARGET, user_conf=_conf(policy))(df_supplier)


def _spy_execution(se: SparkExpectations) -> List[Dict[str, Any]]:
    """Record the expectations passed to execute_dq_process for each run."""
    seen: List[Dict[str, Any]] = []
    original = se._process.execute_dq_process

    def _spy(*args, **kwargs):
        seen.append(kwargs["expectations"])
        return original(*args, **kwargs)

    se._process.execute_dq_process = _spy
    return seen


def _rule_names(expectations: Dict[str, Any]) -> Dict[str, List[str]]:
    return {
        key: [rule["rule"] for rule in value]
        for key, value in expectations.items()
        if key.endswith("_rules") and isinstance(value, list)
    }


def _stub_stages(se: SparkExpectations, input_df) -> Dict[str, MagicMock]:
    """Replace stage runners with mocks, to check which stages a run would execute."""
    stubs = {
        "_run_source_agg_dq_batch": MagicMock(),
        "_run_source_query_dq_batch": MagicMock(),
        "_run_row_dq": MagicMock(return_value=(input_df, 0, 3)),
        "_run_target_agg_dq_batch": MagicMock(),
        "_run_target_query_dq_batch": MagicMock(),
        "_call_row_dq_notifications": MagicMock(return_value=[]),
    }
    for name, stub in stubs.items():
        setattr(se, name, stub)
    return stubs


def test_default_policy_is_nonblocking_and_runs_every_rule(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, INVALID_ROW])
    seen = _spy_execution(se)
    stages = _stub_stages(se, input_df)

    result = _decorate(se, lambda: input_df)()

    assert result is input_df
    assert _rule_names(seen[0]) == {"row_dq_rules": ["row_ok", "row_bad"]}
    stages["_run_row_dq"].assert_called_once()
    assert se._context.get_rules_validation_policy == "warn_and_run"
    run = sinks["stats"][-1]
    assert run["invalid_rule_stats"] == {"num_invalid_rules": 1, "num_skipped_rules": 0, "input_counted": 1}
    assert run["num_row_dq_rules"] == 2
    assert run["skipped_rule_ids"] == []
    sinks["sink_hook"].assert_not_called()


def test_explicit_warn_and_run_matches_default(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, INVALID_ROW, VALID_AGG, INVALID_AGG])
    seen = _spy_execution(se)
    _stub_stages(se, input_df)

    _decorate(se, lambda: input_df, "warn_and_run")()

    assert _rule_names(seen[0]) == {
        "row_dq_rules": ["row_ok", "row_bad"],
        "agg_dq_rules": ["agg_ok", "agg_bad"],
    }
    assert sinks["stats"][-1]["num_agg_dq_rules"]["num_agg_dq_rules"] == 2


def test_skip_invalid_mixed_rule_types_end_to_end(spark_session, input_df, sinks):
    rules = [VALID_ROW, INVALID_ROW, VALID_AGG, INVALID_AGG, VALID_QUERY, INVALID_QUERY]
    se = _se(spark_session, rules)
    seen = _spy_execution(se)

    result = _decorate(se, lambda: input_df, "skip_invalid")()

    assert result.count() == 3
    assert _rule_names(seen[0]) == {
        "row_dq_rules": ["row_ok"],
        "agg_dq_rules": ["agg_ok"],
        "query_dq_rules": ["query_ok"],
    }
    assert _rule_names(se._context.get_dq_expectations) == _rule_names(seen[0])
    run = sinks["stats"][-1]
    assert run["run_status"] == "Passed"
    assert run["invalid_rule_stats"] == {"num_invalid_rules": 3, "num_skipped_rules": 3, "input_counted": 1}
    assert run["num_row_dq_rules"] == 1
    assert run["num_dq_rules"] == 3
    assert run["num_agg_dq_rules"]["num_agg_dq_rules"] == 1
    assert run["num_query_dq_rules"]["num_query_dq_rules"] == 1
    assert len(run["skipped_rule_ids"]) == 3
    assert all('"policy_product"' in rid for rid in run["skipped_rule_ids"])
    sinks["sink_hook"].assert_not_called()


def test_skip_invalid_all_agg_rules_disables_agg_stage(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, INVALID_AGG])
    stages = _stub_stages(se, input_df)

    _decorate(se, lambda: input_df, "skip_invalid")()

    stages["_run_source_agg_dq_batch"].assert_not_called()
    stages["_run_row_dq"].assert_called_once()
    assert se._context.get_rules_execution_settings_config["source_agg_dq"] is False
    assert "agg_dq_rules" not in se._context.get_dq_expectations


def test_skip_invalid_all_rules_invalid_runs_no_stage(spark_session, input_df, sinks):
    se = _se(spark_session, [INVALID_ROW, INVALID_AGG, INVALID_QUERY])
    stages = _stub_stages(se, input_df)

    result = _decorate(se, lambda: input_df, "skip_invalid")()

    assert result is input_df
    for stub in stages.values():
        stub.assert_not_called()
    run = sinks["stats"][-1]
    assert run["invalid_rule_stats"]["num_skipped_rules"] == 3
    assert run["num_dq_rules"] == 0
    assert run["row_dq_status"] == "Skipped"


def test_skip_invalid_removes_secondary_queries_of_skipped_rule(spark_session, input_df, sinks):
    composite_bad = _rule(
        "query_composite_bad",
        "({source_f1}) > 0@source_f1@SELEC FROM policy_src_view",
        "query_dq",
    )
    composite_ok = _rule(
        "query_composite_ok",
        "({source_f1}) > 0@source_f1@select count(*) from policy_src_view",
        "query_dq",
    )
    se = _se(spark_session, [VALID_ROW, composite_bad, composite_ok])
    _stub_stages(se, input_df)
    decorated = _decorate(se, lambda: input_df, "skip_invalid")
    before = dict(se._context.get_querydq_secondary_queries)

    decorated()

    bad_key = f"{PRODUCT}|{TARGET}|query_composite_bad"
    ok_key = f"{PRODUCT}|{TARGET}|query_composite_ok"
    assert bad_key in before and ok_key in before
    after = se._context.get_querydq_secondary_queries
    assert bad_key not in after
    assert ok_key in after


def test_fail_raises_before_count_and_execution_with_fresh_stats(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, INVALID_ROW])
    seen = _spy_execution(se)
    stages = _stub_stages(se, input_df)
    df = MagicMock(wraps=input_df)
    df.isStreaming = False

    with pytest.raises(SparkExpectationsMiscException) as excinfo:
        _decorate(se, lambda: df, "fail")()

    message = str(excinfo.value)
    assert "1 rule(s) failed validation" in message
    assert f'["{PRODUCT}", "{TARGET}", "row_dq", "row_bad", 1]' in message
    df.count.assert_not_called()
    assert seen == []
    for stub in stages.values():
        stub.assert_not_called()
    assert len(sinks["stats"]) == 1
    run = sinks["stats"][0]
    assert run["run_status"] == "Failed"
    assert run["input_count"] == 0
    assert run["invalid_rule_stats"] == {"num_invalid_rules": 1, "num_skipped_rules": 0, "input_counted": 0}
    assert run["num_row_dq_rules"] == 2


def test_fail_after_success_on_same_instance_resets_state(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, _rule("needs_col3", "col3 > 0")])
    _stub_stages(se, input_df)
    df_with_col3 = input_df.withColumn("col3", input_df.col1)
    frames = [df_with_col3, input_df]
    decorated = _decorate(se, lambda: frames.pop(0), "fail")

    decorated()
    first = sinks["stats"][-1]
    assert first["run_status"] == "Passed"
    assert first["invalid_rule_stats"]["num_invalid_rules"] == 0
    assert first["input_count"] == 3

    with pytest.raises(SparkExpectationsMiscException, match="needs_col3"):
        decorated()
    second = sinks["stats"][-1]
    assert second["run_status"] == "Failed"
    assert second["input_count"] == 0
    assert second["invalid_rule_stats"] == {"num_invalid_rules": 1, "num_skipped_rules": 0, "input_counted": 0}
    assert second["source_agg_dq_status"] == "Skipped"


def test_repeated_runs_do_not_reuse_skipped_state(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, _rule("needs_col3", "col3 > 0")])
    seen = _spy_execution(se)
    _stub_stages(se, input_df)
    df_with_col3 = input_df.withColumn("col3", input_df.col1)
    frames = [input_df, df_with_col3, input_df]
    decorated = _decorate(se, lambda: frames.pop(0), "skip_invalid")

    decorated()
    decorated()
    decorated()

    assert [_rule_names(e) for e in seen] == [
        {"row_dq_rules": ["row_ok"]},
        {"row_dq_rules": ["row_ok", "needs_col3"]},
        {"row_dq_rules": ["row_ok"]},
    ]
    assert [s["num_row_dq_rules"] for s in sinks["stats"]] == [1, 2, 1]
    assert [s["invalid_rule_stats"]["num_skipped_rules"] for s in sinks["stats"]] == [1, 0, 1]


def test_two_decorators_on_one_instance_keep_their_own_rules(spark_session, input_df, sinks):
    other_table = "policy_db.other_target"
    se = _se(
        spark_session,
        [VALID_ROW, _rule("other_a", "col1 > 1", table=other_table), _rule("other_b", "col1 > 2", table=other_table)],
    )
    seen = _spy_execution(se)
    _stub_stages(se, input_df)
    first = se.with_expectations(TARGET, user_conf=_conf("skip_invalid"))(lambda: input_df)
    second = se.with_expectations(other_table, user_conf=_conf("skip_invalid"))(lambda: input_df)

    first()
    assert sinks["stats"][-1]["num_row_dq_rules"] == 1
    second()
    assert sinks["stats"][-1]["num_row_dq_rules"] == 2
    assert [_rule_names(e) for e in seen] == [
        {"row_dq_rules": ["row_ok"]},
        {"row_dq_rules": ["other_a", "other_b"]},
    ]


def test_scope_ignores_inactive_and_other_table_rules(spark_session, input_df, sinks):
    rules = [
        VALID_ROW,
        _rule("inactive_bad", "sum(col1) > 0", active=False),
        _rule("other_table_bad", "sum(col1) > 0", table="policy_db.other"),
        _rule("other_product_bad", "sum(col1) > 0", product="another_product"),
    ]
    se = _se(spark_session, rules)
    _stub_stages(se, input_df)

    _decorate(se, lambda: input_df, "fail")()

    assert sinks["stats"][-1]["invalid_rule_stats"]["num_invalid_rules"] == 0


def test_unknown_rule_type_fails(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, _rule("odd", "col1 > 0", "nope")])
    _stub_stages(se, input_df)

    with pytest.raises(SparkExpectationsMiscException, match=r'"nope", "odd"'):
        _decorate(se, lambda: input_df, "fail")()


def test_unknown_rule_type_is_skipped(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, _rule("odd", "col1 > 0", "nope")])
    seen = _spy_execution(se)
    _stub_stages(se, input_df)

    _decorate(se, lambda: input_df, "skip-invalid")()

    assert _rule_names(seen[0]) == {"row_dq_rules": ["row_ok"]}
    assert sinks["stats"][-1]["invalid_rule_stats"]["num_skipped_rules"] == 1


def test_duplicate_rule_names_count_each_occurrence(spark_session, input_df, sinks):
    se = _se(spark_session, [_rule("dup", "col1 > 0"), _rule("dup", "sum(col1) > 0"), _rule("dup", "max(col1) > 0")])
    seen = _spy_execution(se)
    _stub_stages(se, input_df)

    _decorate(se, lambda: input_df, "skip_invalid")()

    assert [r["expectation"] for r in seen[0]["row_dq_rules"]] == ["col1 > 0"]
    stats = sinks["stats"][-1]
    assert stats["invalid_rule_stats"]["num_skipped_rules"] == 2
    assert len(set(stats["skipped_rule_ids"])) == 2


def test_formatted_params_are_validated_and_reported(spark_session, input_df, sinks):
    rules = [_rule("{env}_check", "sum(col1) > 0"), _rule("{env}_ok", "col1 > 0")]
    se = _se(spark_session, rules)
    _stub_stages(se, input_df)
    conf = {**_conf("fail"), user_config.se_dq_rules_params: {"env": "dev"}}

    with pytest.raises(SparkExpectationsMiscException) as excinfo:
        se.with_expectations(TARGET, user_conf=conf)(lambda: input_df)()

    assert '"dev_check"' in str(excinfo.value)
    assert "{env}" not in str(excinfo.value)
    assert sinks["stats"][-1]["invalid_rule_stats"]["num_invalid_rules"] == 1


def test_unknown_policy_value_fails_at_decoration(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW])
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="maybe"):
        _decorate(se, lambda: input_df, "maybe")
    assert sinks["stats"] == []


def test_policy_from_spark_conf(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, INVALID_ROW])
    _stub_stages(se, input_df)
    spark_session.conf.set(user_config.se_rules_validation_policy, "fail")
    try:
        with pytest.raises(SparkExpectationsMiscException, match="row_bad"):
            se.with_expectations(TARGET, user_conf=None)(lambda: input_df)()
    finally:
        spark_session.conf.unset(user_config.se_rules_validation_policy)


@pytest.mark.parametrize("raw", ["null", "None", "true"])
def test_raw_null_like_spark_conf_policy_is_rejected(spark_session, input_df, sinks, raw):
    se = _se(spark_session, [VALID_ROW])
    spark_session.conf.set(user_config.se_rules_validation_policy, raw)
    try:
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match=repr(raw)):
            se.with_expectations(TARGET, user_conf=_conf())(lambda: input_df)
    finally:
        spark_session.conf.unset(user_config.se_rules_validation_policy)
    assert sinks["stats"] == []


def test_explicit_null_user_conf_policy_is_rejected(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW])
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException):
        _decorate(se, lambda: input_df, "null")
    conf = {**_conf(), user_config.se_rules_validation_policy: None}
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException):
        se.with_expectations(TARGET, user_conf=conf)(lambda: input_df)


def test_serverless_ignores_session_policy(spark_session, input_df, sinks):
    se = _se(spark_session, [VALID_ROW, INVALID_ROW])
    seen = _spy_execution(se)
    _stub_stages(se, input_df)
    spark_session.conf.set(user_config.se_rules_validation_policy, "fail")
    try:
        conf = {**_conf(), user_config.is_serverless: True}
        se.with_expectations(TARGET, user_conf=conf)(lambda: input_df)()
    finally:
        spark_session.conf.unset(user_config.se_rules_validation_policy)
    assert _rule_names(seen[0]) == {"row_dq_rules": ["row_ok", "row_bad"]}
    assert se._context.get_rules_validation_policy == "warn_and_run"


def test_stats_row_contains_invalid_rules_map(spark_session, input_df):
    """Exercise the real write_error_stats with only the table write mocked."""
    se = _se(spark_session, [VALID_ROW, INVALID_ROW])
    _stub_stages(se, input_df)
    written = []

    def _save(writer_self, df, table_name, config, stats_table=False):
        written.append((table_name, df.collect()))

    sink_hook = MagicMock(side_effect=AssertionError("stats streaming sink must not be called"))
    with patch.object(SparkExpectationsWriter, "save_df_as_table", autospec=True, side_effect=_save), patch(
        "spark_expectations.sinks.utils.writer._sink_hook.writer", sink_hook
    ):
        _decorate(se, lambda: input_df, "skip_invalid")()

    stats_rows = [rows for table, rows in written if table == "policy_db.policy_stats"]
    assert len(stats_rows) == 1
    dq_rules = stats_rows[0][0]["dq_rules"]
    assert dq_rules["invalid_rules"] == {"num_invalid_rules": 1, "num_skipped_rules": 1, "input_counted": 1}
    assert dq_rules["rules"] == {"num_row_dq_rules": 1, "num_dq_rules": 1}
    sink_hook.assert_not_called()
