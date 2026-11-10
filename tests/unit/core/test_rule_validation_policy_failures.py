"""Failures must retain only current-call state and reject secondary SQL."""

from typing import Any, List
from unittest.mock import patch
import pytest
from pyspark.sql.types import StructType, StructField, BooleanType
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from tests.unit.core import test_rule_validation_policy_runtime as rt
from tests.unit.core.test_rule_validation_policy_runtime import fixture_spark_session, fixture_input_df, fixture_sinks
from spark_expectations.core.exceptions import SparkExpectationsMiscException


def test_supplier_failure_resets_policy_stats(spark_session: Any, input_df: Any, sinks: Any) -> None:
    se = rt._se(spark_session, [rt.VALID_ROW, rt.INVALID_ROW])
    rt._stub_stages(se, input_df)
    calls: List[int] = []

    def supplier() -> Any:
        if calls:
            raise RuntimeError("synthetic supplier failure")
        calls.append(1)
        return input_df

    run = rt._decorate(se, supplier, "skip_invalid")
    run()
    with pytest.raises(SparkExpectationsMiscException, match="synthetic supplier failure"):
        run()
    (a, b) = sinks["stats"]
    assert b["invalid_rule_stats"] == dict(num_invalid_rules=0, num_skipped_rules=0, input_counted=0)
    assert b["input_count"] == 0
    assert se._context.get_invalid_rule_ids == []
    assert se._context.get_skipped_rule_ids == []


def test_count_failure_resets_input_and_stage_stats(spark_session: Any, input_df: Any, sinks: Any) -> None:
    se = rt._se(spark_session, [rt.VALID_ROW])
    rt._stub_stages(se, input_df)
    run = rt._decorate(se, lambda: input_df, "fail")
    run()
    with patch.object(type(input_df), "count", side_effect=RuntimeError("synthetic count failure")):
        with pytest.raises(SparkExpectationsMiscException, match="synthetic count failure"):
            run()
    b = sinks["stats"][-1]
    assert b["invalid_rule_stats"]["input_counted"] == 0 and b["input_count"] == 0


@pytest.mark.parametrize("policy", ["fail", "skip_invalid"])
def test_invalid_secondary_never_reaches_execution(spark_session: Any, input_df: Any, sinks: Any, policy: Any) -> None:
    from pyspark.sql.types import StructType, StructField, BooleanType
    from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter

    r = rt._rule("hidden_bad", "({used}) > 0@used@select count(*) from policy_src_view@unused@SELEC FROM", "query_dq")
    rules = [dict(rt.VALID_ROW, enable_querydq_custom_output=False), dict(r, enable_querydq_custom_output=True)]
    schema = StructType(rt.RULES_SCHEMA.fields + [StructField("enable_querydq_custom_output", BooleanType())])
    w = WrappedDataFrameWriter().mode("append").format("parquet")
    se = SparkExpectations(
        product_id=rt.PRODUCT,
        rules_df=spark_session.createDataFrame(rules, schema),
        stats_table="policy_db.policy_stats",
        stats_table_writer=w,
        target_and_error_table_writer=w,
        stats_streaming_options={rt.user_config.se_enable_streaming: False},
    )
    conf = {**rt._conf(policy), rt.user_config.se_enable_query_dq_detailed_result: True}
    run = se.with_expectations(rt.TARGET, user_conf=conf)(lambda: input_df)
    seen = []
    orig = spark_session.sql

    def sql(q: Any, *a: Any, **k: Any) -> Any:
        seen.append(q)
        if q == "SELEC FROM":
            raise RuntimeError("synthetic invalid secondary reached execution")
        return orig(q, *a, **k)

    with patch.object(spark_session, "sql", side_effect=sql):
        if policy == "fail":
            with pytest.raises(SparkExpectationsMiscException, match="hidden_bad"):
                run()
        else:
            run()
    assert "SELEC FROM" not in seen
    assert sinks["stats"][-1]["invalid_rule_stats"]["num_invalid_rules"] == 1


@pytest.mark.parametrize("bad_last", [False, True])
def test_real_shared_key_execution(spark_session: Any, input_df: Any, sinks: Any, bad_last: Any) -> None:
    good = rt._rule(
        "shared",
        "({used}) > 0@used@select count(*) from policy_src_view@unused@select count(*) from policy_src_view",
        "query_dq",
    )
    bad = rt._rule("shared", "({used}) > 0@used@select count(*) from policy_src_view@unused@SELEC FROM", "query_dq")
    rules = [good, bad] if bad_last else [bad, good]
    schema = StructType(rt.RULES_SCHEMA.fields + [StructField("enable_querydq_custom_output", BooleanType())])
    writer = WrappedDataFrameWriter().mode("append").format("parquet")
    se = SparkExpectations(
        product_id=rt.PRODUCT,
        rules_df=spark_session.createDataFrame([dict(r, enable_querydq_custom_output=True) for r in rules], schema),
        stats_table="policy_db.policy_stats",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
        stats_streaming_options={rt.user_config.se_enable_streaming: False},
    )
    run = se.with_expectations(
        rt.TARGET, user_conf={**rt._conf("skip_invalid"), rt.user_config.se_enable_query_dq_detailed_result: True}
    )(lambda: input_df)
    original = spark_session.sql
    seen = []

    def sql(query: Any, *a: Any, **k: Any) -> Any:
        seen.append(query)
        assert query != "SELEC FROM", "malformed query escaped"
        return original(query, *a, **k)

    with patch.object(spark_session, "sql", side_effect=sql):
        run()
    assert "SELEC FROM" not in seen
    assert sinks["stats"][-1]["invalid_rule_stats"]["num_invalid_rules"] == 1
    kept = se._context.get_dq_expectations["query_dq_rules"]
    assert len(kept) == 1 and kept[0]["expectation_unused"].startswith("select")
