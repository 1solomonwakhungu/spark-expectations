"""Failures must retain only current-call state and reject secondary SQL."""
from unittest.mock import patch
import pytest
from tests.unit.core import test_rule_validation_policy_runtime as rt
from tests.unit.core.test_rule_validation_policy_runtime import fixture_spark_session, fixture_input_df, fixture_sinks
from spark_expectations.core.exceptions import SparkExpectationsMiscException

def test_supplier_failure_reuses_policy_stats(spark_session, input_df, sinks):
    se = rt._se(spark_session, [rt.VALID_ROW, rt.INVALID_ROW])
    rt._stub_stages(se, input_df)
    calls = []

    def supplier():
        if calls:
            raise RuntimeError('synthetic supplier failure')
        calls.append(1)
        return input_df
    run = rt._decorate(se, supplier, 'skip_invalid')
    run()
    with pytest.raises(SparkExpectationsMiscException, match='synthetic supplier failure'):
        run()
    (a, b) = sinks['stats']
    assert b['invalid_rule_stats'] == dict(num_invalid_rules=0, num_skipped_rules=0, input_counted=0)
    assert b['input_count'] == 0
    assert se._context.get_invalid_rule_ids == []
    assert se._context.get_skipped_rule_ids == []

def test_count_failure_reuses_input_and_stage_stats(spark_session, input_df, sinks):
    se = rt._se(spark_session, [rt.VALID_ROW])
    rt._stub_stages(se, input_df)
    run = rt._decorate(se, lambda : input_df, 'fail')
    run()
    with patch.object(type(input_df), 'count', side_effect=RuntimeError('synthetic count failure')):
        with pytest.raises(SparkExpectationsMiscException, match='synthetic count failure'):
            run()
    b = sinks['stats'][-1]
    assert b['invalid_rule_stats']['input_counted'] == 0 and b['input_count'] == 0

@pytest.mark.parametrize('policy', ['fail', 'skip_invalid'])
def test_invalid_secondary_reaches_spark_execution(spark_session, input_df, sinks, policy):
    from pyspark.sql.types import StructType, StructField, BooleanType
    from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
    r = rt._rule('hidden_bad', '({used}) > 0@used@select count(*) from policy_src_view@unused@SELEC FROM', 'query_dq')
    rules = [dict(rt.VALID_ROW, enable_querydq_custom_output=False), dict(r, enable_querydq_custom_output=True)]
    schema = StructType(rt.RULES_SCHEMA.fields + [StructField('enable_querydq_custom_output', BooleanType())])
    w = WrappedDataFrameWriter().mode('append').format('parquet')
    se = SparkExpectations(product_id=rt.PRODUCT, rules_df=spark_session.createDataFrame(rules, schema), stats_table='policy_db.policy_stats', stats_table_writer=w, target_and_error_table_writer=w, stats_streaming_options={rt.user_config.se_enable_streaming: False})
    conf = {**rt._conf(policy), rt.user_config.se_enable_query_dq_detailed_result: True}
    run = se.with_expectations(rt.TARGET, user_conf=conf)(lambda : input_df)
    seen = []
    orig = spark_session.sql

    def sql(q, *a, **k):
        seen.append(q)
        if q == 'SELEC FROM':
            raise RuntimeError('synthetic invalid secondary reached execution')
        return orig(q, *a, **k)
    with patch.object(spark_session, 'sql', side_effect=sql):
        if policy == 'fail':
            with pytest.raises(SparkExpectationsMiscException, match='hidden_bad'):
                run()
        else:
            run()
    assert 'SELEC FROM' not in seen
    assert sinks['stats'][-1]['invalid_rule_stats']['num_invalid_rules'] == 1
