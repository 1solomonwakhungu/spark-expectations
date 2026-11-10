"""Spark-backed checks for the rule_type-specific action_if_failed matrix."""
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.core.expectations import SparkExpectations
from spark_expectations.utils.validate_rules import SparkExpectationsValidateRules

EXPECTATIONS = {
    "row_dq": "col1 > 1",
    "agg_dq": "sum(col1) > 1",
    "query_dq": "(select count(*) from test_table) > 1",
}

MATRIX = [
    pytest.param(rule_type, action, not (action == "drop" and rule_type != "row_dq"), id=f"{rule_type}-{action}")
    for rule_type in EXPECTATIONS
    for action in ("drop", "ignore", "fail")
]


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("DQActionMatrixValidation").getOrCreate()


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame([{"col1": 1, "col2": 10}, {"col1": 2, "col2": 20}])


def _rule(rule_type, action):
    return {
        "rule_type": rule_type,
        "expectation": EXPECTATIONS[rule_type],
        "rule": f"{rule_type}_{action}",
        "action_if_failed": action,
    }


@pytest.mark.parametrize("rule_type, action, ok", MATRIX)
def test_action_matrix_warn_mode(sample_df, spark, rule_type, action, ok):
    rule = _rule(rule_type, action)
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark)
    if ok:
        assert invalid_results == {}
    else:
        assert [r.rule for r in invalid_results[rule_type]] == [rule]


@pytest.mark.parametrize("rule_type, action, ok", MATRIX)
def test_action_matrix_raise_mode(sample_df, spark, rule_type, action, ok):
    rule = _rule(rule_type, action)
    if ok:
        result = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark, raise_exception=True)
        assert result == {}
    else:
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match=f"rule_type '{rule_type}'"):
            SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark, raise_exception=True)


def test_pipeline_caller_stays_non_blocking(sample_df, spark):
    rules = [_rule("row_dq", "drop"), _rule("agg_dq", "drop"), _rule("query_dq", "drop")]
    SparkExpectations._check_invalid_rules(SimpleNamespace(spark=spark), sample_df, rules)
