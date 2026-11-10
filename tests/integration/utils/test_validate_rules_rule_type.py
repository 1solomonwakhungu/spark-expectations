"""Spark-backed checks for rule_type handling in validate_expectations and its pipeline caller."""
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

from spark_expectations.core.exceptions import (
    SparkExpectationsInvalidRuleTypeException,
    SparkExpectationsUserInputOrConfigInvalidException,
)
from spark_expectations.core.expectations import SparkExpectations
from spark_expectations.utils.validate_rules import SparkExpectationsValidateRules

VALID_RULES = [
    {"rule_type": "row_dq", "expectation": "col1 > 1", "rule": "valid_row", "action_if_failed": "drop"},
    {"rule_type": "agg_dq", "expectation": "sum(col1) > 1", "rule": "valid_agg", "action_if_failed": "ignore"},
    {
        "rule_type": "query_dq",
        "expectation": "(select count(*) from test_table) > 1",
        "rule": "valid_query",
        "action_if_failed": "fail",
    },
]


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("DQRuleTypeValidation").getOrCreate()


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame([{"col1": 1, "col2": 10}, {"col1": 2, "col2": 20}])


@pytest.mark.parametrize(
    "fields, message",
    [
        ({"rule_type": "row_dqq"}, "Unknown rule_type: 'row_dqq'"),
        ({}, "Missing rule_type"),
    ],
    ids=["unknown", "missing"],
)
def test_strict_mode_raises_for_unknown_or_missing_rule_type(sample_df, spark, fields, message):
    rule = {"expectation": "col1 > 1", "rule": "bad_type", "action_if_failed": "ignore", **fields}
    with pytest.raises(SparkExpectationsInvalidRuleTypeException, match=message) as exc_info:
        SparkExpectationsValidateRules.validate_expectations(
            sample_df, [*VALID_RULES, rule], spark, raise_exception=True
        )
    assert isinstance(exc_info.value, SparkExpectationsUserInputOrConfigInvalidException)


def test_strict_mode_passes_known_rule_types(sample_df, spark):
    result = SparkExpectationsValidateRules.validate_expectations(sample_df, VALID_RULES, spark, raise_exception=True)
    assert result == {}


def test_warn_mode_reports_unknown_rule_type(sample_df, spark):
    rule = {"rule_type": "row_dqq", "expectation": "col1 > 1", "rule": "bad_type", "action_if_failed": "ignore"}
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [*VALID_RULES, rule], spark)
    assert list(invalid_results) == ["row_dqq"]
    assert invalid_results["row_dqq"][0].rule is rule


def test_pipeline_caller_stays_non_blocking(sample_df, spark):
    rule = {"rule_type": "row_dqq", "expectation": "col1 > 1", "rule": "bad_type", "action_if_failed": "ignore"}
    caller = SimpleNamespace(spark=spark)
    SparkExpectations._check_invalid_rules(caller, sample_df, [*VALID_RULES, rule])
