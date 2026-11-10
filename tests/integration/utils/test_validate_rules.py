import re

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from spark_expectations.core.exceptions import (
    SparkExpectationsInvalidAggDQExpectationException,
    SparkExpectationsInvalidQueryDQExpectationException,
    SparkExpectationsInvalidRowDQExpectationException,
    SparkExpectationsInvalidRuleTypeException)
from spark_expectations.utils.validate_rules import (
    SparkExpectationsValidateRules,
    RuleType,
    ValidationResult,
)


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("DQValidation").getOrCreate()


@pytest.fixture
def sample_df(spark):
    return spark.createDataFrame([{"col1": 1, "col2": 10}, {"col1": 2, "col2": 20}, {"col1": 3, "col2": 30}])


@pytest.mark.parametrize(
    "expectation",
    [
        "col1 is null",
        "col1 is not null",
        "col1 > 10",
        "col2 < 25",
        "(col1 % 2) = 0",
    ],
)
def test_valid_row_dq(sample_df, expectation, spark):
    rule = {
        "rule_type": "row_dq",
        "expectation": expectation,
        "rule": "valid_row_dq",
    }
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark)
    assert "row_dq" not in invalid_results


@pytest.mark.parametrize(
    "expectation",
    [
        "sum(col1) > 20",
        "avg(col2) < 25",
        "min(col1) > 10",
        "stddev(col2) > 10",
        "count(distinct col1) > 4",
        "avg(col1) > 4",
        "avg(col2) > 18 and avg(col2) < 2",
        "avg(col1) between 18 and 25",
    ],
)
def test_valid_agg_dq(sample_df, expectation, spark):
    rule = {
        "rule_type": "agg_dq",
        "expectation": expectation,
        "rule": "valid_agg_dq",
    }
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark)
    assert "agg_dq" not in invalid_results


@pytest.mark.parametrize(
    "expectation",
    [
        "(select sum(col1) from test_table) > 10",
        "(select stddev(col2) from test_table) > 0",
        "(select max(col1) from test_final_table_view) > 10",
        "(select min(col1) from test_final_table_view) > 0",
        "(select count(col1) from test_final_table_view) > 3",
        "(select count(case when col2>0 then 1 else 0 end) from test_final_table_view) > 10",
        "(select sum(col1) from {table}) > 10",
        "((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f1}) b) ) < 3@source_f1@select distinct product_id,order_id from order_source@target_f1@select distinct product_id,order_id from order_target",
        "(select count(*) from test_table) > 10",
    ],
)
def test_valid_query_dq(sample_df, expectation, spark):
    rule = {
        "rule_type": "query_dq",
        "expectation": expectation,
        "rule": "valid_query_dq",
    }
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark)
    assert "query_dq" not in invalid_results


@pytest.mark.parametrize(
    "expectation",
    [
        "sum(col1) > 20",  # agg_dq expression, not allowed in row_dq
        "(select stddev(col2) from test_table) > 0",  # SQL query, not allowed in row_dq
        "non_existing_col > 20",  # column does not exist
        "col1 = = = invalid",  # Invalid syntax that sqlglot can't parse
    ],
)
def test_invalid_row_dq(sample_df, expectation, spark):
    rule = {
        "rule_type": "row_dq",
        "expectation": expectation,
        "rule": "invalid_row_dq",
    }
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark)
    assert "row_dq" in invalid_results
    # Check that the rule is in the invalid results
    invalid_rules = [r.rule for r in invalid_results["row_dq"]]
    assert rule in invalid_rules


@pytest.mark.parametrize(
    "expectation",
    [
        "sum(non_existing_col) > 20",  # non_existing_col does not exist
        "col1 > 20",                   # not an aggregate expression
        "sum(col1) = = = invalid",   # Invalid syntax that sqlglot can't parse
    ],
)
def test_invalid_agg_dq(sample_df, expectation, spark):
    rule = {
        "rule_type": "agg_dq",
        "expectation": expectation,
        "rule": "invalid_agg_dq",
    }
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark)
    assert "agg_dq" in invalid_results
    # Check that the rule is in the invalid results
    invalid_rules = [r.rule for r in invalid_results["agg_dq"]]
    assert rule in invalid_rules


@pytest.mark.parametrize(
    "expectation",
    [
        "SELECT SUM(col1) > 5 AS result",             # syntax error
        "col1 > 20",                                  # not a valid query_dq
        "avg(col1) < 100",                            # not a valid query_dq
        "((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f2}) b) ) < 3@source_f1@select distinct product_id,order_id from order_source@target_f1@select distinct product_id,order_id from order_target",  # Placeholder mismatch or missing
        "((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f1}) b) ) < 3@source_f1@select distinct product_id,order_id from order_source@@select distinct product_id,order_id from order_target",  # Placeholder target_f1 missing
        "((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f1}) b) ) < 3@source_f1@invalid_query_without_select_from@target_f1@select distinct product_id,order_id from order_target",  # Invalid subquery without SELECT FROM
        "((select count(*) from ({}) a) - (select count(*) from ({target_f1}) b) ) < 3@source_f1@select distinct product_id,order_id from order_source@target_f1@select distinct product_id,order_id from order_target",  # Missing key
        "((select count(*) from ({source_f1}) a) - (select count(*) from ({target_f1}) b) ) < 3@source_f1@select distinct product_id,order_id from order_source@target_f1@{}",  # Invalid format in subquery
        "((select count(*) from {source_f1=y} a) - (select count(*) from ({target_f1}) b) ) < 3@source_f1@select distinct product_id,order_id from order_source@target_f1@select distinct product_id,order_id from order_target",  # Invalid format spec
        "select count(*) from table_name where col1 = = = invalid",  # Invalid SQL syntax
    ],
)
def test_invalid_query_dq(sample_df, expectation, spark):
    rule = {
        "rule_type": "query_dq",
        "expectation": expectation,
        "rule": "invalid_query_dq",
    }
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark)
    assert "query_dq" in invalid_results
    # Check that the rule is in the invalid results
    invalid_rules = [r.rule for r in invalid_results["query_dq"]]
    assert rule in invalid_rules


@pytest.mark.parametrize(
    "expectation",
    [
        "col1 > 20",
    ],
)
def test_invalid_rule_type_logged_as_invalid(sample_df, expectation, spark):
    """Test that unknown rule types are logged as invalid but don't raise exceptions."""
    rule = {
        "rule_type": "foo_dq",
        "expectation": expectation,
        "rule": "invalid_rule_type",
    }
    # No longer raises exception, instead returns invalid result
    invalid_results = SparkExpectationsValidateRules.validate_expectations(sample_df, [rule], spark)
    assert "foo_dq" in invalid_results
    assert len(invalid_results["foo_dq"]) == 1
    assert "Unknown rule_type" in invalid_results["foo_dq"][0].error_message

@pytest.mark.parametrize(
    "line_break, indent",
    [
        pytest.param("\n", "    ", id="lf_spaces"),
        pytest.param("\r\n", "\t", id="crlf_tab"),
    ],
)
def test_multiline_query_dq_is_valid_and_executes_in_spark(spark, line_break, indent):
    """A multi-line query_dq expectation that Spark executes is accepted by the validator."""
    view_name = "se_multiline_query_dq_synthetic_values"
    spark.createDataFrame([(1,), (2,), (3,)], "col1 int").createOrReplaceTempView(view_name)
    try:
        query = line_break.join(
            [
                "SELECT count(*)",
                f"{indent}FROM {view_name}",
                f"{indent}WHERE col1 IS NOT NULL",
            ]
        )
        single_line_query = f"SELECT count(*) FROM {view_name} WHERE col1 IS NOT NULL"
        assert spark.sql(query).collect()[0][0] == 3
        assert spark.sql(single_line_query).collect()[0][0] == 3

        expectation = f"({query}) = 3"
        # Same statement shape that the query_dq runtime evaluates.
        assert spark.sql(f"SELECT ({expectation}) AS OUTPUT").collect()[0][0] is True

        rule = {
            "rule_type": "query_dq",
            "expectation": expectation,
            "rule": "multiline_query_dq",
            "action_if_failed": "ignore",
        }
        result = SparkExpectationsValidateRules.validate_query_dq_expectation(
            spark.table(view_name), rule, spark, raise_exception=True
        )
        assert result.is_valid is True
        invalid_results = SparkExpectationsValidateRules.validate_expectations(
            spark.table(view_name), [rule], spark, raise_exception=True
        )
        assert invalid_results == {}
    finally:
        spark.catalog.dropTempView(view_name)
