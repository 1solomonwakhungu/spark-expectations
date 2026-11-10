"""Unit tests for rule_type handling in SparkExpectationsValidateRules.validate_expectations.

With raise_exception=True an unknown or missing rule_type raises
SparkExpectationsInvalidRuleTypeException. With the default raise_exception=False the
rule is reported in the returned map of invalid results and validation continues.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark_expectations.core.exceptions import (
    SparkExpectationsInvalidRowDQExpectationException,
    SparkExpectationsInvalidRuleTypeException,
    SparkExpectationsUserInputOrConfigInvalidException,
)
from spark_expectations.core.expectations import SparkExpectations
from spark_expectations.utils.validate_rules import SparkExpectationsValidateRules

VALID_RULES_BY_TYPE = {
    "row_dq": {"rule_type": "row_dq", "expectation": "col1 > 10", "rule": "valid_row", "action_if_failed": "drop"},
    "agg_dq": {
        "rule_type": "agg_dq",
        "expectation": "sum(col1) > 100",
        "rule": "valid_agg",
        "action_if_failed": "ignore",
    },
    "query_dq": {
        "rule_type": "query_dq",
        "expectation": "SELECT count(*) FROM table1",
        "rule": "valid_query",
        "action_if_failed": "fail",
    },
}

UNKNOWN_RULE_TYPES = ["row_dqq", "bogus_dq", "ROW_DQ", "row_dq ", "dq"]

# Map key used by validate_expectations for each way a rule_type can be absent.
MISSING_RULE_TYPES = [
    pytest.param({}, "unknown", id="key_absent"),
    pytest.param({"rule_type": None}, None, id="none"),
    pytest.param({"rule_type": ""}, "", id="empty_string"),
]


def _rule(name="type_rule", **fields):
    rule = {"expectation": "col1 > 10", "rule": name, "action_if_failed": "ignore"}
    rule.update(fields)
    return rule


@pytest.fixture
def mock_df():
    mock = MagicMock()
    mock.select.return_value.limit.return_value = mock
    mock.selectExpr.return_value.limit.return_value = mock
    return mock


@pytest.fixture
def mock_spark():
    return MagicMock()


@pytest.fixture
def mock_expr(mocker):
    return mocker.patch("spark_expectations.utils.validate_rules.expr", return_value=MagicMock())


class TestRuleTypeStrictMode:
    """raise_exception=True"""

    @pytest.mark.parametrize("rule_type", UNKNOWN_RULE_TYPES)
    def test_unknown_rule_type_raises(self, mock_df, mock_spark, mock_expr, rule_type):
        rule = _rule(name="unknown_type_rule", rule_type=rule_type)
        with pytest.raises(SparkExpectationsInvalidRuleTypeException) as exc_info:
            SparkExpectationsValidateRules.validate_expectations(mock_df, [rule], mock_spark, raise_exception=True)
        message = str(exc_info.value)
        assert f"Unknown rule_type: '{rule_type}'" in message
        assert "'unknown_type_rule'" in message
        assert "['row_dq', 'agg_dq', 'query_dq']" in message

    @pytest.mark.parametrize("fields, _key", MISSING_RULE_TYPES)
    def test_missing_rule_type_raises(self, mock_df, mock_spark, mock_expr, fields, _key):
        rule = _rule(name="missing_type_rule", **fields)
        with pytest.raises(SparkExpectationsInvalidRuleTypeException) as exc_info:
            SparkExpectationsValidateRules.validate_expectations(mock_df, [rule], mock_spark, raise_exception=True)
        assert "Missing rule_type for rule 'missing_type_rule'" in str(exc_info.value)

    def test_rule_type_exception_is_user_input_or_config_exception(self, mock_df, mock_spark, mock_expr):
        """Callers that already catch the input/config exception also catch rule_type errors."""
        rule = _rule(rule_type="row_dqq")
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException):
            SparkExpectationsValidateRules.validate_expectations(mock_df, [rule], mock_spark, raise_exception=True)

    @pytest.mark.parametrize("rule_type", sorted(VALID_RULES_BY_TYPE))
    def test_known_rule_type_does_not_raise(self, mock_df, mock_spark, mock_expr, rule_type):
        result = SparkExpectationsValidateRules.validate_expectations(
            mock_df, [dict(VALID_RULES_BY_TYPE[rule_type])], mock_spark, raise_exception=True
        )
        assert result == {}

    def test_unknown_rule_type_raises_after_valid_rules(self, mock_df, mock_spark, mock_expr):
        rules = [dict(r) for r in VALID_RULES_BY_TYPE.values()] + [_rule(rule_type="bogus_dq")]
        with pytest.raises(SparkExpectationsInvalidRuleTypeException):
            SparkExpectationsValidateRules.validate_expectations(mock_df, rules, mock_spark, raise_exception=True)

    def test_stops_at_unknown_rule_type_before_later_invalid_rule(self, mock_df, mock_spark, mock_expr):
        rules = [
            _rule(name="first", rule_type="bogus_dq"),
            {"rule_type": "row_dq", "expectation": "sum(col1) > 10", "rule": "second", "action_if_failed": "drop"},
        ]
        with pytest.raises(SparkExpectationsInvalidRuleTypeException):
            SparkExpectationsValidateRules.validate_expectations(mock_df, rules, mock_spark, raise_exception=True)

    def test_earlier_invalid_expectation_still_raises_its_own_exception(self, mock_df, mock_spark, mock_expr):
        rules = [
            {"rule_type": "row_dq", "expectation": "sum(col1) > 10", "rule": "first", "action_if_failed": "drop"},
            _rule(name="second", rule_type="bogus_dq"),
        ]
        with pytest.raises(SparkExpectationsInvalidRowDQExpectationException):
            SparkExpectationsValidateRules.validate_expectations(mock_df, rules, mock_spark, raise_exception=True)

    def test_rule_type_checked_before_action_if_failed(self, mock_df, mock_spark, mock_expr):
        rule = _rule(rule_type="bogus_dq", action_if_failed="abort")
        with pytest.raises(SparkExpectationsInvalidRuleTypeException):
            SparkExpectationsValidateRules.validate_expectations(mock_df, [rule], mock_spark, raise_exception=True)

    def test_unknown_rule_type_does_not_touch_dataframe(self, mock_spark, mock_expr):
        df = MagicMock()
        with pytest.raises(SparkExpectationsInvalidRuleTypeException):
            SparkExpectationsValidateRules.validate_expectations(
                df, [_rule(rule_type="bogus_dq")], mock_spark, raise_exception=True
            )
        df.select.assert_not_called()
        df.selectExpr.assert_not_called()


class TestRuleTypeWarnMode:
    """raise_exception=False (default)"""

    @pytest.mark.parametrize("raise_exception_kwargs", [{}, {"raise_exception": False}], ids=["default", "explicit"])
    @pytest.mark.parametrize("rule_type", UNKNOWN_RULE_TYPES)
    def test_unknown_rule_type_returns_invalid_result(
        self, mock_df, mock_spark, mock_expr, rule_type, raise_exception_kwargs
    ):
        rule = _rule(rule_type=rule_type)
        result = SparkExpectationsValidateRules.validate_expectations(
            mock_df, [rule], mock_spark, **raise_exception_kwargs
        )
        assert list(result) == [rule_type]
        [invalid] = result[rule_type]
        assert invalid.is_valid is False
        assert invalid.error_message == f"Unknown rule_type: '{rule_type}'"
        assert invalid.rule is rule
        assert invalid.rule_type == rule_type

    @pytest.mark.parametrize("fields, key", MISSING_RULE_TYPES)
    def test_missing_rule_type_returns_invalid_result(self, mock_df, mock_spark, mock_expr, fields, key):
        rule = _rule(**fields)
        result = SparkExpectationsValidateRules.validate_expectations(mock_df, [rule], mock_spark)
        assert list(result) == [key]
        [invalid] = result[key]
        assert invalid.is_valid is False
        assert invalid.error_message == f"Unknown rule_type: '{key}'"
        assert invalid.rule is rule

    def test_unknown_rule_type_does_not_stop_validation(self, mock_df, mock_spark, mock_expr):
        bad_row = {
            "rule_type": "row_dq", "expectation": "sum(col1) > 10", "rule": "bad_row", "action_if_failed": "drop"
        }
        rules = [_rule(name="bogus", rule_type="bogus_dq"), dict(VALID_RULES_BY_TYPE["agg_dq"]), bad_row]
        result = SparkExpectationsValidateRules.validate_expectations(mock_df, rules, mock_spark)
        assert sorted(result) == ["bogus_dq", "row_dq"]
        assert [r.rule["rule"] for r in result["bogus_dq"]] == ["bogus"]
        assert [r.rule["rule"] for r in result["row_dq"]] == ["bad_row"]

    def test_unknown_rule_type_logs_warning(self, mocker, mock_df, mock_spark, mock_expr):
        logger = mocker.patch("spark_expectations.utils.validate_rules.logger")
        SparkExpectationsValidateRules.validate_expectations(
            mock_df, [_rule(name="bogus", rule_type="bogus_dq")], mock_spark
        )
        assert any(
            call.args[0].startswith("Invalid rule detected") and "bogus" in call.args
            for call in logger.warning.call_args_list
        )


class TestCheckInvalidRulesCaller:
    """The pipeline caller keeps non-blocking validation for unknown rule types."""

    def test_check_invalid_rules_warns_and_does_not_raise(self, mocker, mock_df, mock_spark, mock_expr):
        log = mocker.patch("spark_expectations.core.expectations._log")
        validate = mocker.spy(SparkExpectationsValidateRules, "validate_expectations")
        caller = SimpleNamespace(spark=mock_spark)
        rules = [_rule(name="bogus_rule", rule_type="row_dqq"), dict(VALID_RULES_BY_TYPE["row_dq"])]

        SparkExpectations._check_invalid_rules(caller, mock_df, rules)

        assert validate.call_count == 1
        assert validate.call_args.kwargs.get("raise_exception", False) is False
        [warning] = log.warning.call_args_list
        assert "Some rules failed validation" in warning.args[0]
        assert warning.args[1] == ["bogus_rule"]
        log.info.assert_not_called()

    def test_check_invalid_rules_with_only_known_types_logs_success(self, mocker, mock_df, mock_spark, mock_expr):
        log = mocker.patch("spark_expectations.core.expectations._log")
        caller = SimpleNamespace(spark=mock_spark)

        SparkExpectations._check_invalid_rules(caller, mock_df, [dict(r) for r in VALID_RULES_BY_TYPE.values()])

        log.warning.assert_not_called()
        log.info.assert_called_once()
