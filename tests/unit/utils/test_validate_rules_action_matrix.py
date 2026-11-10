"""Unit tests for the rule_type-specific action_if_failed matrix.

row_dq accepts drop, ignore and fail. agg_dq and query_dq accept ignore and fail.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.core.expectations import SparkExpectations
from spark_expectations.utils.validate_rules import (
    ALLOWED_ACTIONS_BY_RULE_TYPE,
    ActionIfFailed,
    RuleType,
    SparkExpectationsValidateRules,
)

VALID_EXPECTATIONS = {
    "row_dq": "col1 > 10",
    "agg_dq": "sum(col1) > 100",
    "query_dq": "SELECT count(*) FROM table1",
}

ACCEPTED = {
    ("row_dq", "drop"): True,
    ("row_dq", "ignore"): True,
    ("row_dq", "fail"): True,
    ("agg_dq", "drop"): False,
    ("agg_dq", "ignore"): True,
    ("agg_dq", "fail"): True,
    ("query_dq", "drop"): False,
    ("query_dq", "ignore"): True,
    ("query_dq", "fail"): True,
}

MATRIX = [
    pytest.param(rule_type, action, ok, id=f"{rule_type}-{action}")
    for (rule_type, action), ok in ACCEPTED.items()
]

ACCEPTED_LISTS = {
    "row_dq": "['drop', 'fail', 'ignore']",
    "agg_dq": "['fail', 'ignore']",
    "query_dq": "['fail', 'ignore']",
}


def _rule(rule_type, action, name="matrix_rule"):
    rule = {"rule": name, "expectation": VALID_EXPECTATIONS.get(rule_type, "col1 > 10"), "action_if_failed": action}
    if rule_type is not None:
        rule["rule_type"] = rule_type
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


def test_allowed_actions_table_matches_documented_matrix():
    table = {
        (rule_type.value, action.value): action.value in ALLOWED_ACTIONS_BY_RULE_TYPE[rule_type]
        for rule_type in RuleType
        for action in ActionIfFailed
    }
    assert table == ACCEPTED


class TestValidateActionIfFailedMatrix:

    @pytest.mark.parametrize("rule_type, action, ok", MATRIX)
    def test_warn_mode(self, rule_type, action, ok):
        rule = _rule(rule_type, action)
        result = SparkExpectationsValidateRules.validate_action_if_failed(rule)
        assert result.is_valid is ok
        assert result.rule is rule
        assert result.rule_type == rule_type
        if ok:
            assert result.error_message is None
        else:
            assert result.error_message == (
                f"Rule 'matrix_rule' has invalid action_if_failed value: '{action}' for rule_type "
                f"'{rule_type}'. Accepted values are: {ACCEPTED_LISTS[rule_type]}"
            )

    @pytest.mark.parametrize("rule_type, action, ok", MATRIX)
    def test_raise_mode(self, rule_type, action, ok):
        rule = _rule(rule_type, action)
        if ok:
            assert SparkExpectationsValidateRules.validate_action_if_failed(rule, raise_exception=True).is_valid
        else:
            expected = f"for rule_type '{rule_type}'"
            with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match=expected):
                SparkExpectationsValidateRules.validate_action_if_failed(rule, raise_exception=True)

    @pytest.mark.parametrize("raise_exception", [False, True])
    @pytest.mark.parametrize("action", ["", None, "<absent>"])
    @pytest.mark.parametrize("rule_type", ["row_dq", "agg_dq", "query_dq", "bogus_dq", None])
    def test_missing_or_blank_action_is_accepted(self, rule_type, action, raise_exception):
        rule = _rule(rule_type, action)
        if action == "<absent>":
            del rule["action_if_failed"]
        result = SparkExpectationsValidateRules.validate_action_if_failed(rule, raise_exception=raise_exception)
        assert result.is_valid is True
        assert result.rule_type == (rule_type if rule_type is not None else "unknown")

    @pytest.mark.parametrize("rule_type", ["row_dq", "agg_dq", "query_dq"])
    @pytest.mark.parametrize("action", ["abort", "Drop", "IGNORE", "skip", " fail"])
    def test_unrecognized_action_lists_type_specific_values(self, rule_type, action):
        result = SparkExpectationsValidateRules.validate_action_if_failed(_rule(rule_type, action))
        assert result.is_valid is False
        assert f"'{action}' for rule_type '{rule_type}'" in result.error_message
        assert result.error_message.endswith(f"Accepted values are: {ACCEPTED_LISTS[rule_type]}")

    @pytest.mark.parametrize("raise_exception", [False, True])
    @pytest.mark.parametrize("action", ["drop", "ignore", "fail"])
    @pytest.mark.parametrize("rule_type", ["bogus_dq", "", None])
    def test_unknown_rule_type_keeps_generic_action_set(self, rule_type, action, raise_exception):
        """The rule_type itself is reported by validate_expectations, not by this helper."""
        result = SparkExpectationsValidateRules.validate_action_if_failed(
            _rule(rule_type, action), raise_exception=raise_exception
        )
        assert result.is_valid is True

    def test_unknown_rule_type_with_unrecognized_action_uses_generic_message(self):
        result = SparkExpectationsValidateRules.validate_action_if_failed(_rule("bogus_dq", "abort"))
        assert result.is_valid is False
        assert result.error_message == (
            "Rule 'matrix_rule' has invalid action_if_failed value: 'abort'. "
            "Accepted values are: ['drop', 'fail', 'ignore']"
        )


class TestValidateExpectationsActionMatrix:
    """The orchestrator applies the same matrix in both modes."""

    @pytest.mark.parametrize("rule_type, action, ok", MATRIX)
    def test_warn_mode(self, mock_df, mock_spark, mock_expr, rule_type, action, ok):
        rule = _rule(rule_type, action)
        result = SparkExpectationsValidateRules.validate_expectations(mock_df, [rule], mock_spark)
        if ok:
            assert result == {}
        else:
            assert list(result) == [rule_type]
            [invalid] = result[rule_type]
            assert invalid.is_valid is False
            assert invalid.rule is rule
            assert "action_if_failed" in invalid.error_message

    @pytest.mark.parametrize("rule_type, action, ok", MATRIX)
    def test_raise_mode(self, mock_df, mock_spark, mock_expr, rule_type, action, ok):
        rule = _rule(rule_type, action)
        if ok:
            assert SparkExpectationsValidateRules.validate_expectations(
                mock_df, [rule], mock_spark, raise_exception=True
            ) == {}
        else:
            with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="'drop' for rule_type"):
                SparkExpectationsValidateRules.validate_expectations(mock_df, [rule], mock_spark, raise_exception=True)

    @pytest.mark.parametrize("raise_exception", [False, True])
    @pytest.mark.parametrize("rule_type", ["row_dq", "agg_dq", "query_dq"])
    def test_missing_action_is_accepted(self, mock_df, mock_spark, mock_expr, rule_type, raise_exception):
        rule = _rule(rule_type, None)
        del rule["action_if_failed"]
        assert SparkExpectationsValidateRules.validate_expectations(
            mock_df, [rule], mock_spark, raise_exception=raise_exception
        ) == {}

    def test_warn_mode_reports_every_invalid_action_and_continues(self, mock_df, mock_spark, mock_expr):
        rules = [
            _rule("agg_dq", "drop", name="agg_drop"),
            _rule("row_dq", "drop", name="row_drop"),
            _rule("query_dq", "drop", name="query_drop"),
            _rule("agg_dq", "fail", name="agg_fail"),
        ]
        result = SparkExpectationsValidateRules.validate_expectations(mock_df, rules, mock_spark)
        assert sorted(result) == ["agg_dq", "query_dq"]
        assert [r.rule["rule"] for r in result["agg_dq"]] == ["agg_drop"]
        assert [r.rule["rule"] for r in result["query_dq"]] == ["query_drop"]

    def test_raise_mode_stops_at_first_invalid_action(self, mock_df, mock_spark, mock_expr):
        rules = [
            _rule("row_dq", "drop", name="row_drop"),
            _rule("query_dq", "drop", name="query_drop"),
            _rule("agg_dq", "drop", name="agg_drop"),
        ]
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="'query_drop'"):
            SparkExpectationsValidateRules.validate_expectations(mock_df, rules, mock_spark, raise_exception=True)


class TestCheckInvalidRulesCaller:
    """The pipeline caller reports type-mismatched actions as warnings and does not raise."""

    def test_agg_and_query_drop_are_warned(self, mocker, mock_df, mock_spark, mock_expr):
        log = mocker.patch("spark_expectations.core.expectations._log")
        validate = mocker.spy(SparkExpectationsValidateRules, "validate_expectations")
        caller = SimpleNamespace(spark=mock_spark)
        rules = [
            _rule("row_dq", "drop", name="row_drop"),
            _rule("agg_dq", "drop", name="agg_drop"),
            _rule("query_dq", "drop", name="query_drop"),
        ]

        SparkExpectations._check_invalid_rules(caller, mock_df, rules)

        assert validate.call_args.kwargs.get("raise_exception", False) is False
        [warning] = log.warning.call_args_list
        assert "Some rules failed validation" in warning.args[0]
        assert sorted(warning.args[1]) == ["agg_drop", "query_drop"]

    def test_documented_actions_log_success(self, mocker, mock_df, mock_spark, mock_expr):
        log = mocker.patch("spark_expectations.core.expectations._log")
        caller = SimpleNamespace(spark=mock_spark)
        rules = [_rule(rt, action, name=f"{rt}_{action}") for (rt, action), ok in ACCEPTED.items() if ok]

        SparkExpectations._check_invalid_rules(caller, mock_df, rules)

        log.warning.assert_not_called()
        log.info.assert_called_once()
