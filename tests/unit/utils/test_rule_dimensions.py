"""Unit tests for spark_expectations.utils.rule_dimensions."""

import copy
import logging

import pytest

from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.utils import rule_dimensions
from spark_expectations.utils.rule_dimensions import (
    RULE_DIMENSIONS,
    STRICT_DIMENSION_VALIDATION_KEY,
    find_invalid_dimension_tags,
    is_strict_dimension_validation,
    validate_rule_dimensions,
)


def _rule(name, tag, **extra):
    rule = {
        "product_id": "p1",
        "table_name": "db.orders",
        "rule": name,
        "rule_type": "row_dq",
        "tag": tag,
    }
    rule.update(extra)
    return rule


@pytest.fixture
def dimension_log(caplog):
    """Capture records from the module logger (the package logger does not propagate)."""
    module_logger = logging.getLogger(rule_dimensions.__name__)
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.WARNING, logger=rule_dimensions.__name__)
    yield caplog
    module_logger.removeHandler(caplog.handler)


def test_allowed_dimensions_are_the_six_lowercase_names():
    assert RULE_DIMENSIONS == (
        "accuracy",
        "completeness",
        "consistency",
        "timeliness",
        "uniqueness",
        "validity",
    )


def test_strict_key_matches_user_config_constant():
    assert STRICT_DIMENSION_VALIDATION_KEY == user_config.se_rules_dimension_validation_strict
    assert STRICT_DIMENSION_VALIDATION_KEY == "spark.expectations.rules.dimension.validation.strict"


@pytest.mark.parametrize("tag", RULE_DIMENSIONS)
def test_each_allowed_dimension_is_accepted(tag, dimension_log):
    assert validate_rule_dimensions([_rule("r", tag)]) == []
    assert validate_rule_dimensions([_rule("r", tag)], strict=True) == []
    assert dimension_log.records == []


@pytest.mark.parametrize("tag", [None, ""])
def test_unset_tag_is_accepted_in_both_modes(tag, dimension_log):
    assert validate_rule_dimensions([_rule("r", tag)]) == []
    assert validate_rule_dimensions([_rule("r", tag)], strict=True) == []
    assert dimension_log.records == []


def test_missing_tag_key_is_accepted():
    rule = _rule("r", None)
    del rule["tag"]
    assert validate_rule_dimensions([rule], strict=True) == []


@pytest.mark.parametrize(
    "tag, suggestion",
    [
        ("completness", "completeness"),
        ("validty", "validity"),
        ("Validity", "validity"),
        ("COMPLETENESS", "completeness"),
        (" validity", "validity"),
        ("uniqueness ", "uniqueness"),
        ("strict", None),
        ("my_custom_dimension", None),
        ("   ", None),
    ],
)
def test_unknown_tag_warns_once_with_rule_identity(tag, suggestion, dimension_log):
    issues = validate_rule_dimensions([_rule("col1_not_null", tag)])

    assert len(issues) == 1
    issue = issues[0]
    assert issue.tag == tag
    assert issue.suggestion == suggestion
    assert (issue.product_id, issue.table_name, issue.rule, issue.rule_type) == (
        "p1",
        "db.orders",
        "col1_not_null",
        "row_dq",
    )

    warnings = [r for r in dimension_log.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert message == issue.message
    assert "Rule 'col1_not_null'" in message
    assert "product_id='p1'" in message
    assert "table_name='db.orders'" in message
    assert "rule_type='row_dq'" in message
    assert repr(tag) in message
    assert "accuracy, completeness, consistency, timeliness, uniqueness, validity" in message
    if suggestion:
        assert f"Did you mean '{suggestion}'?" in message
    else:
        assert "Did you mean" not in message


def test_non_string_tag_is_reported_not_crashing():
    issues = find_invalid_dimension_tags([_rule("r_int", 5), _rule("r_list", ["validity"])])
    assert [i.rule for i in issues] == ["r_int", "r_list"]


def test_rules_are_not_modified_and_order_is_kept(dimension_log):
    rules = [
        _rule("a", "validity"),
        _rule("b", "Validity"),
        _rule("c", ""),
        _rule("d", "strict"),
    ]
    original = copy.deepcopy(rules)

    issues = validate_rule_dimensions(rules)

    assert rules == original
    assert [i.rule for i in issues] == ["b", "d"]
    assert [r.getMessage() for r in dimension_log.records] == [i.message for i in issues]


def test_strict_mode_raises_listing_every_offending_rule_without_warnings(dimension_log):
    rules = [
        _rule("ok", "accuracy"),
        _rule("typo", "completness"),
        _rule("custom", "strict", table_name="db.other", rule_type="agg_dq"),
    ]

    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
        validate_rule_dimensions(rules, strict=True)

    message = str(exc_info.value)
    assert "2 rule(s) have an unrecognized dimension tag" in message
    assert STRICT_DIMENSION_VALIDATION_KEY in message
    assert "Rule 'typo' (product_id='p1', table_name='db.orders', rule_type='row_dq')" in message
    assert "Rule 'custom' (product_id='p1', table_name='db.other', rule_type='agg_dq')" in message
    assert "Did you mean 'completeness'?" in message
    assert "'ok'" not in message
    assert dimension_log.records == []


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, False),
        (False, False),
        (True, True),
        ("true", True),
        ("TRUE", True),
        (" True ", True),
        ("false", False),
        ("False", False),
    ],
)
def test_is_strict_dimension_validation_accepts_booleans(value, expected):
    assert is_strict_dimension_validation(value) is expected


@pytest.mark.parametrize("value", ["yes", "1", "", 1, 0, {"strict": True}])
def test_is_strict_dimension_validation_rejects_other_values(value):
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
        is_strict_dimension_validation(value)
    assert STRICT_DIMENSION_VALIDATION_KEY in str(exc_info.value)


@pytest.mark.parametrize("field", ["rule", "product_id", "table_name", "rule_type", "tag"])
@pytest.mark.parametrize("length", [2, 10000])
def test_diagnostics_bound_and_escape_supplied_fields(field, length, dimension_log):
    rule = _rule("r", "invalid")
    rule[field] = "x" * length + "\n\r\tFORGED"
    issues = validate_rule_dimensions([rule])
    assert len(issues) == 1
    message = issues[0].message
    assert len(message) < 1000
    assert not any(character in message for character in "\n\r\t")
    assert dimension_log.records[0].getMessage() == message


def test_large_numeric_tag_does_not_abort_warning_mode(dimension_log):
    (issue,) = validate_rule_dimensions([_rule("r", 10**5000)])
    assert issue.tag == 10**5000
    assert issue.suggestion is None
    assert "<unrepresentable value>" in issue.message
    assert len(dimension_log.records) == 1
