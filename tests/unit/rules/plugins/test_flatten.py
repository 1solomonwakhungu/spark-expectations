"""Tests for spark_expectations.rules.plugins._flatten"""

import json
import textwrap
from decimal import Decimal
from fractions import Fraction
from unittest.mock import patch

import pytest

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins._flatten import (
    COLUMN_DEFAULTS,
    RULES_SCHEMA_COLUMNS,
    _cast_value,
    _describe_lossy_coercion,
    flatten_rules_list,
)
from spark_expectations.rules.plugins.json_loader import _read_json
from spark_expectations.rules.plugins.yaml_loader import _read_yaml


@pytest.fixture
def minimal_rules_list():
    return {
        "product_id": "prod1",
        "table_name": "db.table1",
        "rules": [
            {
                "rule": "col1_not_null",
                "rule_type": "row_dq",
                "expectation": "col1 IS NOT NULL",
                "column_name": "col1",
                "tag": "completeness",
            }
        ],
    }


@pytest.fixture
def minimal_dq_env_rules():
    return {
        "product_id": "prod1",
        "dq_env": {
            "DEV": {
                "table_name": "catalog.schema.orders",
                "action_if_failed": "ignore",
                "is_active": True,
                "priority": "medium",
            },
            "QA": {
                "table_name": "catalog2.schema.orders",
                "action_if_failed": "ignore",
                "is_active": True,
                "priority": "medium",
            },
            "PROD": {
                "table_name": "catalog3.schema.orders",
                "action_if_failed": "fail",
                "is_active": True,
                "priority": "high",
            },
        },
        "rules": [
            {
                "rule": "col1_not_null",
                "rule_type": "row_dq",
                "expectation": "col1 IS NOT NULL",
                "column_name": "col1",
                "tag": "completeness",
            }
        ],
    }


# ── rules-list format (classic with table_name) ────────────────────────


def test_flatten_rules_list_basic(minimal_rules_list):
    rows = flatten_rules_list(minimal_rules_list)
    assert len(rows) == 1
    row = rows[0]
    assert row["product_id"] == "prod1"
    assert row["table_name"] == "db.table1"
    assert row["rule_type"] == "row_dq"
    assert row["rule"] == "col1_not_null"
    assert row["expectation"] == "col1 IS NOT NULL"


def test_flatten_rules_list_all_schema_columns_present(minimal_rules_list):
    rows = flatten_rules_list(minimal_rules_list)
    for col in RULES_SCHEMA_COLUMNS:
        assert col in rows[0], f"Missing column: {col}"


def test_flatten_rules_list_defaults_applied(minimal_rules_list):
    rows = flatten_rules_list(minimal_rules_list)
    row = rows[0]
    assert row["action_if_failed"] == COLUMN_DEFAULTS["action_if_failed"]
    assert row["is_active"] == COLUMN_DEFAULTS["is_active"]
    assert row["priority"] == COLUMN_DEFAULTS["priority"]


def test_flatten_rules_list_user_defaults_override_column_defaults():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "defaults": {"action_if_failed": "fail", "priority": "high"},
        "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["action_if_failed"] == "fail"
    assert rows[0]["priority"] == "high"


def test_flatten_rules_list_rule_level_overrides_defaults():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "defaults": {"action_if_failed": "ignore"},
        "rules": [
            {
                "rule": "r1",
                "rule_type": "row_dq",
                "expectation": "x > 0",
                "action_if_failed": "drop",
            }
        ],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["action_if_failed"] == "drop"


def test_flatten_rules_list_rule_type_from_defaults():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "defaults": {"rule_type": "row_dq"},
        "rules": [{"rule": "r1", "expectation": "x > 0"}],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["rule_type"] == "row_dq"


def test_flatten_rules_list_rule_type_per_rule_overrides_default():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "defaults": {"rule_type": "row_dq"},
        "rules": [
            {"rule": "r1", "expectation": "x > 0"},
            {"rule": "r2", "rule_type": "agg_dq", "expectation": "count(*) > 0"},
        ],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["rule_type"] == "row_dq"
    assert rows[1]["rule_type"] == "agg_dq"


def test_flatten_rules_list_table_name_override_per_rule():
    data = {
        "product_id": "prod1",
        "table_name": "default_table",
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"},
            {"rule": "r2", "rule_type": "row_dq", "expectation": "y > 0", "table_name": "other_table"},
        ],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["table_name"] == "default_table"
    assert rows[1]["table_name"] == "other_table"


def test_flatten_rules_list_mixed_rule_types():
    data = {
        "product_id": "prod1",
        "table_name": "t1",
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "c1 > 0"},
            {"rule": "r2", "rule_type": "agg_dq", "expectation": "count(*) > 10"},
            {"rule": "r3", "rule_type": "query_dq", "expectation": "SELECT 1"},
        ],
    }
    rows = flatten_rules_list(data)
    assert len(rows) == 3
    assert {r["rule_type"] for r in rows} == {"row_dq", "agg_dq", "query_dq"}


def test_flatten_rules_list_missing_product_id_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="product_id"):
        flatten_rules_list({"rules": [{"rule": "r", "expectation": "x"}]})


def test_flatten_rules_list_empty_rules_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="non-empty"):
        flatten_rules_list({"product_id": "p1", "rules": []})


def test_flatten_rules_list_missing_rule_field_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="missing required"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": [{"expectation": "x > 0"}],
        })


def test_flatten_rules_list_missing_expectation_field_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="missing required"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": [{"rule": "r1"}],
        })


def test_flatten_rules_list_missing_rule_type_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="missing 'rule_type'"):
        flatten_rules_list({
            "product_id": "p1",
            "table_name": "t1",
            "rules": [{"rule": "r1", "expectation": "x > 0"}],
        })


def test_flatten_rules_list_invalid_rule_type_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="Invalid rule_type"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": [{"rule": "r1", "rule_type": "bad_type", "expectation": "x > 0"}],
        })


def test_flatten_rules_list_boolean_values_are_native(minimal_rules_list):
    rows = flatten_rules_list(minimal_rules_list)
    row = rows[0]
    assert isinstance(row["is_active"], bool)
    assert isinstance(row["enable_error_drop_alert"], bool)


def test_flatten_rules_list_no_table_name_at_top_or_rule():
    data = {
        "product_id": "p1",
        "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}],
    }
    rows = flatten_rules_list(data)
    assert rows[0]["table_name"] == ""


# ── rules-list format (dq_env) ─────────────────────────────────────────


def test_flatten_rules_list_dq_env_basic(minimal_dq_env_rules):
    rows = flatten_rules_list(minimal_dq_env_rules, env="DEV")
    assert len(rows) == 1
    row = rows[0]
    assert row["product_id"] == "prod1"
    assert row["table_name"] == "catalog.schema.orders"
    assert row["rule"] == "col1_not_null"
    assert row["action_if_failed"] == "ignore"
    assert row["priority"] == "medium"


def test_flatten_rules_list_dq_env_selects_correct_env(minimal_dq_env_rules):
    rows_dev = flatten_rules_list(minimal_dq_env_rules, env="DEV")
    rows_prod = flatten_rules_list(minimal_dq_env_rules, env="PROD")
    assert rows_dev[0]["table_name"] == "catalog.schema.orders"
    assert rows_prod[0]["table_name"] == "catalog3.schema.orders"
    assert rows_prod[0]["action_if_failed"] == "fail"
    assert rows_prod[0]["priority"] == "high"


def test_flatten_rules_list_dq_env_qa_env(minimal_dq_env_rules):
    rows = flatten_rules_list(minimal_dq_env_rules, env="QA")
    assert rows[0]["table_name"] == "catalog2.schema.orders"


def test_flatten_rules_list_dq_env_all_schema_columns(minimal_dq_env_rules):
    rows = flatten_rules_list(minimal_dq_env_rules, env="DEV")
    for col in RULES_SCHEMA_COLUMNS:
        assert col in rows[0], f"Missing column: {col}"


def test_flatten_rules_list_dq_env_rule_overrides_env_defaults():
    data = {
        "product_id": "prod1",
        "dq_env": {
            "DEV": {
                "table_name": "t1",
                "action_if_failed": "ignore",
                "priority": "medium",
            },
        },
        "rules": [
            {
                "rule": "r1",
                "rule_type": "row_dq",
                "expectation": "x > 0",
                "action_if_failed": "drop",
                "priority": "high",
            }
        ],
    }
    rows = flatten_rules_list(data, env="DEV")
    assert rows[0]["action_if_failed"] == "drop"
    assert rows[0]["priority"] == "high"


def test_flatten_rules_list_dq_env_case_insensitive_lookup(minimal_dq_env_rules):
    for env_value in ("DEV", "dev", "Dev", "dEv"):
        rows = flatten_rules_list(minimal_dq_env_rules, env=env_value)
        assert rows[0]["table_name"] == "catalog.schema.orders"


def test_flatten_rules_list_dq_env_lowercase_option_matches_uppercase_key():
    data = {
        "product_id": "prod1",
        "dq_env": {
            "DEV": {
                "table_name": "dev.orders",
                "priority": "medium",
            },
        },
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0"}
        ],
    }
    rows = flatten_rules_list(data, env="dev")
    assert rows[0]["table_name"] == "dev.orders"


def test_flatten_rules_list_dq_env_no_env_raises(minimal_dq_env_rules):
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="no environment"):
        flatten_rules_list(minimal_dq_env_rules)


def test_flatten_rules_list_dq_env_missing_env_raises(minimal_dq_env_rules):
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="not found"):
        flatten_rules_list(minimal_dq_env_rules, env="staging")


def test_flatten_rules_list_dq_env_empty_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="non-empty mapping"):
        flatten_rules_list({
            "product_id": "p1",
            "dq_env": {},
            "rules": [{"rule": "r1", "expectation": "x > 0"}],
        }, env="DEV")


def test_flatten_rules_list_dq_env_not_dict_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="non-empty mapping"):
        flatten_rules_list({
            "product_id": "p1",
            "dq_env": "not_a_dict",
            "rules": [{"rule": "r1", "expectation": "x > 0"}],
        }, env="DEV")


def test_flatten_rules_list_dq_env_with_multiple_rules():
    data = {
        "product_id": "prod1",
        "dq_env": {
            "DEV": {
                "table_name": "dev.orders",
                "action_if_failed": "ignore",
                "is_active": True,
                "priority": "medium",
            },
        },
        "rules": [
            {
                "rule": "r1",
                "rule_type": "row_dq",
                "expectation": "col1 IS NOT NULL",
                "action_if_failed": "drop",
                "priority": "high",
            },
            {
                "rule": "r2",
                "rule_type": "agg_dq",
                "expectation": "count(*) > 0",
                "action_if_failed": "fail",
            },
        ],
    }
    rows = flatten_rules_list(data, env="DEV")
    assert len(rows) == 2
    assert rows[0]["table_name"] == "dev.orders"
    assert rows[0]["action_if_failed"] == "drop"
    assert rows[0]["priority"] == "high"
    assert rows[1]["table_name"] == "dev.orders"
    assert rows[1]["action_if_failed"] == "fail"
    assert rows[1]["priority"] == "medium"


# ── helper coverage ────────────────────────────────────────────────────


def test_flatten_rules_list_non_dict_rule_entry_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="must be a dict"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": ["not_a_dict"],
        })


def test_flatten_rules_list_rules_not_a_list_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="non-empty"):
        flatten_rules_list({
            "product_id": "p1",
            "rules": "not_a_list",
        })


def test_flatten_rules_list_non_numeric_error_drop_threshold_raises():
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="expects an integer"):
        flatten_rules_list({
            "product_id": "p1",
            "table_name": "t1",
            "rules": [
                {
                    "rule": "r1",
                    "rule_type": "row_dq",
                    "expectation": "x > 0",
                    "error_drop_threshold": "abc",
                }
            ],
        })


# ── _cast_value coverage ───────────────────────────────────────────────


def test_cast_value_none_boolean_column():
    assert _cast_value("is_active", None) is True
    assert _cast_value("enable_error_drop_alert", None) is False


def test_cast_value_none_int_column():
    assert _cast_value("error_drop_threshold", None) == 0


def test_cast_value_none_string_column():
    assert _cast_value("description", None) == ""
    assert _cast_value("tag", None) == ""


def test_cast_value_boolean_from_string():
    assert _cast_value("is_active", "true") is True
    assert _cast_value("is_active", "True") is True
    assert _cast_value("is_active", "1") is True
    assert _cast_value("is_active", "yes") is True
    assert _cast_value("is_active", "false") is False
    assert _cast_value("is_active", "0") is False
    assert _cast_value("is_active", "no") is False


def test_cast_value_boolean_from_non_bool_non_str():
    assert _cast_value("is_active", 1) is True
    assert _cast_value("is_active", 0) is False


# ── lossy coercion warnings ────────────────────────────────────────────


def _warning_messages(mock_log):
    return [c.args[0] for c in mock_log.warning.call_args_list]


@pytest.mark.parametrize(
    "col, value",
    [
        ("is_active", True),
        ("is_active", False),
        ("is_active", None),
        ("is_active", "true"),
        ("is_active", "TRUE"),
        ("is_active", "Yes"),
        ("is_active", "1"),
        ("is_active", "false"),
        ("is_active", "No"),
        ("is_active", "0"),
        ("is_active", 1),
        ("is_active", 0),
        ("is_active", 1.0),
        ("enable_error_drop_alert", "yes"),
        ("error_drop_threshold", None),
        ("error_drop_threshold", 0),
        ("error_drop_threshold", 5),
        ("error_drop_threshold", 2.0),
        ("error_drop_threshold", -3.0),
        ("error_drop_threshold", "7"),
        ("error_drop_threshold", " 7 "),
        ("error_drop_threshold", True),
        ("error_drop_threshold", Decimal("3")),
        ("error_drop_threshold", "1.9"),  # rejected by _cast_value with an error
        ("error_drop_threshold", float("nan")),  # rejected by _cast_value with an error
        ("tag", "tru"),
    ],
)
def test_describe_lossy_coercion_accepts_recognised_values(col, value):
    assert _describe_lossy_coercion(col, value) is None


@pytest.mark.parametrize(
    "col, value, expected_cast, expected_fragment",
    [
        ("is_active", "tru", False, "unrecognized boolean string 'tru' is treated as False; use true or false"),
        ("is_active", " true ", False, "string ' true ' is treated as False; remove the surrounding whitespace"),
        ("is_active", "", False, "unrecognized boolean string '' is treated as False"),
        ("is_active", "on", False, "unrecognized boolean string 'on' is treated as False"),
        ("enable_for_source_dq_validation", "Y", False, "unrecognized boolean string 'Y'"),
        ("error_drop_threshold", 1.9, 1, "fractional value 1.9 is truncated to 1; use a whole number"),
        ("error_drop_threshold", -0.5, 0, "fractional value -0.5 is truncated to 0"),
        ("error_drop_threshold", Decimal("2.5"), 2, "fractional value Decimal('2.5') is truncated to 2"),
        ("error_drop_threshold", Fraction(7, 2), 3, "fractional value Fraction(7, 2) is truncated to 3"),
    ],
)
def test_describe_lossy_coercion_reports_without_changing_cast(col, value, expected_cast, expected_fragment):
    assert expected_fragment in _describe_lossy_coercion(col, value)
    # The existing normalisation result is unchanged.
    assert _cast_value(col, value) == expected_cast
    assert type(_cast_value(col, value)) is type(expected_cast)


def test_describe_lossy_coercion_truncates_long_values():
    message = _describe_lossy_coercion("is_active", "x" * 500)
    assert "..." in message
    assert len(message) < 300


def test_flatten_rule_level_unrecognised_boolean_warns_with_identity():
    data = {
        "product_id": "prod1",
        "table_name": "db.table1",
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "a > 0", "is_active": "tru"},
            {"rule": "r2", "rule_type": "row_dq", "expectation": "b > 0", "is_active": "true"},
        ],
    }
    with patch("spark_expectations.rules.plugins._flatten._log") as mock_log:
        rows = flatten_rules_list(data)

    assert rows[0]["is_active"] is False
    assert rows[1]["is_active"] is True
    messages = _warning_messages(mock_log)
    assert messages == [
        "Rules file field 'is_active' for rule 'r1' (product_id='prod1', table_name='db.table1', "
        "set in rule definition): unrecognized boolean string 'tru' is treated as False; use true or false "
        "(recognized strings, case-insensitive: 'true', '1', 'yes', 'false', '0', 'no')."
    ]


def test_flatten_dq_env_inherited_values_warn_once_per_rule_and_field():
    data = {
        "product_id": "prod1",
        "dq_env": {
            "Prod": {
                "table_name": "cat.sch.orders",
                "is_active": " true ",
                "error_drop_threshold": 1.9,
            }
        },
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "a > 0"},
            {"rule": "r2", "rule_type": "agg_dq", "expectation": "count(*) > 0"},
        ],
    }
    with patch("spark_expectations.rules.plugins._flatten._log") as mock_log:
        rows = flatten_rules_list(data, env="PROD")

    assert [r["is_active"] for r in rows] == [False, False]
    assert [r["error_drop_threshold"] for r in rows] == [1, 1]
    messages = _warning_messages(mock_log)
    assert len(messages) == 4
    for rule in ("r1", "r2"):
        rule_messages = [m for m in messages if f"for rule '{rule}'" in m]
        assert len(rule_messages) == 2
        assert all("table_name='cat.sch.orders', set in dq_env['Prod']" in m for m in rule_messages)
        assert any("'is_active'" in m and "' true '" in m for m in rule_messages)
        assert any("'error_drop_threshold'" in m and "1.9 is truncated to 1" in m for m in rule_messages)


def test_flatten_top_level_defaults_warn_only_where_inherited():
    data = {
        "product_id": "prod1",
        "table_name": "db.table1",
        "defaults": {"is_active": "tru"},
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "a > 0", "is_active": True},
            {"rule": "r2", "rule_type": "row_dq", "expectation": "b > 0"},
            {"rule": "r3", "rule_type": "row_dq", "expectation": "c > 0", "is_active": None},
        ],
    }
    with patch("spark_expectations.rules.plugins._flatten._log") as mock_log:
        rows = flatten_rules_list(data)

    assert [r["is_active"] for r in rows] == [True, False, True]
    messages = _warning_messages(mock_log)
    assert len(messages) == 1
    assert "for rule 'r2'" in messages[0]
    assert "set in defaults" in messages[0]


def test_flatten_dq_env_shared_defaults_source_is_reported():
    data = {
        "product_id": "prod1",
        "defaults": {"enable_error_drop_alert": "ture"},
        "dq_env": {"DEV": {"table_name": "db.t"}},
        "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "a > 0"}],
    }
    with patch("spark_expectations.rules.plugins._flatten._log") as mock_log:
        rows = flatten_rules_list(data, env="dev")

    assert rows[0]["enable_error_drop_alert"] is False
    messages = _warning_messages(mock_log)
    assert len(messages) == 1
    assert "'enable_error_drop_alert' for rule 'r1'" in messages[0]
    assert "set in defaults" in messages[0]


def test_flatten_valid_files_do_not_warn(minimal_rules_list, minimal_dq_env_rules):
    with patch("spark_expectations.rules.plugins._flatten._log") as mock_log:
        flatten_rules_list(minimal_rules_list)
        flatten_rules_list(minimal_dq_env_rules, env="PROD")
        flatten_rules_list({
            "product_id": "p1",
            "table_name": "t1",
            "rules": [
                {
                    "rule": "r1",
                    "rule_type": "row_dq",
                    "expectation": "x > 0",
                    "is_active": "Yes",
                    "enable_error_drop_alert": "0",
                    "enable_querydq_custom_output": None,
                    "error_drop_threshold": 10.0,
                },
                {"rule": "r2", "rule_type": "row_dq", "expectation": "x > 0", "error_drop_threshold": "25"},
            ],
        })
    mock_log.warning.assert_not_called()


def test_flatten_rejected_threshold_still_raises_without_warning():
    with patch("spark_expectations.rules.plugins._flatten._log") as mock_log:
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="expects an integer"):
            flatten_rules_list({
                "product_id": "p1",
                "table_name": "t1",
                "rules": [
                    {"rule": "r1", "rule_type": "row_dq", "expectation": "x > 0", "error_drop_threshold": "1.9"}
                ],
            })
    mock_log.warning.assert_not_called()


def test_yaml_and_json_files_produce_identical_rows_and_warnings(tmp_path):
    yaml_path = tmp_path / "rules.yaml"
    yaml_path.write_text(textwrap.dedent("""\
        product_id: prod1
        dq_env:
          PROD:
            table_name: cat.sch.orders
            is_active: "tru"
            error_drop_threshold: 1.9
        rules:
          - rule: r1
            rule_type: row_dq
            expectation: "a > 0"
          - rule: r2
            rule_type: row_dq
            expectation: "b > 0"
            is_active: "Yes"
            enable_error_drop_alert: " true "
            error_drop_threshold: 3
    """), encoding="utf-8")
    json_path = tmp_path / "rules.json"
    json_path.write_text(json.dumps({
        "product_id": "prod1",
        "dq_env": {
            "PROD": {"table_name": "cat.sch.orders", "is_active": "tru", "error_drop_threshold": 1.9},
        },
        "rules": [
            {"rule": "r1", "rule_type": "row_dq", "expectation": "a > 0"},
            {
                "rule": "r2",
                "rule_type": "row_dq",
                "expectation": "b > 0",
                "is_active": "Yes",
                "enable_error_drop_alert": " true ",
                "error_drop_threshold": 3,
            },
        ],
    }), encoding="utf-8")

    with patch("spark_expectations.rules.plugins._flatten._log") as yaml_log:
        yaml_rows = flatten_rules_list(_read_yaml(str(yaml_path)), env="PROD")
    with patch("spark_expectations.rules.plugins._flatten._log") as json_log:
        json_rows = flatten_rules_list(_read_json(str(json_path)), env="PROD")

    assert yaml_rows == json_rows
    assert [(r["is_active"], r["enable_error_drop_alert"], r["error_drop_threshold"]) for r in yaml_rows] == [
        (False, False, 1),
        (True, False, 3),
    ]
    yaml_messages = _warning_messages(yaml_log)
    assert yaml_messages == _warning_messages(json_log)
    assert len(yaml_messages) == 3
    assert sum("for rule 'r1'" in m and "set in dq_env['PROD']" in m for m in yaml_messages) == 2
    assert sum("for rule 'r2'" in m and "set in rule definition" in m for m in yaml_messages) == 1


@pytest.mark.parametrize("value", [Fraction(1, 10**5000), Decimal("1" + "0" * 5000 + ".5")],
                         ids=["tiny_fraction", "large_decimal"])
def test_warning_representation_cannot_reject_numeric_input(value):
    data = {
        "product_id": "p1",
        "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "a > 0", "error_drop_threshold": value}],
    }
    with patch("spark_expectations.rules.plugins._flatten._log") as mock_log:
        rows = flatten_rules_list(data)
    assert rows[0]["error_drop_threshold"] == int(value)
    messages = _warning_messages(mock_log)
    assert len(messages) == 1
    assert "<unrepresentable value>" in messages[0]
    assert len(messages[0]) < 500


@pytest.mark.parametrize("suffix", ["\nFORGED", "\r\tCONTROL"])
@pytest.mark.parametrize("length", [2, 10000])
def test_warning_environment_provenance_is_bounded_and_escaped(tmp_path, suffix, length):
    import yaml

    env = "E" * length + suffix
    data = {
        "product_id": "p1",
        "dq_env": {env: {"table_name": "t1", "is_active": "tru"}},
        "rules": [{"rule": "r1", "rule_type": "row_dq", "expectation": "a > 0"}],
    }
    json_path = tmp_path / "rules.json"
    json_path.write_text(json.dumps(data), encoding="utf-8")
    yaml_path = tmp_path / "rules.yaml"
    yaml_path.write_text(yaml.safe_dump(data), encoding="utf-8")
    messages = []
    for document in (_read_json(str(json_path)), _read_yaml(str(yaml_path))):
        with patch("spark_expectations.rules.plugins._flatten._log") as mock_log:
            rows = flatten_rules_list(document, env=env.lower())
        assert rows[0]["is_active"] is False
        warning, = _warning_messages(mock_log)
        assert "set in dq_env[" in warning
        assert len(warning) < 500
        assert not any(character in warning for character in "\n\r\t")
        messages.append(warning)
    assert messages[0] == messages[1]
