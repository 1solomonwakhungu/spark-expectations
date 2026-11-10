"""Dimension-tag validation through the real rule loaders and SparkExpectations.with_expectations.

The same logical rules are supplied as a DataFrame, a YAML file and a JSON file;
all three must produce identical diagnostics and keep their tags unchanged.
"""

import json
import logging

import pytest
import yaml
from pyspark.sql import SparkSession

from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.core.expectations import SparkExpectations, WrappedDataFrameWriter
from spark_expectations.rules import load_rules, load_rules_from_json, load_rules_from_yaml
from spark_expectations.rules.plugins._flatten import RULES_SCHEMA_COLUMNS, rules_schema
from spark_expectations.utils import rule_dimensions

STRICT_KEY = user_config.se_rules_dimension_validation_strict
PRODUCT = "dim_product"
TARGET = "dim_db.orders"

# (rule, tag, extra overrides). Tags that are omitted entirely use the sentinel _MISSING.
_MISSING = object()
RULE_SPECS = [
    ("r_accuracy", "accuracy", {}),
    ("r_completeness", "completeness", {}),
    ("r_consistency", "consistency", {}),
    ("r_timeliness", "timeliness", {}),
    ("r_uniqueness", "uniqueness", {}),
    ("r_validity", "validity", {}),
    ("r_empty", "", {}),
    ("r_null", None, {}),
    ("r_missing", _MISSING, {}),
    ("r_typo", "completness", {}),
    ("r_custom", "strict", {}),
    ("r_case", "Validity", {}),
    ("r_space", " validity", {}),
    ("r_inactive_typo", "validty", {"is_active": False}),
    ("r_other_table", "custom", {"table_name": "dim_db.other"}),
]
REPORTED_AT_RUNTIME = ["r_typo", "r_custom", "r_case", "r_space"]


def _rule_defs():
    defs = []
    for name, tag, extra in RULE_SPECS:
        rule = {
            "rule": name,
            "rule_type": "row_dq",
            "column_name": "col1",
            "expectation": "col1 IS NOT NULL",
            "action_if_failed": "ignore",
        }
        if tag is not _MISSING:
            rule["tag"] = tag
        rule.update(extra)
        defs.append(rule)
    return defs


def _file_payload():
    return {"product_id": PRODUCT, "table_name": TARGET, "rules": _rule_defs()}


@pytest.fixture(scope="module")
def dim_spark():
    # Reuse the suite's session when one is active; otherwise start a plain local one.
    session = SparkSession.getActiveSession()
    if session is None:
        session = (
            SparkSession.builder.master("local[1]")
            .appName("dimension-tag-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    return session


@pytest.fixture
def dimension_log(caplog):
    """Capture records from the validator logger (the package logger does not propagate)."""
    module_logger = logging.getLogger(rule_dimensions.__name__)
    module_logger.addHandler(caplog.handler)
    caplog.set_level(logging.WARNING, logger=rule_dimensions.__name__)
    yield caplog
    module_logger.removeHandler(caplog.handler)


@pytest.fixture
def yaml_path(tmp_path):
    path = tmp_path / "rules.yaml"
    path.write_text(yaml.safe_dump(_file_payload(), sort_keys=False), encoding="utf-8")
    return str(path)


@pytest.fixture
def json_path(tmp_path):
    path = tmp_path / "rules.json"
    path.write_text(json.dumps(_file_payload()), encoding="utf-8")
    return str(path)


def _dataframe_rules(spark):
    rows = []
    for rule in _rule_defs():
        row = {
            "product_id": PRODUCT,
            "table_name": rule.get("table_name", TARGET),
            "rule_type": rule["rule_type"],
            "rule": rule["rule"],
            "column_name": rule["column_name"],
            "expectation": rule["expectation"],
            "action_if_failed": rule["action_if_failed"],
            "tag": rule.get("tag"),
            "description": "",
            "enable_for_source_dq_validation": True,
            "enable_for_target_dq_validation": True,
            "is_active": rule.get("is_active", True),
            "enable_error_drop_alert": False,
            "error_drop_threshold": 0,
            "query_dq_delimiter": None,
            "enable_querydq_custom_output": False,
            "priority": "medium",
        }
        rows.append(tuple(row[c] for c in RULES_SCHEMA_COLUMNS))
    return spark.createDataFrame(rows, schema=rules_schema())


def _rules_df(source, spark, yaml_path, json_path):
    if source == "dataframe":
        return _dataframe_rules(spark)
    if source == "yaml":
        return load_rules_from_yaml(yaml_path, spark)
    return load_rules_from_json(json_path, spark)


def _decorate(rules_df, user_conf):
    writer = WrappedDataFrameWriter().mode("append").format("parquet")
    se = SparkExpectations(
        product_id=PRODUCT,
        rules_df=rules_df,
        stats_table="dim_db.dq_stats",
        stats_table_writer=writer,
        target_and_error_table_writer=writer,
    )
    conf = {user_config.se_enable_streaming: False, user_config.is_serverless: True}
    conf.update(user_conf)

    @se.with_expectations(target_table=TARGET, user_conf=conf)
    def build():  # pragma: no cover - never executed by these tests
        raise AssertionError("decorated function must not run")

    return se, build


SOURCES = ["dataframe", "yaml", "json"]


@pytest.mark.parametrize("source", SOURCES)
def test_default_mode_warns_once_per_rule_and_keeps_rules(source, dim_spark, yaml_path, json_path, dimension_log):
    rules_df = _rules_df(source, dim_spark, yaml_path, json_path)
    assert dimension_log.records == [], "loaders must not warn in default mode"

    se, build = _decorate(rules_df, {})

    assert callable(build)
    messages = [r.getMessage() for r in dimension_log.records]
    assert len(messages) == len(REPORTED_AT_RUNTIME)
    for name, message in zip(REPORTED_AT_RUNTIME, messages):
        assert message.startswith(f"Rule '{name}' (product_id='{PRODUCT}', table_name='{TARGET}', rule_type='row_dq')")
    assert "Did you mean 'completeness'?" in messages[0]

    selected = {r["rule"]: r["tag"] for r in se._context.get_dq_expectations["row_dq_rules"]}
    active_target_rules = [n for n, _, extra in RULE_SPECS if not extra]
    assert sorted(selected) == sorted(active_target_rules)
    assert selected["r_typo"] == "completness"
    assert selected["r_custom"] == "strict"
    assert selected["r_case"] == "Validity"
    assert selected["r_space"] == " validity"
    assert selected["r_validity"] == "validity"


def test_dataframe_yaml_json_produce_identical_diagnostics(dim_spark, yaml_path, json_path, dimension_log):
    per_source = {}
    for source in SOURCES:
        dimension_log.clear()
        _decorate(_rules_df(source, dim_spark, yaml_path, json_path), {})
        per_source[source] = [r.getMessage() for r in dimension_log.records]

    assert per_source["dataframe"] == per_source["yaml"] == per_source["json"]
    assert len(per_source["dataframe"]) == len(REPORTED_AT_RUNTIME)


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("strict_value", [True, "true"])
def test_strict_mode_raises_at_decoration(source, strict_value, dim_spark, yaml_path, json_path, dimension_log):
    rules_df = _rules_df(source, dim_spark, yaml_path, json_path)

    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
        _decorate(rules_df, {STRICT_KEY: strict_value})

    message = str(exc_info.value)
    assert f"{len(REPORTED_AT_RUNTIME)} rule(s) have an unrecognized dimension tag" in message
    for name in REPORTED_AT_RUNTIME:
        assert f"Rule '{name}' (product_id='{PRODUCT}', table_name='{TARGET}'" in message
    assert "r_inactive_typo" not in message
    assert "r_other_table" not in message
    assert dimension_log.records == []


def test_strict_mode_passes_when_tags_are_allowed_or_unset(dim_spark, tmp_path, dimension_log):
    payload = {
        "product_id": PRODUCT,
        "table_name": TARGET,
        "rules": [r for r in _rule_defs() if r["rule"] not in REPORTED_AT_RUNTIME],
    }
    path = tmp_path / "clean.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    rules_df = load_rules_from_yaml(str(path), dim_spark)

    _, build = _decorate(rules_df, {STRICT_KEY: True})

    assert callable(build)
    assert dimension_log.records == []


def test_strict_mode_from_spark_conf(dim_spark, yaml_path, json_path):
    rules_df = _rules_df("dataframe", dim_spark, yaml_path, json_path)
    dim_spark.conf.set(STRICT_KEY, "true")
    try:
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException):
            # serverless=False so the Spark session configuration is consulted
            _decorate(rules_df, {user_config.is_serverless: False})
    finally:
        dim_spark.conf.unset(STRICT_KEY)


def test_invalid_strict_setting_is_rejected(dim_spark, yaml_path, json_path):
    rules_df = _rules_df("dataframe", dim_spark, yaml_path, json_path)
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
        _decorate(rules_df, {STRICT_KEY: "yes"})
    assert STRICT_KEY in str(exc_info.value)


@pytest.mark.parametrize("loader", ["yaml", "json", "auto"])
def test_loader_strict_option_raises_for_whole_file(loader, dim_spark, yaml_path, json_path, dimension_log):
    options = {STRICT_KEY: "true"}
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
        if loader == "yaml":
            load_rules_from_yaml(yaml_path, dim_spark, options=options)
        elif loader == "json":
            load_rules_from_json(json_path, dim_spark, options=options)
        else:
            load_rules(yaml_path, dim_spark, options=options)

    message = str(exc_info.value)
    # The loader checks every rule in the file, including inactive rules and other tables.
    for name in REPORTED_AT_RUNTIME + ["r_inactive_typo", "r_other_table"]:
        assert f"Rule '{name}'" in message
    assert "table_name='dim_db.other'" in message
    assert dimension_log.records == []


@pytest.mark.parametrize("options", [None, {STRICT_KEY: "false"}, {STRICT_KEY: False}])
def test_loader_without_strict_keeps_every_rule_and_tag(options, dim_spark, yaml_path, json_path, dimension_log):
    for df in (
        load_rules_from_yaml(yaml_path, dim_spark, options=options),
        load_rules_from_json(json_path, dim_spark, options=options),
    ):
        tags = {row["rule"]: row["tag"] for row in df.collect()}
        assert len(tags) == len(RULE_SPECS)
        assert tags["r_typo"] == "completness"
        assert tags["r_case"] == "Validity"
        assert tags["r_space"] == " validity"
        assert tags["r_null"] == ""
        assert tags["r_missing"] == ""
    assert dimension_log.records == []


def test_loader_rejects_invalid_strict_option(dim_spark, yaml_path):
    with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException) as exc_info:
        load_rules_from_yaml(yaml_path, dim_spark, options={STRICT_KEY: "maybe"})
    assert STRICT_KEY in str(exc_info.value)
