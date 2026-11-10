"""Unit tests for spark_expectations.core.rule_validation_policy (no Spark session)."""
import copy
from unittest.mock import Mock

import pytest

from spark_expectations.core.context import SparkExpectationsContext
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.core.rule_validation_policy import (
    FAIL,
    SKIP_INVALID,
    WARN_AND_RUN,
    RuleIdentity,
    compute_execution_settings,
    evaluate_policy,
    install_rule_counters,
    normalize_policy,
    resolve_policy,
    secondary_query_key,
)
from spark_expectations.utils.validate_rules import ValidationResult


def _rule(name, rule_type="row_dq", expectation="ok", src=True, tgt=False, product="p1", table="db.t1"):
    return {
        "product_id": product,
        "table_name": table,
        "rule_type": rule_type,
        "rule": name,
        "column_name": "c",
        "expectation": expectation,
        "action_if_failed": "ignore",
        "enable_for_source_dq_validation": src,
        "enable_for_target_dq_validation": tgt,
        "enable_querydq_custom_output": False,
    }


def _expectations(*rules, target="db.t1"):
    result = {}
    for rule in rules:
        result.setdefault(f"{rule['rule_type']}_rules", []).append(rule)
    result["target_table_name"] = target
    return result


def _validator(calls=None):
    """Fake validator: a rule is invalid when its expectation starts with 'bad'."""

    def _validate(rule):
        if calls is not None:
            calls.append(rule)
        if str(rule.get("expectation", "")).startswith("bad"):
            rule_type = rule.get("rule_type", "unknown")
            return {rule_type: [ValidationResult(False, f"bad expectation for {rule.get('rule')}", rule, rule_type)]}
        if rule.get("rule_type") not in ("row_dq", "agg_dq", "query_dq"):
            rule_type = rule.get("rule_type", "unknown")
            return {rule_type: [ValidationResult(False, f"Unknown rule_type: '{rule_type}'", rule, rule_type)]}
        return {}

    return _validate


SETTINGS_ALL = {
    "row_dq": True,
    "source_agg_dq": True,
    "source_query_dq": True,
    "target_agg_dq": True,
    "target_query_dq": True,
}


def _mixed():
    return _expectations(
        _rule("row_ok"),
        _rule("row_bad", expectation="bad sum(x) > 0"),
        _rule("agg_ok", "agg_dq", "sum(c) > 0", src=True, tgt=True),
        _rule("agg_bad", "agg_dq", "bad c > 0", src=True, tgt=True),
        _rule("q_ok", "query_dq", "select 1", src=True, tgt=True),
        _rule("q_bad", "query_dq", "bad SELEC", src=True, tgt=True),
    )


def _queries():
    return {
        "p1|db.t1|q_ok": {"source_f1": "select 1"},
        "p1|db.t1|q_bad": {"source_f1": "SELEC"},
    }


class TestNormalizePolicy:
    def test_absent_value_is_warn_and_run(self):
        assert normalize_policy(None) == WARN_AND_RUN

    @pytest.mark.parametrize("value", [None, "", "  ", "null", "None", "NULL"])
    def test_present_null_or_empty_values_raise(self, value):
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="rules.validation.policy"):
            normalize_policy(value, explicit=True)

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("warn_and_run", WARN_AND_RUN),
            ("Warn-And-Run", WARN_AND_RUN),
            ("skip_invalid", SKIP_INVALID),
            ("skip-invalid", SKIP_INVALID),
            (" FAIL ", FAIL),
        ],
    )
    def test_spellings(self, value, expected):
        assert normalize_policy(value) == expected

    @pytest.mark.parametrize("value", ["maybe", "skip", "true", 1, True, ["fail"], ""])
    def test_unknown_values_raise(self, value):
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException, match="Invalid rule validation policy"):
            normalize_policy(value)


class TestResolvePolicy:
    KEY = "spark.expectations.rules.validation.policy"

    @staticmethod
    def _session(values):
        return lambda key: values.get(key)

    def test_unset_everywhere_is_default(self):
        assert resolve_policy(None, self._session({})) == WARN_AND_RUN
        assert resolve_policy({}, self._session({})) == WARN_AND_RUN

    def test_user_conf_wins_over_session(self):
        assert resolve_policy({self.KEY: "skip_invalid"}, self._session({self.KEY: "fail"})) == SKIP_INVALID

    def test_session_value_used_when_user_conf_lacks_key(self):
        assert resolve_policy({"other": 1}, self._session({self.KEY: "FAIL"})) == FAIL

    @pytest.mark.parametrize("raw", ["null", "None", "", "true", "maybe"])
    def test_raw_session_values_are_validated(self, raw):
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException):
            resolve_policy(None, self._session({self.KEY: raw}))

    @pytest.mark.parametrize("raw", [None, "null", "None", ""])
    def test_explicit_user_conf_null_values_raise(self, raw):
        with pytest.raises(SparkExpectationsUserInputOrConfigInvalidException):
            resolve_policy({self.KEY: raw}, self._session({self.KEY: "fail"}))

    def test_serverless_ignores_session_conf(self):
        session = Mock(side_effect=AssertionError("session conf must not be read"))
        conf = {"spark.expectations.is.serverless": True}
        assert resolve_policy(conf, session) == WARN_AND_RUN
        assert resolve_policy({**conf, self.KEY: "fail"}, session) == FAIL


class TestWarnAndRun:
    def test_runs_unchanged_rule_set_and_reports_invalid(self):
        expectations, queries = _mixed(), _queries()
        outcome = evaluate_policy(WARN_AND_RUN, expectations, queries, SETTINGS_ALL, _validator())

        assert outcome.expectations == expectations
        assert outcome.dq_queries == queries
        assert outcome.execution_settings == SETTINGS_ALL
        assert [i.identity.rule for i in outcome.invalid_rules] == ["row_bad", "agg_bad", "q_bad"]
        assert outcome.skipped_rules == []
        assert outcome.stats == {"num_invalid_rules": 3, "num_skipped_rules": 0}
        assert not outcome.should_fail
        assert outcome.validated_rule_count == 6

    def test_no_invalid_rules(self):
        expectations = _expectations(_rule("a"), _rule("b"))
        outcome = evaluate_policy(WARN_AND_RUN, expectations, {}, {"row_dq": True}, _validator())
        assert outcome.invalid_rules == []
        assert outcome.stats == {"num_invalid_rules": 0, "num_skipped_rules": 0}


class TestSkipInvalid:
    def test_mixed_rule_types(self):
        expectations, queries = _mixed(), _queries()
        outcome = evaluate_policy(SKIP_INVALID, expectations, queries, SETTINGS_ALL, _validator())

        assert [r["rule"] for r in outcome.expectations["row_dq_rules"]] == ["row_ok"]
        assert [r["rule"] for r in outcome.expectations["agg_dq_rules"]] == ["agg_ok"]
        assert [r["rule"] for r in outcome.expectations["query_dq_rules"]] == ["q_ok"]
        assert outcome.expectations["target_table_name"] == "db.t1"
        assert outcome.dq_queries == {"p1|db.t1|q_ok": {"source_f1": "select 1"}}
        assert outcome.execution_settings == SETTINGS_ALL
        assert outcome.stats == {"num_invalid_rules": 3, "num_skipped_rules": 3}
        assert [i.identity.rule for i in outcome.skipped_rules] == ["row_bad", "agg_bad", "q_bad"]

    def test_all_rules_of_a_type_invalid_clears_its_flags(self):
        expectations = _expectations(
            _rule("row_ok"),
            _rule("agg_bad_1", "agg_dq", "bad x", src=True, tgt=True),
            _rule("agg_bad_2", "agg_dq", "bad y", src=True, tgt=True),
        )
        settings = {**SETTINGS_ALL, "source_query_dq": False, "target_query_dq": False}
        outcome = evaluate_policy(SKIP_INVALID, expectations, {}, settings, _validator())

        assert "agg_dq_rules" not in outcome.expectations
        assert outcome.execution_settings == {
            "row_dq": True,
            "source_agg_dq": False,
            "source_query_dq": False,
            "target_agg_dq": False,
            "target_query_dq": False,
        }

    def test_all_rules_invalid(self):
        expectations = _expectations(_rule("r", expectation="bad"), _rule("a", "agg_dq", "bad"))
        outcome = evaluate_policy(SKIP_INVALID, expectations, {}, SETTINGS_ALL, _validator())
        assert outcome.expectations == {"target_table_name": "db.t1"}
        assert not any(outcome.execution_settings.values())
        assert outcome.stats == {"num_invalid_rules": 2, "num_skipped_rules": 2}

    def test_unknown_rule_type_is_validated_and_skipped(self):
        expectations = _expectations(_rule("row_ok"), _rule("odd", "nope"))
        calls = []
        outcome = evaluate_policy(SKIP_INVALID, expectations, {}, {"row_dq": True}, _validator(calls))

        assert [c["rule"] for c in calls] == ["row_ok", "odd"]
        assert "nope_rules" not in outcome.expectations
        assert outcome.invalid_rules[0].identity.rule_type == "nope"
        assert "Unknown rule_type" in outcome.invalid_rules[0].errors[0]

    def test_missing_rule_type_falls_back_to_bucket(self):
        odd = _rule("odd")
        del odd["rule_type"]
        expectations = {"mystery_rules": [odd], "target_table_name": "db.t1"}
        outcome = evaluate_policy(SKIP_INVALID, expectations, {}, {}, _validator())
        assert outcome.invalid_rules[0].identity.rule_type == "mystery"
        assert outcome.expectations == {"target_table_name": "db.t1"}

    def test_duplicate_rules_are_counted_per_occurrence(self):
        expectations = _expectations(
            _rule("dup", expectation="bad 1"),
            _rule("dup", expectation="bad 1"),
            _rule("dup", expectation="c > 0"),
        )
        outcome = evaluate_policy(SKIP_INVALID, expectations, {}, {"row_dq": True}, _validator())

        assert outcome.stats == {"num_invalid_rules": 2, "num_skipped_rules": 2}
        assert len({i.identity for i in outcome.invalid_rules}) == 2
        assert [r["expectation"] for r in outcome.expectations["row_dq_rules"]] == ["c > 0"]

    def test_shared_secondary_query_key_is_kept_for_valid_rule(self):
        expectations = _expectations(
            _rule("q", "query_dq", "bad SELEC"),
            _rule("q", "query_dq", "select 1"),
        )
        queries = {"p1|db.t1|q": {"source_f1": "select 1"}}
        outcome = evaluate_policy(SKIP_INVALID, expectations, queries, SETTINGS_ALL, _validator())
        assert outcome.dq_queries == queries
        assert len(outcome.expectations["query_dq_rules"]) == 1

    def test_string_flags_follow_spark_boolean_cast(self):
        expectations = _expectations(
            _rule("a", "agg_dq", "sum(c) > 0", src="True", tgt="False"),
            _rule("bad", "query_dq", "bad", src="true", tgt="true"),
        )
        outcome = evaluate_policy(SKIP_INVALID, expectations, {}, SETTINGS_ALL, _validator())
        assert outcome.execution_settings["source_agg_dq"] is True
        assert outcome.execution_settings["target_agg_dq"] is False
        assert outcome.execution_settings["source_query_dq"] is False


class TestFail:
    def test_fail_reports_every_invalid_rule(self):
        outcome = evaluate_policy(FAIL, _mixed(), _queries(), SETTINGS_ALL, _validator())
        assert outcome.should_fail
        assert outcome.skipped_rules == []
        message = outcome.failure_message()
        assert message.startswith("3 rule(s) failed validation")
        assert '["p1", "db.t1", "row_dq", "row_bad", 1]' in message
        assert "bad expectation for q_bad" in message

    def test_fail_without_invalid_rules_does_not_fail(self):
        outcome = evaluate_policy(FAIL, _expectations(_rule("ok")), {}, {"row_dq": True}, _validator())
        assert not outcome.should_fail

    def test_unknown_type_fails(self):
        outcome = evaluate_policy(FAIL, _expectations(_rule("odd", "nope")), {}, {}, _validator())
        assert outcome.should_fail


class TestIdentity:
    def test_delimiter_components_do_not_collide(self):
        first = _rule("d", expectation="bad", product="a|b", table="c")
        second = _rule("d", expectation="bad", product="a", table="b|c")
        outcome = evaluate_policy(FAIL, {"row_dq_rules": [first, second]}, {}, {}, _validator())
        components = {i.identity[:4] for i in outcome.invalid_rules}
        assert components == {("a|b", "c", "row_dq", "d"), ("a", "b|c", "row_dq", "d")}
        same_position = [RuleIdentity("a|b", "c", "row_dq", "d", 0), RuleIdentity("a", "b|c", "row_dq", "d", 0)]
        assert len({i.display() for i in same_position}) == 2

    def test_formatted_rule_name_is_used(self):
        # The reader formats names with params before the policy sees them.
        outcome = evaluate_policy(FAIL, _expectations(_rule("dev_check", expectation="bad")), {}, {}, _validator())
        assert outcome.invalid_rules[0].identity.rule == "dev_check"

    def test_secondary_query_key_matches_reader_format(self):
        assert secondary_query_key(_rule("q", "query_dq")) == "p1|db.t1|q"


class TestRobustness:
    def test_inputs_are_never_mutated_and_runs_repeat(self):
        expectations, queries = _mixed(), _queries()
        settings = dict(SETTINGS_ALL)
        snapshot = copy.deepcopy((expectations, queries, settings))

        def _mutating_validator(rule):
            result = _validator()(rule)
            rule["expectation"] = "mutated"
            return result

        first = evaluate_policy(SKIP_INVALID, expectations, queries, settings, _mutating_validator)
        first.expectations["row_dq_rules"].clear()
        first.dq_queries.clear()
        second = evaluate_policy(SKIP_INVALID, expectations, queries, settings, _validator())

        assert (expectations, queries, settings) == snapshot
        assert [r["rule"] for r in second.expectations["row_dq_rules"]] == ["row_ok"]
        assert second.dq_queries == {"p1|db.t1|q_ok": {"source_f1": "select 1"}}

    def test_validator_exception_counts_as_invalid(self):
        def _boom(rule):
            raise RuntimeError("plan failed")

        outcome = evaluate_policy(SKIP_INVALID, _expectations(_rule("r")), {}, {"row_dq": True}, _boom)
        assert outcome.stats == {"num_invalid_rules": 1, "num_skipped_rules": 1}
        assert "RuntimeError: plan failed" in outcome.invalid_rules[0].errors[0]

    def test_valid_entries_in_result_map_are_ignored(self):
        def _valid_entries(rule):
            return {"row_dq": [ValidationResult(True, None, rule, "row_dq")], "other": []}

        outcome = evaluate_policy(FAIL, _expectations(_rule("r")), {}, {"row_dq": True}, _valid_entries)
        assert outcome.invalid_rules == []

    def test_non_list_result_entries(self):
        def _single(rule):
            return {"row_dq": ValidationResult(False, "nope", rule, "row_dq")}

        outcome = evaluate_policy(FAIL, _expectations(_rule("r")), {}, {}, _single)
        assert outcome.invalid_rules[0].errors == ("nope",)


class TestSettingsAndCounters:
    def test_compute_execution_settings(self):
        expectations = _expectations(
            _rule("r"),
            _rule("a", "agg_dq", src=False, tgt=True),
            _rule("q", "query_dq", src=True, tgt=None),
        )
        assert compute_execution_settings(expectations) == {
            "row_dq": True,
            "source_agg_dq": False,
            "source_query_dq": True,
            "target_agg_dq": True,
            "target_query_dq": False,
        }

    def test_install_rule_counters_resets_and_recounts(self):
        context = SparkExpectationsContext(product_id="p1", spark=Mock())
        for _ in range(5):
            context.set_num_row_dq_rules()
        expectations = _expectations(
            _rule("r1"),
            _rule("r2"),
            _rule("a", "agg_dq", src=True, tgt=False),
            _rule("q", "query_dq", src=True, tgt=True),
            _rule("odd", "nope"),
        )
        install_rule_counters(context, expectations)

        assert context.get_num_row_dq_rules == 2
        assert context.get_num_dq_rules == 4
        assert context.get_num_agg_dq_rules == {
            "num_agg_dq_rules": 1,
            "num_source_agg_dq_rules": 1,
            "num_final_agg_dq_rules": 0,
        }
        assert context.get_num_query_dq_rules == {
            "num_query_dq_rules": 1,
            "num_source_query_dq_rules": 1,
            "num_final_query_dq_rules": 1,
        }

    def test_context_policy_result_round_trip(self):
        context = SparkExpectationsContext(product_id="p1", spark=Mock())
        assert context.get_invalid_rule_stats is None
        context.set_rules_validation_policy_result(SKIP_INVALID, {"num_invalid_rules": 1}, ["x"], ["x"])
        stats = context.get_invalid_rule_stats
        stats["num_invalid_rules"] = 99
        assert context.get_invalid_rule_stats == {"num_invalid_rules": 1}
        assert context.get_rules_validation_policy == SKIP_INVALID
        assert context.get_invalid_rule_ids == ["x"]
        assert context.get_skipped_rule_ids == ["x"]
