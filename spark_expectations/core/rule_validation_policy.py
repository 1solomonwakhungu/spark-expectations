"""Runtime policy for rules that fail validation.

The policy is read from ``spark.expectations.rules.validation.policy``:

* ``warn_and_run`` (default): log invalid rules and execute every selected rule,
  exactly as earlier releases did.
* ``skip_invalid``: remove invalid rules from the current run before any rule
  executes. Removed rules are counted as skipped, never as passed.
* ``fail``: stop the run before any rule executes and before the input
  DataFrame is counted.

Only the rules selected for execution are validated: the rules returned by
``SparkExpectationsReader.get_rules_from_df`` for the target table, after the
``is_active`` filter and after ``params`` formatting. Buckets for unknown rule
types are validated as well, so they are reported (and skipped or failed) rather
than silently dropped.

Everything here works on copies. The expectations, secondary queries and
execution settings captured when the decorator was applied are never mutated, so
every call of a decorated function starts from the same rule set.
"""

import copy
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, NamedTuple, Optional, Tuple

from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException

WARN_AND_RUN = "warn_and_run"
SKIP_INVALID = "skip_invalid"
FAIL = "fail"

DEFAULT_POLICY = WARN_AND_RUN

POLICY_CONFIG_KEY = user_config.se_rules_validation_policy
SERVERLESS_CONFIG_KEY = user_config.is_serverless
SUPPORTED_POLICIES: Tuple[str, ...] = (WARN_AND_RUN, SKIP_INVALID, FAIL)

ROW_DQ = "row_dq"
AGG_DQ = "agg_dq"
QUERY_DQ = "query_dq"

_RULES_SUFFIX = "_rules"

# Spark's string-to-boolean cast, used by the reader when it computes the
# execution settings from the rules DataFrame.
_SPARK_TRUE_STRINGS = frozenset({"t", "true", "y", "yes", "1"})


def normalize_policy(value: Any, explicit: bool = False) -> str:
    """Return the canonical policy name for ``value``.

    ``value=None`` selects the default only when the setting is absent
    (``explicit=False``). A setting that is present must name a policy: ``None``,
    empty strings and null-like strings such as ``"null"`` are rejected rather
    than treated as the default. Hyphens are accepted in place of underscores and
    case is ignored, so ``Skip-Invalid`` means ``skip_invalid``.

    Raises:
        SparkExpectationsUserInputOrConfigInvalidException: for any other value.
    """
    if value is None and not explicit:
        return DEFAULT_POLICY
    normalized = value.strip().lower().replace("-", "_") if isinstance(value, str) else None
    if normalized not in SUPPORTED_POLICIES:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Invalid rule validation policy {value!r} for '{POLICY_CONFIG_KEY}': "
            f"expected one of {list(SUPPORTED_POLICIES)}"
        )
    return normalized


def resolve_policy(
    user_conf: Optional[Mapping[str, Any]],
    read_session_conf: Callable[[str], Any],
) -> str:
    """Resolve the policy from the raw, uncast configuration values.

    The generic config builder casts values (``"null"`` becomes ``None``,
    ``"true"`` becomes ``True``), which would hide a misspelled or null setting
    behind the default. This reads the raw value instead, with the same
    precedence as the builder:

    1. ``user_conf`` when it contains the key (even if the value is ``None``);
    2. the Spark session configuration, unless ``user_conf`` enables serverless
       mode (where session configuration is not read);
    3. the default, ``warn_and_run``.

    Args:
        user_conf: The ``user_conf`` passed to ``with_expectations``.
        read_session_conf: Returns the raw session value for a key, or ``None`` if unset.
    """
    if user_conf and POLICY_CONFIG_KEY in user_conf:
        return normalize_policy(user_conf[POLICY_CONFIG_KEY], explicit=True)
    is_serverless = bool(user_conf.get(SERVERLESS_CONFIG_KEY, False)) if user_conf else False
    if is_serverless:
        return DEFAULT_POLICY
    raw = read_session_conf(POLICY_CONFIG_KEY)
    if raw is None:
        return DEFAULT_POLICY
    return normalize_policy(raw, explicit=True)


class RuleIdentity(NamedTuple):
    """Identity of one rule occurrence in a run.

    The components are kept as a tuple, never joined into one delimited string,
    so values that contain separators cannot collide. ``occurrence`` is the
    position of the rule in the execution order; it keeps duplicate rules apart.
    """

    product_id: Optional[str]
    table_name: Optional[str]
    rule_type: Optional[str]
    rule: Optional[str]
    occurrence: int

    def display(self) -> str:
        """Unambiguous text form, used in log lines and exception messages."""
        return json.dumps(
            [self.product_id, self.table_name, self.rule_type, self.rule, self.occurrence],
            ensure_ascii=False,
        )


@dataclass(frozen=True)
class InvalidRule:
    """A rule that failed validation."""

    identity: RuleIdentity
    rule_type: Optional[str]
    errors: Tuple[str, ...]


@dataclass
class PolicyOutcome:
    """Rule state for one run after the policy is applied.

    Attributes:
        policy: The canonical policy name.
        expectations: Expectations to execute. Under ``warn_and_run`` and when
            nothing was skipped this is a copy of the decoration-time value.
        dq_queries: Secondary query map to install for this run.
        execution_settings: Stage flags (``row_dq``, ``source_agg_dq``, ...) for this run.
        invalid_rules: Every invalid rule occurrence, in execution order.
        skipped_rules: Occurrences removed from this run (``skip_invalid`` only).
        validated_rule_count: Number of rule occurrences that were validated.
    """

    policy: str
    expectations: Dict[str, Any]
    dq_queries: Dict[str, Any]
    execution_settings: Dict[str, bool]
    invalid_rules: List[InvalidRule] = field(default_factory=list)
    skipped_rules: List[InvalidRule] = field(default_factory=list)
    validated_rule_count: int = 0

    @property
    def should_fail(self) -> bool:
        return self.policy == FAIL and bool(self.invalid_rules)

    @property
    def stats(self) -> Dict[str, int]:
        """Counts written to the ``invalid_rules`` entry of the stats ``dq_rules`` map."""
        return {
            "num_invalid_rules": len(self.invalid_rules),
            "num_skipped_rules": len(self.skipped_rules),
        }

    def failure_message(self) -> str:
        details = "; ".join(
            f"{item.identity.display()}: {' | '.join(item.errors) or 'invalid'}" for item in self.invalid_rules
        )
        return (
            f"{len(self.invalid_rules)} rule(s) failed validation and the rule validation policy is "
            f"'{FAIL}'; no rule was executed. Invalid rules "
            f"[product_id, table_name, rule_type, rule, occurrence]: {details}"
        )


def _rule_buckets(expectations: Mapping[str, Any]) -> List[str]:
    """Keys of ``expectations`` that hold rule lists, in insertion order.

    This includes buckets for unknown rule types: the reader stores every rule
    under ``f"{rule_type}_rules"``.
    """
    return [
        key for key, value in expectations.items() if key.endswith(_RULES_SUFFIX) and isinstance(value, list)
    ]


def select_execution_rules(expectations: Mapping[str, Any]) -> List[Tuple[str, int, Dict[str, Any]]]:
    """Return ``(bucket, index, rule)`` for every rule that the run would execute."""
    selected: List[Tuple[str, int, Dict[str, Any]]] = []
    for bucket in _rule_buckets(expectations):
        for index, rule in enumerate(expectations[bucket]):
            selected.append((bucket, index, rule))
    return selected


def _identity(rule: Mapping[str, Any], bucket: str, occurrence: int) -> RuleIdentity:
    rule_type = rule.get("rule_type") if isinstance(rule, Mapping) else None
    if rule_type is None:
        rule_type = bucket[: -len(_RULES_SUFFIX)]
    return RuleIdentity(
        product_id=rule.get("product_id") if isinstance(rule, Mapping) else None,
        table_name=rule.get("table_name") if isinstance(rule, Mapping) else None,
        rule_type=rule_type,
        rule=rule.get("rule") if isinstance(rule, Mapping) else None,
        occurrence=occurrence,
    )


def _errors_from_result(result: Any) -> Optional[Tuple[str, ...]]:
    """Return error messages if ``result`` reports an invalid rule, else ``None``.

    ``result`` is the ``Dict[str, List[ValidationResult]]`` returned by
    ``SparkExpectationsValidateRules.validate_expectations`` for a single rule.
    Entries flagged ``is_valid=True`` are ignored; anything else counts as invalid.
    """
    if not result:
        return None
    errors: List[str] = []
    invalid = False
    values: Iterable[Any] = result.values() if isinstance(result, Mapping) else [result]
    for entries in values:
        if entries is None:
            continue
        if not isinstance(entries, (list, tuple)):
            entries = [entries]
        for entry in entries:
            if getattr(entry, "is_valid", False) is True:
                continue
            invalid = True
            message = getattr(entry, "error_message", None)
            if message:
                errors.append(str(message))
    return tuple(errors) if invalid else None


def _spark_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in _SPARK_TRUE_STRINGS


def compute_execution_settings(expectations: Mapping[str, Any]) -> Dict[str, bool]:
    """Recompute the stage flags from the rules that remain in ``expectations``.

    Mirrors ``SparkExpectationsReader._get_rules_execution_settings``.
    """
    rules = [rule for _, _, rule in select_execution_rules(expectations)]

    def _any(rule_type: str, flag: Optional[str] = None) -> bool:
        return any(
            rule.get("rule_type") == rule_type and (flag is None or _spark_bool(rule.get(flag)))
            for rule in rules
        )

    return {
        "row_dq": _any(ROW_DQ),
        "source_agg_dq": _any(AGG_DQ, "enable_for_source_dq_validation"),
        "source_query_dq": _any(QUERY_DQ, "enable_for_source_dq_validation"),
        "target_agg_dq": _any(AGG_DQ, "enable_for_target_dq_validation"),
        "target_query_dq": _any(QUERY_DQ, "enable_for_target_dq_validation"),
    }


def secondary_query_key(rule: Mapping[str, Any]) -> str:
    """The key the reader and actions use for a query rule's secondary queries.

    The key is built from the rule's own fields, never parsed back into parts.
    """
    return f"{rule.get('product_id')}|{rule.get('table_name')}|{rule.get('rule')}"


def _filter_queries(
    dq_queries: Mapping[str, Any],
    kept_rules: Iterable[Mapping[str, Any]],
    skipped_rules: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Drop secondary query entries used only by skipped query rules.

    An entry that a kept rule still maps to is retained, even if a skipped
    rule shares the same key.
    """
    kept_keys = {secondary_query_key(rule) for rule in kept_rules if rule.get("rule_type") == QUERY_DQ}
    skipped_keys = {
        secondary_query_key(rule) for rule in skipped_rules if rule.get("rule_type") == QUERY_DQ
    }
    removable = skipped_keys - kept_keys
    return {key: copy.deepcopy(value) for key, value in dq_queries.items() if key not in removable}


Validator = Callable[[Dict[str, Any]], Any]


def evaluate_policy(
    policy: str,
    expectations: Mapping[str, Any],
    dq_queries: Mapping[str, Any],
    execution_settings: Mapping[str, Any],
    validate_rule: Validator,
) -> PolicyOutcome:
    """Validate the execution rules and build this run's rule state.

    Args:
        policy: Canonical policy name (see :func:`normalize_policy`).
        expectations: Decoration-time expectations. Not modified.
        dq_queries: Decoration-time secondary query map. Not modified.
        execution_settings: Decoration-time stage flags. Not modified.
        validate_rule: Called with a copy of one rule; returns the
            ``validate_expectations`` result map for that rule. An exception
            counts as a validation failure for that rule.

    Returns:
        PolicyOutcome with deep copies of the state to install for this run.
    """
    policy = normalize_policy(policy)
    run_expectations: Dict[str, Any] = copy.deepcopy(dict(expectations))
    selected = select_execution_rules(run_expectations)

    invalid: List[InvalidRule] = []
    invalid_positions = set()
    for occurrence, (bucket, index, rule) in enumerate(selected):
        try:
            errors = _errors_from_result(validate_rule(copy.deepcopy(rule)))
        except Exception as exc:  # pylint: disable=broad-except
            errors = (f"validation raised {type(exc).__name__}: {exc}",)
        if errors is not None:
            identity = _identity(rule, bucket, occurrence)
            invalid.append(InvalidRule(identity=identity, rule_type=identity.rule_type, errors=errors))
            invalid_positions.add((bucket, index))

    outcome = PolicyOutcome(
        policy=policy,
        expectations=run_expectations,
        dq_queries=copy.deepcopy(dict(dq_queries)),
        execution_settings={key: bool(value) for key, value in dict(execution_settings).items()},
        invalid_rules=invalid,
        validated_rule_count=len(selected),
    )

    if policy != SKIP_INVALID or not invalid:
        return outcome

    kept_rules: List[Dict[str, Any]] = []
    skipped_rules: List[Dict[str, Any]] = []
    for bucket in _rule_buckets(run_expectations):
        kept = []
        for index, rule in enumerate(run_expectations[bucket]):
            if (bucket, index) in invalid_positions:
                skipped_rules.append(rule)
            else:
                kept.append(rule)
        kept_rules.extend(kept)
        if kept:
            run_expectations[bucket] = kept
        else:
            del run_expectations[bucket]

    outcome.expectations = run_expectations
    outcome.dq_queries = _filter_queries(dq_queries, kept_rules, skipped_rules)
    outcome.execution_settings = compute_execution_settings(run_expectations)
    outcome.skipped_rules = list(invalid)
    return outcome


def install_rule_counters(context: Any, expectations: Mapping[str, Any]) -> None:
    """Reset and recount the per-type rule counters for ``expectations``.

    Uses the same context setters, with the same arguments, as the reader.
    Rules of unknown types are not counted, as in the reader.
    """
    context.reset_num_row_dq_rules()
    context.reset_num_agg_dq_rules()
    context.reset_num_query_dq_rules()
    context.reset_num_dq_rules()
    for _, _, rule in select_execution_rules(expectations):
        rule_type = rule.get("rule_type")
        if rule_type == context.get_row_dq_rule_type_name:
            context.set_num_row_dq_rules()
        elif rule_type == context.get_agg_dq_rule_type_name:
            context.set_num_agg_dq_rules(
                rule.get("enable_for_source_dq_validation"),
                rule.get("enable_for_target_dq_validation"),
            )
        elif rule_type == context.get_query_dq_rule_type_name:
            context.set_num_query_dq_rules(
                rule.get("enable_for_source_dq_validation"),
                rule.get("enable_for_target_dq_validation"),
            )
