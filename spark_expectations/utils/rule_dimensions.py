"""Validation of the data-quality dimension recorded in a rule's ``tag`` field.

Spark Expectations recognises six data-quality dimensions:
``accuracy``, ``completeness``, ``consistency``, ``timeliness``,
``uniqueness`` and ``validity``.

The check is shared by every way of supplying rules (a rules DataFrame or
table, or a YAML/JSON rules file) so the same tag produces the same
diagnostic regardless of where it came from.

Policy:

* A missing, ``None`` or empty-string tag means "no dimension specified"
  and is accepted.
* Matching is exact: the tag must be one of the six lowercase names.
  Different case (``Validity``) or surrounding whitespace (``" validity"``)
  is reported, never silently normalised.
* By default an unrecognised tag is logged as a warning and the rule runs
  with its tag unchanged.  When strict mode is enabled
  (``spark.expectations.rules.dimension.validation.strict``) an
  unrecognised tag raises
  :class:`~spark_expectations.core.exceptions.SparkExpectationsUserInputOrConfigInvalidException`.
"""

import difflib
import logging
from dataclasses import dataclass
from typing import Any, Iterable, List, Mapping, Optional, Tuple

from spark_expectations.config.user_config import Constants as user_config
from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException

logger = logging.getLogger(__name__)

RULE_DIMENSIONS: Tuple[str, ...] = (
    "accuracy",
    "completeness",
    "consistency",
    "timeliness",
    "uniqueness",
    "validity",
)
"""The data-quality dimensions accepted in a rule's ``tag`` field."""

STRICT_DIMENSION_VALIDATION_KEY = user_config.se_rules_dimension_validation_strict
"""Configuration key that turns unrecognised dimension tags into errors."""

_ALLOWED = frozenset(RULE_DIMENSIONS)
_MAX_DIAGNOSTIC_LENGTH = 120


def _diagnostic_repr(value: Any) -> str:
    """Bound and escape diagnostic values without rejecting accepted rule data."""
    try:
        text = repr(value)
    except Exception:
        return "<unrepresentable value>"
    text = text.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")
    return text if len(text) <= _MAX_DIAGNOSTIC_LENGTH else text[: _MAX_DIAGNOSTIC_LENGTH - 3] + "..."


@dataclass(frozen=True)
class DimensionTagIssue:
    """A rule whose ``tag`` is not one of :data:`RULE_DIMENSIONS`.

    Attributes:
        product_id: Product the rule belongs to, if known.
        table_name: Table the rule applies to, if known.
        rule: Rule name, if known.
        rule_type: Rule type, if known.
        tag: The tag exactly as supplied.
        suggestion: The closest allowed dimension, if one is close enough.
    """

    product_id: Optional[str]
    table_name: Optional[str]
    rule: Optional[str]
    rule_type: Optional[str]
    tag: Any
    suggestion: Optional[str] = None

    @property
    def message(self) -> str:
        """Human-readable description that identifies the rule and the tag."""
        text = (
            f"Rule {_diagnostic_repr(self.rule)} (product_id={_diagnostic_repr(self.product_id)}, "
            f"table_name={_diagnostic_repr(self.table_name)}, rule_type={_diagnostic_repr(self.rule_type)}) "
            f"has unrecognized dimension tag {_diagnostic_repr(self.tag)}. "
            f"Allowed values (exact, lowercase): {', '.join(RULE_DIMENSIONS)}. "
            "Leave the tag empty if the rule has no dimension."
        )
        if self.suggestion:
            text += f" Did you mean '{self.suggestion}'?"
        return text


def _suggest(tag: Any) -> Optional[str]:
    if not isinstance(tag, str):
        return None
    candidate = tag.strip().lower()
    if len(candidate) > 2 * max(map(len, RULE_DIMENSIONS)):
        return None
    if candidate in _ALLOWED:
        return candidate
    matches = difflib.get_close_matches(candidate, RULE_DIMENSIONS, n=1, cutoff=0.8)
    return matches[0] if matches else None


def _as_text(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def find_invalid_dimension_tags(rules: Iterable[Mapping[str, Any]]) -> List[DimensionTagIssue]:
    """Return one issue per rule whose tag is set but is not an allowed dimension.

    The rules are only inspected; neither the rules nor their tags are modified.

    Args:
        rules: Rule mappings (dicts or ``Row.asDict()`` results) with the usual
            rules-table keys. Only ``tag`` is checked; ``product_id``,
            ``table_name``, ``rule`` and ``rule_type`` identify the rule in
            diagnostics.

    Returns:
        Issues in input order. Empty when every tag is allowed or unset.
    """
    issues: List[DimensionTagIssue] = []
    for rule in rules:
        tag = rule.get("tag")
        if tag is None or (isinstance(tag, str) and (tag == "" or tag in _ALLOWED)):
            continue
        issues.append(
            DimensionTagIssue(
                product_id=_as_text(rule.get("product_id")),
                table_name=_as_text(rule.get("table_name")),
                rule=_as_text(rule.get("rule")),
                rule_type=_as_text(rule.get("rule_type")),
                tag=tag,
                suggestion=_suggest(tag),
            )
        )
    return issues


def validate_rule_dimensions(
    rules: Iterable[Mapping[str, Any]],
    strict: bool = False,
) -> List[DimensionTagIssue]:
    """Check rule tags against :data:`RULE_DIMENSIONS`.

    Args:
        rules: Rule mappings to check. They are not modified.
        strict: When ``True``, raise if any rule has an unrecognised tag;
            otherwise log one warning per offending rule.

    Returns:
        The issues found (empty when all tags are allowed or unset).

    Raises:
        SparkExpectationsUserInputOrConfigInvalidException: When *strict* is
            ``True`` and at least one rule has an unrecognised tag. The message
            lists every offending rule.
    """
    issues = find_invalid_dimension_tags(rules)
    if not issues:
        return issues
    if strict:
        details = "\n".join(f"  - {issue.message}" for issue in issues)
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"{len(issues)} rule(s) have an unrecognized dimension tag and "
            f"'{STRICT_DIMENSION_VALIDATION_KEY}' is enabled:\n{details}"
        )
    for issue in issues:
        logger.warning(issue.message)
    return issues


def is_strict_dimension_validation(value: Any) -> bool:
    """Interpret the value of :data:`STRICT_DIMENSION_VALIDATION_KEY`.

    Accepts booleans, the strings ``"true"``/``"false"`` (any case, surrounding
    whitespace ignored) and ``None`` (treated as ``False``).

    Raises:
        SparkExpectationsUserInputOrConfigInvalidException: For any other value,
            so that a mistyped setting does not silently disable strict mode.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise SparkExpectationsUserInputOrConfigInvalidException(
        f"'{STRICT_DIMENSION_VALIDATION_KEY}' must be true or false, got {_diagnostic_repr(value)}."
    )
