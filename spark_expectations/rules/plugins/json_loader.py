"""Pluggy plugin that loads DQ rules from a JSON file."""

import json
import os
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from spark_expectations.core.exceptions import SparkExpectationsUserInputOrConfigInvalidException
from spark_expectations.rules.plugins._flatten import (
    flatten_rules_list,
    rows_to_dataframe,
)
from spark_expectations.rules.plugins.base_rule_loader import spark_expectations_rule_loader_impl
from spark_expectations.utils.rule_dimensions import (
    STRICT_DIMENSION_VALIDATION_KEY,
    is_strict_dimension_validation,
    validate_rule_dimensions,
)

JSON_EXTENSIONS = {".json"}


def _read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError as exc:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rules JSON file not found: {path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Error parsing JSON rules file '{path}': {exc}"
        ) from exc

    if data is None:
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rules JSON file is empty or null: {path}"
        )
    if not isinstance(data, dict):
        raise SparkExpectationsUserInputOrConfigInvalidException(
            f"Rules file must contain an object at the top level, got {type(data).__name__}: {path}"
        )
    return data


class SparkExpectationsJsonRuleLoaderImpl:
    """Loads DQ rules from a JSON file."""

    @spark_expectations_rule_loader_impl
    def load_rules(  # pylint: disable=unused-argument
        self,
        path: str,
        format: str,
        options: Dict[str, str],
        spark: Optional[SparkSession] = None,
    ) -> Optional[DataFrame]:
        if format == "json":
            pass
        elif format == "auto" and os.path.splitext(path)[1].lower() in JSON_EXTENSIONS:
            pass
        else:
            return None

        if spark is None:
            spark = SparkSession.getActiveSession()
        if spark is None:
            raise SparkExpectationsUserInputOrConfigInvalidException(
                "No active SparkSession found. Please create a SparkSession before loading rules."
            )

        data = _read_json(path)
        env = (options or {}).get("dq_env")
        rows = flatten_rules_list(data, env=env)
        if is_strict_dimension_validation((options or {}).get(STRICT_DIMENSION_VALIDATION_KEY)):
            validate_rule_dimensions(rows, strict=True)
        return rows_to_dataframe(rows, spark)
