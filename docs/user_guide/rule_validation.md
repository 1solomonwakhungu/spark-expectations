# Rule Validation

Spark Expectations validates selected active rules before the DQ pipeline runs. The default `warn_and_run` policy logs validation failures and still executes those rules, so malformed rules can cause runtime failures. The optional `skip_invalid` and `fail` policies change this behavior; see [Rule Validation Policy](rule_validation_policy.md).

## What Gets Validated

Each rule is validated based on its `rule_type`:

| Rule Type | Validation Checks |
|---|---|
| `row_dq` | Expression must be a valid SQL predicate. Aggregate functions (`sum`, `avg`, `count`, etc.) are **not** allowed at the top level (they are allowed inside subqueries). |
| `agg_dq` | Expression must contain at least one aggregate function (`sum`, `avg`, `min`, `max`, `count`, `stddev`, etc.). |
| `query_dq` | Expression must be a valid SQL query. Composite queries with custom delimiters (e.g., `@source_f1@SELECT ...@target_f1@SELECT ...`) are parsed and each sub-query is validated. |

Additionally, for all rule types:

- **`action_if_failed`** must be one of `"drop"`, `"ignore"`, or `"fail"` for `row_dq`, or one of `"ignore"` or `"fail"` for `agg_dq` and `query_dq`.
- **`rule_type`** must be one of `"row_dq"`, `"agg_dq"`, or `"query_dq"`.

## Non-Blocking Validation

Under the default `warn_and_run` policy, when a rule fails validation:

1. A warning is logged with details about the validation failure.
2. The validation step continues without raising an exception.
3. The rule remains in the execution set and is still applied during the DQ pipeline.

Monitor your driver logs for validation warnings and fix invalid rules in your rules table to avoid runtime failures.

!!! tip
    Check your Spark driver logs for messages containing `"Invalid rule detected"` or `"Some rules failed validation"` after a run.

### Strict validation outside the pipeline

The pipeline collects validation results without raising from the helper, then applies the configured
[runtime validation policy](rule_validation_policy.md). To check rules yourself, for example in a CI job, call
`SparkExpectationsValidateRules.validate_expectations(df, rules, spark, raise_exception=True)`.
It raises on the first invalid rule instead of returning the map of invalid results:

| Problem | Exception |
|---|---|
| `rule_type` is missing or is not `row_dq`, `agg_dq`, or `query_dq` | `SparkExpectationsInvalidRuleTypeException` (a subclass of `SparkExpectationsUserInputOrConfigInvalidException`) |
| Invalid `action_if_failed` | `SparkExpectationsUserInputOrConfigInvalidException` |
| Invalid `row_dq`, `agg_dq`, or `query_dq` expectation | `SparkExpectationsInvalidRowDQExpectationException`, `SparkExpectationsInvalidAggDQExpectationException`, or `SparkExpectationsInvalidQueryDQExpectationException` |

## Validation Examples

### Valid `row_dq` Expressions

```sql
age IS NOT NULL
age > 18
salary BETWEEN 30000 AND 200000
lower(trim(status)) IN ('active', 'pending')
col1 > (SELECT avg(col1) FROM ref_table)
```

### Invalid `row_dq` Expressions

```sql
sum(age) > 100          -- Aggregate at top level; use agg_dq instead
AVG(salary) > 50000     -- Aggregate at top level; use agg_dq instead
```

### Valid `agg_dq` Expressions

```sql
count(*) > 0
sum(amount) > 10000
avg(score) BETWEEN 60 AND 100
count(DISTINCT category) <= 10
```

### Invalid `agg_dq` Expressions

```sql
age > 18                -- No aggregate function; use row_dq instead
col1 IS NOT NULL        -- No aggregate function; use row_dq instead
```

## `action_if_failed` Validation

| Rule Type | Valid Values |
|---|---|
| `row_dq` | `drop`, `ignore`, `fail` |
| `agg_dq` | `ignore`, `fail` |
| `query_dq` | `ignore`, `fail` |

Using `drop` with `agg_dq` or `query_dq` will trigger a validation warning. Under the default `warn_and_run` policy, the rule is still executed unless you fix or deactivate it. Use the explicit runtime policy to skip invalid rules or stop before execution.

Values are case-sensitive. A missing or empty `action_if_failed` passes validation; when rules are read for execution, it defaults to `ignore`.

## Dimension Tag Validation

The `tag` column records the data-quality dimension a rule checks. The allowed values are:

`accuracy`, `completeness`, `consistency`, `timeliness`, `uniqueness`, `validity`

The same check applies to rules from a rules table or DataFrame and to rules loaded from YAML or JSON files.

| Tag value | Result |
|---|---|
| One of the six values above | Accepted |
| Missing, `NULL` or empty string | Accepted (no dimension) |
| Anything else, including `Validity`, `" validity"` or a custom label such as `strict` | Reported |

Matching is exact and case-sensitive. Tags are never rewritten: rules keep the value you supplied.

### Default: warning

When `with_expectations` is applied to a function, every active rule selected for the target table is checked once. Each rule with an unrecognized tag produces one warning that names the product, table, rule, rule type and supplied tag, lists the allowed values and, when one is close, suggests it:

```
Rule 'col1_not_null' (product_id='my_product', table_name='db.orders', rule_type='row_dq') has unrecognized dimension tag 'completness'. Allowed values (exact, lowercase): accuracy, completeness, consistency, timeliness, uniqueness, validity. Leave the tag empty if the rule has no dimension. Did you mean 'completeness'?
```

The rule still runs with its tag unchanged, so existing rules with custom tags keep working.

### Strict mode

Set `spark.expectations.rules.dimension.validation.strict` to `true` to reject unrecognized tags. The same key works in both places:

```python
from spark_expectations.config.user_config import Constants as user_config

# Rules table or DataFrame (also applies to rules loaded from files):
# with_expectations raises when it is applied, before the decorated function runs.
user_conf = {user_config.se_rules_dimension_validation_strict: True}

@se.with_expectations(target_table="db.orders", user_conf=user_conf)
def build_orders():
    ...

# YAML/JSON files: load_rules raises while loading.
rules_df = load_rules(
    "rules.yaml",
    options={"dq_env": "DEV", user_config.se_rules_dimension_validation_strict: "true"},
)
```

In strict mode, `SparkExpectationsUserInputOrConfigInvalidException` is raised with one line per offending rule, and no warnings are logged. The loader checks every rule in the file; `with_expectations` checks the active rules for its target table. The setting accepts `true`/`false` (boolean or string). Any other value raises the same exception. The key can also be set in the Spark session configuration.
