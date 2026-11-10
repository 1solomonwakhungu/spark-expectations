# Invalid Rule Policy

Each time a decorated function runs, Spark Expectations validates its rules before running them (see [Rule Validation](rule_validation.md)). The `spark.expectations.rules.validation.policy` setting controls what happens when one or more rules are invalid.

| Value | What happens | Default |
|---|---|---|
| `warn_and_run` | Logs each invalid rule and runs every rule, including invalid ones. This matches earlier releases. | Yes |
| `skip_invalid` | Logs each invalid rule and removes it from the current run before any rule executes. Valid rules run normally. | No |
| `fail` | Stops the run before any rule executes and before the input DataFrame is counted. | No |

Values are case-insensitive, and `-` is accepted in place of `_` (`skip-invalid`). Only a missing setting selects the default. Any other value, including an empty string, `None`, or the strings `"null"` and `"None"`, raises `SparkExpectationsUserInputOrConfigInvalidException` when the decorator is applied, before the decorated function runs.

## Setting the policy

Pass it in `user_conf`:

```python
from spark_expectations.config.user_config import Constants as user_config

@se.with_expectations(
    target_table="catalog.schema.orders",
    user_conf={user_config.se_rules_validation_policy: "skip_invalid"},
)
def build_orders():
    ...
```

or as a Spark configuration value:

```python
spark.conf.set("spark.expectations.rules.validation.policy", "fail")
```

The setting is read when the decorator is applied, in this order:

1. `user_conf`, when it contains the key;
2. the Spark session configuration, unless `user_conf` sets `spark.expectations.is.serverless`, in which case session configuration is not read;
3. the default, `warn_and_run`.

The policy is validated as the raw text you set. Unlike some other settings, `"null"` is not converted to an unset value. If the session configuration cannot be read, a warning is logged and the default applies.

## Which rules are validated

The policy validates only the rules that would run for the target table:

- rules whose `product_id` and `table_name` match this run and whose `is_active` is true;
- after `se.dq.rules.params` substitution, so identifiers show formatted names such as `dev_check` rather than `{env}_check`;
- including rules with an unknown `rule_type`, which are reported as invalid instead of being silently ignored.

Rules for other tables or products, and inactive rules, are neither validated nor reported. In earlier releases the run validated every row of the rules DataFrame, so warnings could mention rules that never ran.

Validation runs on every call of the decorated function, against the DataFrame that call returned. A rule that refers to a column missing from one batch is invalid for that batch only. Under `skip_invalid` it runs again on a later call whose DataFrame has the column. Skipping a rule in one run has no effect on later runs.

## `skip_invalid` details

- Skipped rules are not passed to the row, aggregate or query stages, so they never appear as passed or failed.
- If every rule of a stage is skipped, that stage does not run. For example, when every `agg_dq` rule is invalid, source and target aggregate checks are skipped.
- Rule counters in the stats table (`num_row_dq_rules`, `agg_dq_rules`, `query_dq_rules`, `num_dq_rules`) count only the rules that ran.
- Secondary queries of a skipped composite `query_dq` rule are removed for that run.

## `fail` details

The decorated function's DataFrame is created and validated, then the run stops. No rule executes and the input is not counted. As with any other failure:

- a stats row is written with `dq_status.run_status = "Failed"`;
- the caller receives `SparkExpectationsMiscException`, whose message lists every invalid rule and its validation error.

On this row `input_count` is `0` because the input was never counted, not because it was empty. `dq_rules.invalid_rules.input_counted` is `0` in that case (see below).

## Rule identifiers

Invalid rules are identified in logs and exception messages as a JSON array:

```text
["<product_id>", "<table_name>", "<rule_type>", "<rule>", <position>]
```

`position` is the rule's zero-based position among the rules selected for the run. It separates two rules that share a name. Each component is a separate JSON string, so a `|` or other separator inside a value cannot make two rules look the same.

## Statistics

For every run of a decorated function, the `dq_rules` column of the stats table gets an extra `invalid_rules` entry. The column type does not change; `invalid_rules` is a new key in the existing `map<string, map<string, int>>`.

| Key | Meaning |
|---|---|
| `num_invalid_rules` | Number of selected rules that failed validation, counting each occurrence. |
| `num_skipped_rules` | Number of rules removed from this run. Always `0` unless the policy is `skip_invalid`. |
| `input_counted` | `1` when `input_count` was measured, `0` when it was not (a `fail` stop, or a streaming DataFrame). |

Under `warn_and_run`, invalid rules still run and their results are reported as usual; `num_invalid_rules` shows how many of them failed validation.

## Logs

Each invalid rule produces a warning containing its identifier, the policy and the validation error. A summary warning beginning `Some rules failed validation:` follows. When every rule is valid, an info message says so.

## Choosing a policy

- Keep `warn_and_run` if you rely on current behavior and want no change.
- Use `skip_invalid` when partial checking is acceptable and you monitor `num_skipped_rules`. A skipped rule checks nothing, so a run with skipped rules can pass data that the rule would have rejected.
- Use `fail` when every rule must be checked before data is written, for example when the rules table is edited by people other than the pipeline owner.
