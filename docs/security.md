# Security Model

This page describes what Spark Expectations trusts, where its boundaries are, and what a deployment has to enforce for itself. Reporting instructions are included below so they remain available in published documentation.

Spark Expectations is a library that runs inside your Spark job. It has no login, no permission model, and no sandbox. Anything it does, it does with the job's own Spark session, filesystem access, network access, secret backends, and notification credentials.

## Reporting a vulnerability

Do not publish vulnerability details, credentials, production data, or reproduction payloads. If this repository's Security tab offers **Report a vulnerability**, use that private route. Otherwise, open an issue asking only for a private security contact, with the affected component and no technical details. The general contact in CONTRIBUTING.md is not a designated security channel. No response timeline, fix, release date, or backport is promised.

## Trust boundaries

| Input | Who usually controls it | What the library does with it | Treat it as |
|---|---|---|---|
| Rules (`row_dq`, `agg_dq`, `query_dq`) | Rule authors | Row and aggregate expressions are compiled with Spark `expr`/`selectExpr`. Query rules run through `spark.sql`, and their results can be collected to the driver. | Executable SQL with the job's data access |
| Rule parameters (`dq_rules_params`) | Job author | Expanded into rule fields and query SQL with Python `str.format`, and into a Spark SQL `REPLACE(...)` expression on `table_name` | Code-adjacent configuration |
| Job configuration (table names, URLs, recipients, TLS options, secret paths) | Job author | Passed to Spark SQL, HTTP clients, SMTP, Kafka, and secret lookups | Trusted configuration |
| Custom email templates | Job author | Rendered with Jinja2 | Template code |
| Rule files passed to `load_rules` | Caller | Opened at the given path and parsed | Trusted file path and trusted content |
| Installed entry-point plugins | Whoever installs packages into the environment | Imported and called in the driver process | Arbitrary Python code |
| Source and target data | Upstream producers | Evaluated by rules; some values are copied into stats tables, error tables, logs, and notifications | Untrusted data |

The library cannot make rule SQL safe through string checks. If people who should not have the job's full data access can write rules, the boundary has to live outside the library. See [Accepting rules from less-trusted authors](#accepting-rules-from-less-trusted-authors).

## Rules are executable

A rule is a piece of Spark SQL. Anyone who can add or edit a rule can:

- read any table the Spark session can read, using a subquery in a row rule or a `query_dq` query;
- start expensive scans, joins, or aggregations on the cluster;
- drop rows from the target table or stop the job, through `action_if_failed` set to `drop` or `fail`.

Rule validation (see [Rule Validation](user_guide/rule_validation.md)) checks shape, not intent. Row and aggregate checks ask Spark to build a plan for the expression. Query checks parse the SQL with `sqlglot` after substituting placeholder table names. Neither check limits which tables a rule touches or how much work it does. Validation is not a security control.

### Placeholder expansion

When you pass `dq_rules_params`, the reader expands `{name}` placeholders with Python `str.format` in `product_id`, `table_name`, and `rule`, and in the SQL of `query_dq` rules. It also builds a Spark SQL `REPLACE(table_name, '{key}', 'value')` expression from each parameter, with the key and value inserted directly into the SQL string. Parameter keys and values are therefore part of the job's code. Do not fill them from end-user input.

## File paths

`spark_expectations.rules.load_rules(path, ...)` opens the path it is given. It does not resolve the path against a base directory, reject `..` segments or symbolic links, or cap file size or rule count. For a library called by the job author, this is expected: the caller already has that filesystem access.

YAML rule files are parsed with `yaml.safe_load`, and JSON rule files with the standard library decoder. Neither loader constructs arbitrary Python objects.

If a service built on Spark Expectations takes a rule file name or path from a user, the service must enforce the boundary before it calls `load_rules`:

1. Canonicalize both the allowlisted root and requested path with `os.path.realpath`, then confirm `os.path.commonpath([root, resolved]) == root`. A string prefix check is not enough. This check is safe only when untrusted users cannot replace files or path components between checking and opening them.
2. For attacker-writable trees, use platform-appropriate descriptor-based traversal and open protections, or accept bounded uploaded content and stage it in job-owned storage. The current loader accepts paths and opens them later; it cannot consume a previously checked descriptor.
3. Bound the content actually read, not only an earlier file-size measurement, and check the parsed rule count before passing rules to Spark. Keep staged files and their parent directories protected throughout the load.

## Plugins are trusted code

Spark Expectations uses [pluggy](https://pluggy.readthedocs.io/) and loads installed setuptools entry points for two groups:

| Entry-point group | When it is loaded | What a plugin receives |
|---|---|---|
| `spark_expectations_rule_loader_plugins` | When `spark_expectations.rules` is first imported | The rule path, format, options, and Spark session |
| `spark_expectations_secrets_backend` | On the first secret lookup | The secret key path and the secret configuration dictionary |

Loading an entry point imports its module. That module's code runs in the driver with the full authority of the job, before any hook is called. A secrets plugin can see every secret lookup. A rule-loader plugin can return any rules it likes.

The writer and notification plugin managers register only the built-in implementations. They do not load third-party entry points.

To keep this boundary:

- install packages only from sources you trust, and pin them with hashes in production environments;
- list the installed plugins before a release with `importlib.metadata.entry_points(group="spark_expectations_rule_loader_plugins")` and `importlib.metadata.entry_points(group="spark_expectations_secrets_backend")`;
- do not let job users install packages into a shared driver environment.

## Templates, notifications, and logs

Custom email templates are Jinja2 source. Whoever supplies a template controls what the email contains. Keep template text with job configuration, not with user-editable rules.

Rule names, descriptions, column names, and collected data values can appear in notifications and in driver logs. This page does not claim that any of these fields is escaped or redacted on every path. Plan for that:

- If rule authors and alert recipients are different groups, review notification content as you would any other report built from user-supplied text.
- Restrict access to driver logs. Treat logs captured at `DEBUG` as sensitive.
- Send credentials only to endpoints that verify TLS, and check the transport settings for each notification and sink you enable.

## Accepting rules from less-trusted authors

If people other than the job owner can write rules, put these controls around the library:

- **Least-privilege Spark identity.** Run the job as a principal that can read only the tables those rule authors are allowed to read, and write only the target, error, and stats tables.
- **Admission limits.** Cap file size, rule count, and expression length before rules reach Spark. Consider review or approval for `query_dq` rules, which can run any query.
- **Resource limits.** Use cluster or pool quotas and job timeouts. The library does not bound the cost of a rule.
- **Separate tenants.** Do not mix rules from different trust domains in one job, one rules table, or one Spark session.
- **Fail on invalid rules in your own pipeline.** Rule validation logs warnings by default and the job continues. If invalid rules must stop a run, call `SparkExpectationsValidateRules.validate_expectations(df, rules, spark)` from `spark_expectations.utils.validate_rules` on the rules for that job before you run it, and stop if the returned dictionary is not empty. This default-mode result check also works without relying on version-specific strict-mode exception behavior.

## Dependency advisories and version ranges

The declared dependency ranges in `pyproject.toml` are wide. Some versions they allow have published advisories. A version match means the advisory may apply to an environment; it does not show that Spark Expectations reaches the affected code.

| Dependency | Declared range | Notes from a review on 2026-11-09 |
|---|---|---|
| `requests` | `>=2.28.1` | Four advisories match 2.28.1: [GHSA-gc5v-m9x4-r6x2](https://github.com/advisories/GHSA-gc5v-m9x4-r6x2), [GHSA-9hjg-9r4m-mvj7](https://github.com/advisories/GHSA-9hjg-9r4m-mvj7), [GHSA-9wx4-h78v-vm56](https://github.com/advisories/GHSA-9wx4-h78v-vm56), and [GHSA-j8r2-6x86-q33q](https://github.com/advisories/GHSA-j8r2-6x86-q33q). Source review did not establish a reachable path; proxy and redirect behavior was not exercised. This is not proof of non-reachability. Install a current release and assess your transport configuration. |
| `PyYAML` | `>=6.0` | No reviewed advisory matched 6.0. Rule and configuration loading use `yaml.safe_load`. |
| `sqlglot` | `>=21.0,<23.0` | No reviewed advisory matched 21.0. |
| `pyspark` (optional extra) | `>=3.0.0,<=4.0.0` | Six Apache Spark advisories match 3.0.0, including [GHSA-9437-39hj-3c93](https://github.com/advisories/GHSA-9437-39hj-3c93) and [GHSA-4x9r-j582-cgr8](https://github.com/advisories/GHSA-4x9r-j582-cgr8). They concern Spark services and cluster settings (History Server, UI, proxy users, network encryption), which this library does not implement or configure. Your cluster's Spark version is your choice; keep it patched. |

Advisory databases change. Check your own resolved environment, for example with `pip-audit`, rather than relying on this table. The project has not published a tested security-support matrix or minimum patched versions.

## Local development stack

The Compose stack under `containers/` is for local development on a single workstation. Do not expose its ports to other hosts or networks, and do not mount production credentials or data into it. Check the published ports and the Jupyter authentication settings in the Compose and Jupyter configuration files before you start it.
