# Security Policy

## Reporting a vulnerability

Do not put vulnerability details in a public issue, pull request, discussion, or commit message. This matters most for reports that involve credentials, secret values, or log output.

1. If this repository's **Security** tab shows a **Report a vulnerability** button, use it. GitHub then keeps the report private between you and the maintainers.
2. If that button is not shown, open a public issue that asks the maintainers for a private way to send a security report. Include only the affected component and the words "security report". Leave out the impact, reproduction steps, payloads, and any secret material until a private channel exists.

The contact address in [CONTRIBUTING.md](CONTRIBUTING.md) is for general questions. This policy does not designate it as a security reporting channel.

A useful report includes:

- the Spark Expectations version or commit, the Python version, and the Spark/PySpark version;
- the affected module or configuration key;
- the smallest reproduction you can give, using synthetic data and synthetic credentials;
- what an attacker needs to control or reach, and what they gain.

Never include real tokens, passwords, client secrets, or production data. If you think a real credential has already been exposed, rotate it first. Rotation does not depend on a fix.

## Response expectations

The project has not published a response-time or disclosure-timeline commitment. This policy makes none. Reports are handled by the maintainers as their capacity allows.

## Supported versions

The project has not published a security support window or a backport policy. This policy does not commit maintainers to a fix, release date, or backport.

| Component | Range declared at time of writing | Source |
|---|---|---|
| Python | `>=3.9,<=3.13` | `requires-python` in `pyproject.toml` |
| Python versions run in CI | 3.10, 3.11, 3.12 | `.github/workflows/onpush.yml` |
| PySpark (optional extra) | `pyspark[connect]>=3.0.0,<=4.0.0` | `pyspark` extra in `pyproject.toml` |
| `requests` | `>=2.28.1` | `dependencies` in `pyproject.toml` |
| `PyYAML` | `>=6.0` | `dependencies` in `pyproject.toml` |
| `sqlglot` | `>=21.0,<23.0` | `dependencies` in `pyproject.toml` |

A declared range is an installation constraint, not a security guarantee. Some versions inside these ranges have published advisories. See [Dependency advisories and version ranges](docs/security.md#dependency-advisories-and-version-ranges).

## Scope and trust model

Spark Expectations is an in-process Python library. It runs with the authority of the Spark session, filesystem, network, secret backends, and notification endpoints that the calling job already has. It has no user authentication layer of its own.

The following are trusted, executable input. Whoever can change them can run code or SQL with the job's authority:

- data quality rules, including row, aggregate, and query rules;
- installed Python packages that register Spark Expectations entry-point plugins;
- job configuration, including rule parameters, table names, URLs, and custom email templates.

A report that depends only on control of one of these inputs is usually a deployment-boundary problem, not a library vulnerability. Reports are still welcome when the library crosses a boundary it should keep. Examples: secret values reaching logs, unescaped report data in HTML email, or a TLS connection that does not verify its peer.

[docs/security.md](docs/security.md) describes the trust boundaries, path handling, plugin loading, and the rule-validation policy in more detail.
