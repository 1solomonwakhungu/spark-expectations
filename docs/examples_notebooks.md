# Jupyter Notebook Examples

Spark Expectations ships with ready-to-run Jupyter notebooks in the [`examples/notebooks/`](https://github.com/Nike-Inc/spark-expectations/tree/main/examples/notebooks) directory. These notebooks are the fastest way to explore the framework interactively.

## Running Locally with Docker

The repository provides a Docker Compose setup that launches Jupyter Lab alongside Kafka and Mailpit (a local SMTP test server):

```bash
# Generate a development CA and mail server certificate
make generate-mailserver-certs

# Start all services (Jupyter + Kafka + Mailpit)
make local-se-server-start ARGS="--build"
```

Once running:

- **Jupyter Lab**: [http://127.0.0.1:8888](http://127.0.0.1:8888) (token required, see below)
- **Mailpit UI** (view sent emails): [http://127.0.0.1:8025](http://127.0.0.1:8025)
- **Mailpit SMTP**: `127.0.0.1:1025`
- **Kafka**: `127.0.0.1:9092`

All four ports are published on the host loopback interface (`127.0.0.1`) only, so other machines on your network cannot reach them. Notebooks inside the stack reach Mailpit and Kafka over the Compose network as before.

### Logging in to Jupyter Lab

Jupyter Lab generates a random login token each time the container starts and prints a login URL in the container log. To show it, run:

```bash
docker compose -f containers/compose.yaml logs spark-notebooks | grep "http://127.0.0.1:8888"
```

Open the `http://127.0.0.1:8888/lab?token=...` URL it prints. If the log has rotated, `docker exec se-notebook jupyter server list` prints the current token; it shows the container's hostname in the URL, so replace the host part with `127.0.0.1`. You can also paste the token into the login page at [http://127.0.0.1:8888](http://127.0.0.1:8888).

Treat the token like a password: anyone who has it can run code in the container. Do not commit it, paste it into issues, or share screenshots that show it.

!!! warning
    Do not disable authentication by setting an empty token or password, and do not publish these ports on `0.0.0.0` or a LAN address. Jupyter Lab gives full code execution in the container. If you add volume mounts or credentials to the Compose file, anyone with access to Jupyter can use them too.

## Available Notebooks

| Notebook | Description |
|---|---|
| [`spark_expectation_basic_jlab.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_jlab.ipynb) | Basic DQ setup with Delta Lake. Demonstrates row, aggregate, and query DQ rules with a local SparkSession. |
| [`spark_expectation_basic_mail.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_mail.ipynb) | Email notifications. Sends DQ alerts to the local Mailpit SMTP server. |
| [`spark_expectation_basic_mail_templates.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_mail_templates.ipynb) | Jinja-templated email notifications. Demonstrates custom and default HTML email templates. |
| [`spark_expectations_basic_slack_notification.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectations_basic_slack_notification.ipynb) | Slack notifications via incoming webhooks. |
| [`spark_expectation_basic_pagerduty_notification.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_pagerduty_notification.ipynb) | PagerDuty notifications via Events API v2. |
| [`spark_expectation_notifications_integration.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_notifications_integration.ipynb) | Multi-channel notification integration (email + Slack + more). |
| [`spark_expectations_aggregation_rules.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectations_aggregation_rules.ipynb) | Aggregate and query DQ rules with detailed stats tables. |
| [`spark_expectation_basic_dbx.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_basic_dbx.ipynb) | Databricks-specific setup. Designed for import into Databricks Repos. |
| [`spark_expectation_streaming_dbx.ipynb`](https://github.com/Nike-Inc/spark-expectations/blob/main/examples/notebooks/spark_expectation_streaming_dbx.ipynb) | Streaming DQ with `WrappedDataFrameStreamWriter` on Databricks. |

## Sample Scripts

In addition to notebooks, runnable Python scripts are available in [`examples/scripts/`](https://github.com/Nike-Inc/spark-expectations/tree/main/examples/scripts):

| Script | Description |
|---|---|
| `sample_dq_delta.py` | Delta Lake batch DQ with order/product/customer data |
| `sample_dq_bigquery.py` | BigQuery batch DQ |
| `sample_dq_iceberg.py` | Iceberg batch DQ |
| `sample_dq_delta_streaming.py` | Streaming DQ with `WrappedDataFrameStreamWriter` |
| `sample_dq_yaml_json.py` | Loading rules from YAML/JSON files via `load_rules_from_yaml` |
| `base_setup.py` | Shared SparkSession setup for Delta, Iceberg, and BigQuery |

### Local mail certificate trust

`make generate-mailserver-certs` creates a private development CA and a 30-day server certificate valid for `mailpit`, `localhost` and `127.0.0.1`. The two local mail notebooks explicitly trust `/app/spark_expectations/containers/certs/mailpit-ca.crt` through `spark.expectations.notifications.email.smtp.ca.file`; keep TLS verification enabled. For a host-side client, use the local `containers/certs/mailpit-ca.crt` path and connect to `localhost` or `127.0.0.1`.

The generator refuses to overwrite existing certificates. To renew an expired or older development certificate, stop the local stack, move the three generated certificate/key files out of `containers/certs`, rerun the generator, then rebuild the images so the server and notebook use the same CA. This is only for throwaway local development, not production PKI. Do not commit generated keys or certificates. The CA signing key is discarded after issuance, and private keys are excluded from the notebook and Kafka images.
