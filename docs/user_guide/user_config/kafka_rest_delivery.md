# Kafka REST Writer: Delivery Semantics and Limits

This page describes how the Kafka REST writer (`SparkExpectationsKafkaRestWritePluginImpl`, transport
`kafka_rest`) sends DQ stats, what it guarantees, and the limits it enforces. For connection settings,
see [Kafka Streaming Config](kafka_custom_config.md).

## How records are sent

- Stats rows are read with `DataFrame.toLocalIterator()`, one partition at a time. The writer does not
  `collect()` the whole stats DataFrame on the driver. Driver memory still has to hold the largest
  partition, so this bounds memory per partition, not overall.
- Each row is sent as its own `POST {base_url}/topics/{topic}` request containing one record.
- Records are sent in order. The writer stops at the first failure; later rows are not sent.

## Delivery guarantee: at-least-once

Failed requests are retried, including `POST`, on connection errors, read errors, and HTTP
408, 429, 500, 502, 503 and 504. A retry can duplicate a record when the proxy wrote it but the
response did not reach the client, for example after a read timeout, a dropped connection, or a 5xx
returned after the write.

- With the default `max_retries` (3), expect occasional duplicates. Consumers should deduplicate,
  for example on run id, product id and table name.
- With `max_retries: 0`, every record is sent once. A failure is reported to the job instead of being
  retried. A record whose response was lost may or may not have been written.
- Re-running a failed job sends every row again, including rows that were acknowledged before the
  failure.

The writer does not provide exactly-once delivery. The Kafka REST v2 produce API has no idempotent
producer or transaction support that the writer could use.

## Partial failures

When a record fails, the job raises `SparkExpectationsMiscException` with:

- the reason: HTTP status, integer `error_code` values from the proxy response, an exception type
  name, or the size-limit violation;
- the 1-based number of the failing record;
- how many earlier records were acknowledged.

Records before the failing one were acknowledged by the proxy. The failing record may or may not have
been written. Later records were not sent.

The message does not include the proxy URL, the response body, proxy-supplied `message` text, the
record payload, or the text of the underlying exception. The original exception is not chained. To
investigate a failure, check the proxy's own logs.

## Limits

These keys are read from `rest_write_options`:

| Key | Default | Meaning |
|---|---|---|
| `max_record_bytes` | `1048576` (1 MiB) | Largest request body allowed. A larger record fails the write before it is sent. |
| `max_retry_after_sec` | `30` | Longest the writer waits for a single `Retry-After` header. Larger values are reduced to this. `0` disables Retry-After sleeping; exponential backoff still applies. |
| `max_retries` | `3` | Retries per record for connection, read and retryable-status failures. `0` disables retries. |
| `backoff_factor` | `0.5` | Exponential backoff between retries when no positive Retry-After delay is applied. |

The longest a single record can wait in `Retry-After` sleeps is about
`max_retries × max_retry_after_sec`, plus request timeouts.

!!! note
    In this version the REST writer is not yet registered with the built-in sink manager, and no
    `user_config` keys map to `max_record_bytes` or `max_retry_after_sec`. The defaults above apply
    unless the caller puts these keys in `rest_write_options`.

A response confirms the single submitted record only when it has a successful HTTP status and exactly one offsets entry with non-negative integer partition and offset values and no error code. Empty, non-JSON or malformed responses now fail with unknown delivery instead of being treated as acknowledgements.
