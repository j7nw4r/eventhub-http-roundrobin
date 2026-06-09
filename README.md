# eventhub-http-roundrobin

A tiny Rust producer that sends events to an Azure Event Hub over the **HTTP REST
API** with **no partition key**, so you can confirm that Event Hubs round-robins
events across partitions when you let the broker pick.

## Why no partition key

Event Hubs decides where an event lands based on what you send:

| What you send | Where it lands |
| --- | --- |
| No partition key, `POST .../{hub}/messages` | Broker distributes events **round-robin** across all partitions |
| `BrokerProperties: {"PartitionKey":"k"}` header | All events with key `k` hash to the **same** partition |
| `POST .../{hub}/partitions/{id}/messages` | Pinned to that exact partition |

This program does the first one. It sets no `BrokerProperties` header and posts to
`/messages`, which is the entire point: with the partition key omitted, distribution
is the broker's job, and it spreads load evenly.

## Auth

There is no Azure SDK dependency. The program builds a Shared Access Signature
(SAS) token by hand:

```
string_to_sign = urlencode(resource_uri) + "\n" + expiry_epoch_seconds
signature      = urlencode(base64(HMAC_SHA256(sas_key, string_to_sign)))
token          = "SharedAccessSignature sr={enc_uri}&sig={signature}&se={expiry}&skn={key_name}"
```

This matches Microsoft's reference algorithm
([Generate SAS token](https://learn.microsoft.com/en-us/rest/api/eventhub/generate-sas-token)),
and a golden unit test pins the output byte-for-byte against it.

## Configure

Easiest: copy a **Shared access policy** connection string from the Azure portal
(Event Hub > Shared access policies, or the namespace-level policy). A `Send` claim
is enough.

```sh
cp .env.example .env   # then fill it in, OR just export the vars
export EVENTHUB_CONNECTION_STRING='Endpoint=sb://NS.servicebus.windows.net/;SharedAccessKeyName=Send;SharedAccessKey=...;EntityPath=myhub'
```

If the connection string has no `EntityPath`, also set `EVENTHUB_NAME` (or pass
`--event-hub`).

Or configure the pieces discretely instead of a connection string:

```sh
export EVENTHUB_FQDN='mynamespace.servicebus.windows.net'
export EVENTHUB_NAME='myhub'
export EVENTHUB_SAS_KEY_NAME='Send'
export EVENTHUB_SAS_KEY='...'
```

## Run

```sh
cargo run --release -- --count 30 --interval-ms 250
```

```
Producing 30 event(s) to https://mynamespace.servicebus.windows.net/myhub (no partition key -> broker round-robins)

seq    0 -> 201 Created
seq    1 -> 201 Created
...
Done. 30 accepted, 0 failed.
```

Each event body carries a `seq` number, the breadcrumb you use to watch the
round-robin on the consumer side.

## Verify the round-robin

The REST send endpoint returns `201 Created` with an **empty body**; it does not
tell the producer which partition received the event. That is expected: round-robin
is observed on the **consumer** side. Pick one:

- **Azure portal** > your Event Hub > **Data Explorer**: receive across all
  partitions and watch the `seq` values land on different partition IDs.
- **az CLI** monitor metrics, "Incoming Messages" split by partition, should be
  roughly even after a run.
- **Any consumer** (e.g. the `azure_messaging_eventhubs` consumer client or a Kafka
  consumer on the Event Hubs Kafka endpoint): print `partition_id` alongside the
  event's `seq`. Consecutive `seq` values should cycle through the partitions.

If you instead want to see events pile onto one partition, add a partition key (see
the `AIDEV-NOTE` in `src/main.rs`) and re-run, the distribution collapses to a
single partition.

## Test

```sh
cargo test     # connection-string parsing, URL shaping, SAS golden vector
cargo clippy
```

## License

MIT
