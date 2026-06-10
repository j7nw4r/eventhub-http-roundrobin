//! Produce events to an Azure Event Hub over the HTTP REST API, deliberately
//! WITHOUT a partition key, so the broker round-robins events across partitions.
//!
//! The crux of the demo lives in [`send_one`]: we POST to the hub's `/messages`
//! endpoint and set NO `BrokerProperties` partition key. Azure Event Hubs then
//! distributes events across partitions itself. Pin events to a partition by
//! adding a `BrokerProperties: {"PartitionKey":"..."}` header (we do not), or by
//! posting to `/partitions/{id}/messages` (we also do not).
//!
//! Auth is a hand-rolled Shared Access Signature (SAS) token; see [`sas_token`].
//!
//! REST + SAS references (verified against Microsoft Learn):
//!   - Send event:   https://learn.microsoft.com/en-us/rest/api/eventhub/send-event
//!   - SAS token:    https://learn.microsoft.com/en-us/rest/api/eventhub/generate-sas-token

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use clap::Parser;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Produce events to an Azure Event Hub via the HTTP REST API with no partition
/// key, demonstrating broker-side round-robin partition distribution.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Full Event Hubs connection string. The simplest way to configure this:
    /// copy a "Shared access policy" connection string from the Azure portal.
    /// Example: Endpoint=sb://NS.servicebus.windows.net/;SharedAccessKeyName=Send;SharedAccessKey=...;EntityPath=myhub
    #[arg(long, env = "EVENTHUB_CONNECTION_STRING")]
    connection_string: Option<String>,

    /// Namespace host, e.g. "mynamespace.servicebus.windows.net".
    /// Ignored when --connection-string is provided.
    #[arg(long, env = "EVENTHUB_FQDN")]
    fqdn: Option<String>,

    /// SAS policy (key) name, e.g. "RootManageSharedAccessKey" or a Send-only policy.
    /// Ignored when --connection-string is provided.
    #[arg(long, env = "EVENTHUB_SAS_KEY_NAME")]
    sas_key_name: Option<String>,

    /// SAS key value. Ignored when --connection-string is provided.
    #[arg(long, env = "EVENTHUB_SAS_KEY")]
    sas_key: Option<String>,

    /// Event hub (entity) name. Required unless the connection string carries an EntityPath.
    #[arg(long, env = "EVENTHUB_NAME")]
    event_hub: Option<String>,

    /// Number of events to send. 0 (the default) sends continuously until Ctrl-C.
    #[arg(long, default_value_t = 0)]
    count: u32,

    /// Delay between sends, in milliseconds.
    #[arg(long, default_value_t = 250)]
    interval_ms: u64,
}

/// Everything we need to address and authenticate against one Event Hub.
#[derive(Debug)]
struct EventHubConfig {
    /// e.g. "mynamespace.servicebus.windows.net"
    fqdn: String,
    /// the entity path / hub name
    event_hub: String,
    sas_key_name: String,
    sas_key: String,
}

impl EventHubConfig {
    /// Resolve config from either a connection string or the discrete flags/env vars.
    fn resolve(args: &Args) -> Result<Self> {
        if let Some(cs) = &args.connection_string {
            let mut cfg = parse_connection_string(cs)?;
            // An explicit --event-hub flag overrides any EntityPath in the string.
            if let Some(eh) = &args.event_hub {
                cfg.event_hub = eh.clone();
            }
            if cfg.event_hub.is_empty() {
                return Err(anyhow!(
                    "no event hub: the connection string has no EntityPath; pass --event-hub / EVENTHUB_NAME"
                ));
            }
            return Ok(cfg);
        }

        Ok(EventHubConfig {
            fqdn: args
                .fqdn
                .clone()
                .context("missing --fqdn / EVENTHUB_FQDN (or pass --connection-string)")?,
            event_hub: args
                .event_hub
                .clone()
                .context("missing --event-hub / EVENTHUB_NAME")?,
            sas_key_name: args
                .sas_key_name
                .clone()
                .context("missing --sas-key-name / EVENTHUB_SAS_KEY_NAME")?,
            sas_key: args
                .sas_key
                .clone()
                .context("missing --sas-key / EVENTHUB_SAS_KEY")?,
        })
    }

    /// The resource URI the SAS token is scoped to and that we POST against.
    fn resource_uri(&self) -> String {
        format!("https://{}/{}", self.fqdn, self.event_hub)
    }

    /// The send endpoint. Note: `/messages` (not `/partitions/{id}/messages`) and
    /// no partition key header => the broker round-robins across partitions.
    fn messages_url(&self) -> String {
        format!(
            "{}/messages?timeout=60&api-version=2014-01",
            self.resource_uri()
        )
    }
}

/// Parse an Event Hubs/Service Bus connection string into [`EventHubConfig`].
///
/// Shape: `Endpoint=sb://NS.servicebus.windows.net/;SharedAccessKeyName=K;SharedAccessKey=V;EntityPath=hub`
/// EntityPath is optional (it may instead be supplied via --event-hub).
fn parse_connection_string(cs: &str) -> Result<EventHubConfig> {
    let mut endpoint = None;
    let mut key_name = None;
    let mut key = None;
    let mut entity = None;

    for part in cs.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // Split on the FIRST '=' only: SharedAccessKey values can contain '=' (base64 padding).
        let (k, v) = part
            .split_once('=')
            .with_context(|| format!("malformed connection string segment: {part:?}"))?;
        match k.trim().to_ascii_lowercase().as_str() {
            "endpoint" => endpoint = Some(v.trim().to_string()),
            "sharedaccesskeyname" => key_name = Some(v.trim().to_string()),
            "sharedaccesskey" => key = Some(v.trim().to_string()),
            "entitypath" => entity = Some(v.trim().to_string()),
            _ => {} // ignore unknown keys (e.g. TransportType)
        }
    }

    let endpoint = endpoint.context("connection string missing Endpoint=")?;
    // Endpoint looks like "sb://NS.servicebus.windows.net/". Strip scheme + trailing slash.
    let fqdn = endpoint
        .trim()
        .trim_start_matches("sb://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_string();

    Ok(EventHubConfig {
        fqdn,
        event_hub: entity.unwrap_or_default(),
        sas_key_name: key_name.context("connection string missing SharedAccessKeyName=")?,
        sas_key: key.context("connection string missing SharedAccessKey=")?,
    })
}

/// Build a Shared Access Signature token for the given resource URI.
///
/// Per Microsoft's reference:
///   string_to_sign = urlencode(resource_uri) + "\n" + expiry_epoch_seconds
///   signature      = urlencode(base64(HMAC_SHA256(key, string_to_sign)))
///   token          = "SharedAccessSignature sr={enc_uri}&sig={signature}&se={expiry}&skn={key_name}"
fn sas_token(resource_uri: &str, key_name: &str, key: &str, ttl: Duration) -> Result<String> {
    let expiry = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs()
        + ttl.as_secs();
    sas_token_with_expiry(resource_uri, key_name, key, expiry)
}

/// Signing core with an explicit expiry (unix seconds), so the signature is
/// deterministic and testable without mocking the clock.
fn sas_token_with_expiry(
    resource_uri: &str,
    key_name: &str,
    key: &str,
    expiry: u64,
) -> Result<String> {
    let encoded_uri = urlencoding::encode(resource_uri);
    let string_to_sign = format!("{encoded_uri}\n{expiry}");

    let mut mac =
        HmacSha256::new_from_slice(key.as_bytes()).map_err(|e| anyhow!("invalid SAS key: {e}"))?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let encoded_sig = urlencoding::encode(&signature);

    Ok(format!(
        "SharedAccessSignature sr={encoded_uri}&sig={encoded_sig}&se={expiry}&skn={key_name}"
    ))
}

/// POST a single event body to the hub with NO partition key.
///
/// AIDEV-NOTE: This is the whole demo. We do not set a `BrokerProperties` header
/// and we hit `/messages` (not `/partitions/{id}/messages`). That omission is
/// what makes Event Hubs distribute events round-robin across partitions.
async fn send_one(
    client: &reqwest::Client,
    url: &str,
    auth: &str,
    body: String,
) -> Result<reqwest::StatusCode> {
    let resp = client
        .post(url)
        .header(reqwest::header::AUTHORIZATION, auth)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .context("HTTP request to Event Hubs failed")?;
    Ok(resp.status())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = EventHubConfig::resolve(&args)?;

    // One token reused for the whole run. Generously valid for an hour.
    let auth = sas_token(
        &cfg.resource_uri(),
        &cfg.sas_key_name,
        &cfg.sas_key,
        Duration::from_secs(3600),
    )?;
    let url = cfg.messages_url();
    let client = reqwest::Client::new();

    let unbounded = args.count == 0;
    if unbounded {
        println!(
            "Producing continuously to {} (no partition key -> broker round-robins). Ctrl-C to stop.\n",
            cfg.resource_uri()
        );
    } else {
        println!(
            "Producing {} event(s) to {} (no partition key -> broker round-robins)\n",
            args.count,
            cfg.resource_uri()
        );
    }

    // Graceful shutdown. A Ctrl-C flips `stop`; the loop checks it each iteration
    // (so it works even at --interval-ms 0) and `notify` wakes any in-flight sleep
    // so shutdown is snappy rather than waiting out the interval.
    let stop = Arc::new(AtomicBool::new(false));
    let notify = Arc::new(tokio::sync::Notify::new());
    {
        let (stop, notify) = (stop.clone(), notify.clone());
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                stop.store(true, Ordering::SeqCst);
                notify.notify_waiters();
            }
        });
    }

    let mut ok = 0u64;
    let mut failed = 0u64;
    let mut total_bytes: u64 = 0; // cumulative accepted payload bytes
    let mut seq: u64 = 0;

    while !stop.load(Ordering::SeqCst) {
        if !unbounded && seq >= args.count as u64 {
            break;
        }

        let sent_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        // The seq number is the breadcrumb: a consumer can map seq -> partition
        // and watch the assignments cycle through every partition.
        let body = format!(
            r#"{{"seq":{seq},"sentAtMs":{sent_at_ms},"note":"no partition key; expect round-robin"}}"#
        );
        // Payload bytes only (the JSON body), not HTTP headers or the SAS token.
        let body_len = body.len() as u64;

        match send_one(&client, &url, &auth, body).await {
            // The REST send endpoint returns 201 Created with an empty body and
            // does NOT tell us which partition received the event. Verify the
            // round-robin on the consumer side (see README).
            Ok(status) if status.is_success() => {
                ok += 1;
                total_bytes += body_len;
                println!("seq {seq:>6} -> {status} | +{body_len} B | total {total_bytes} B / {ok} events");
            }
            Ok(status) => {
                failed += 1;
                eprintln!("seq {seq:>6} -> {status} (NOT accepted)");
            }
            Err(e) => {
                failed += 1;
                eprintln!("seq {seq:>6} -> error: {e:#}");
            }
        }

        seq += 1;

        // Pace between sends, but cut the wait short if Ctrl-C arrived mid-sleep.
        if args.interval_ms > 0 && !stop.load(Ordering::SeqCst) {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(args.interval_ms)) => {}
                _ = notify.notified() => {}
            }
        }
    }

    println!("\nDone. {ok} accepted, {failed} failed, {total_bytes} bytes delivered.");
    if failed > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_connection_string_with_entity_path() {
        let cs = "Endpoint=sb://demo.servicebus.windows.net/;SharedAccessKeyName=Send;SharedAccessKey=abc==;EntityPath=myhub";
        let cfg = parse_connection_string(cs).unwrap();
        assert_eq!(cfg.fqdn, "demo.servicebus.windows.net");
        assert_eq!(cfg.event_hub, "myhub");
        assert_eq!(cfg.sas_key_name, "Send");
        // The '=' padding inside the key must survive (split_once, not split).
        assert_eq!(cfg.sas_key, "abc==");
    }

    #[test]
    fn parses_connection_string_without_entity_path() {
        let cs =
            "Endpoint=sb://demo.servicebus.windows.net/;SharedAccessKeyName=Root;SharedAccessKey=k";
        let cfg = parse_connection_string(cs).unwrap();
        assert_eq!(cfg.fqdn, "demo.servicebus.windows.net");
        assert!(cfg.event_hub.is_empty());
    }

    #[test]
    fn resource_uri_and_messages_url_are_well_formed() {
        let cfg = EventHubConfig {
            fqdn: "demo.servicebus.windows.net".into(),
            event_hub: "myhub".into(),
            sas_key_name: "Send".into(),
            sas_key: "k".into(),
        };
        assert_eq!(
            cfg.resource_uri(),
            "https://demo.servicebus.windows.net/myhub"
        );
        assert_eq!(
            cfg.messages_url(),
            "https://demo.servicebus.windows.net/myhub/messages?timeout=60&api-version=2014-01"
        );
    }

    #[test]
    fn sas_token_has_required_fields_and_encoded_uri() {
        let token = sas_token(
            "https://demo.servicebus.windows.net/myhub",
            "Send",
            "secretkey",
            Duration::from_secs(3600),
        )
        .unwrap();
        assert!(token.starts_with("SharedAccessSignature "));
        // ':' and '/' in the resource URI must be percent-encoded in sr.
        assert!(token.contains("sr=https%3A%2F%2Fdemo.servicebus.windows.net%2Fmyhub"));
        assert!(token.contains("&sig="));
        assert!(token.contains("&se="));
        assert!(token.contains("&skn=Send"));
    }

    /// Golden test: byte-for-byte match against Microsoft's reference Python
    /// algorithm (urlencode(uri)+"\n"+expiry -> HMAC-SHA256 -> base64 -> urlencode)
    /// for fixed inputs. A drift here means a silent 401 against real Event Hubs.
    #[test]
    fn sas_token_matches_microsoft_reference() {
        let token = sas_token_with_expiry(
            "https://demo.servicebus.windows.net/myhub",
            "Send",
            "secretkey",
            1_700_000_000,
        )
        .unwrap();
        let expected = "SharedAccessSignature \
            sr=https%3A%2F%2Fdemo.servicebus.windows.net%2Fmyhub\
            &sig=XCxTTWaPVS%2B2w3EXdPdgpKom3eTQiaPgqKlSK3OmM0Y%3D\
            &se=1700000000&skn=Send";
        assert_eq!(token, expected);
    }
}
