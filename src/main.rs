//! franz: a broker that speaks the Apache Kafka wire protocol.

mod api_versions;
mod batch;
mod broker;
mod crc32c;
mod fetch;
mod list_offsets;
mod log;
mod metadata;
mod produce;
mod protocol;
mod topics;

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::broker::Broker;
use crate::metadata::BrokerInfo;
use crate::protocol::{ApiKey, Reader, RequestHeader, Writer, codes, write_response_header};
use crate::topics::Topics;

const NODE_ID: i32 = 1;
const CLUSTER_ID: &str = "franz-dev";

/// Largest frame we will read, matching Kafka's own default. Without a ceiling
/// four bytes of noise can ask the broker to allocate whatever they can say.
const MAX_FRAME_BYTES: u32 = 100 * 1024 * 1024;

struct Settings {
    host: String,
    port: u16,
    data_dir: PathBuf,
    verbose: bool,
    topics: Topics,
    log: log::Config,
}

/// What to do with a request once it has been handled.
enum Reply {
    Send(Vec<u8>),
    /// Produce with acks=0: the client is not waiting for anything, and
    /// answering would desynchronise the connection.
    Silent,
    /// Nothing safe is left to say, so the connection goes.
    Close,
}

fn main() -> std::io::Result<()> {
    let settings = match parse_args() {
        Ok(settings) => settings,
        Err(e) => {
            eprintln!("franz: {e}");
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };

    println!(
        "data in {}, fsync {}",
        settings.data_dir.display(),
        describe_sync(settings.log.sync)
    );

    // Opened before the port, so a client never connects to a broker that is
    // still deciding where its logs end.
    let opened = Instant::now();
    let broker = Arc::new(Broker::open(
        settings.topics,
        &settings.data_dir,
        settings.log,
    )?);
    println!("recovered in {:.3}s", opened.elapsed().as_secs_f64());

    let listen = format!("{}:{}", settings.host, settings.port);
    let listener = TcpListener::bind(&listen)?;
    println!("franz listening on {listen}");

    let info = Arc::new(BrokerInfoOwned {
        host: settings.host,
        port: settings.port as i32,
    });
    let verbose = settings.verbose;

    for client in listener.incoming() {
        let client = client?;
        let broker = Arc::clone(&broker);
        let info = Arc::clone(&info);

        // A thread each, rather than one connection at a time. Clients keep
        // several connections open at once and expect all of them served.
        std::thread::spawn(move || {
            let peer = match client.peer_addr() {
                Ok(peer) => peer,
                Err(_) => return,
            };
            // Request and response over a long-lived connection, so Nagle has
            // nothing to batch and only delays each small reply.
            let _ = client.set_nodelay(true);

            if verbose {
                println!("\n{peer} connected");
            }
            match serve(client, &broker, &info, verbose) {
                Ok(()) => {
                    if verbose {
                        println!("{peer} disconnected");
                    }
                }
                Err(e) => println!("{peer} dropped: {e}"),
            }
        });
    }

    Ok(())
}

struct BrokerInfoOwned {
    host: String,
    port: i32,
}

const USAGE: &str = "\
usage: franz [options]
  --topic NAME:PARTITIONS   create a topic at startup, repeatable
  --data-dir PATH           where logs live (default ./data)
  --host HOST               address to listen on and advertise (default 127.0.0.1)
  --port PORT               default 9092
  --fsync always|never|MS   durability policy (default never)
  --segment-bytes N         roll a segment past this size (default 64 MiB)
  --index-interval N        one index entry per N bytes of log (default 4096)
  --verbose                 print every frame";

fn parse_args() -> Result<Settings, String> {
    let mut specs = Vec::new();
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 9092;
    let mut data_dir = PathBuf::from("data");
    let mut verbose = false;
    let mut cfg = log::Config::default();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || args.next().ok_or(format!("{arg} needs a value"));
        match arg.as_str() {
            "--topic" => specs.push(value()?),
            "--data-dir" => data_dir = PathBuf::from(value()?),
            "--host" => host = value()?,
            "--port" => port = parse(&value()?, "port")?,
            "--segment-bytes" => cfg.segment_bytes = parse(&value()?, "segment-bytes")?,
            "--index-interval" => cfg.index_interval_bytes = parse(&value()?, "index-interval")?,
            "--fsync" => {
                let raw = value()?;
                cfg.sync = match raw.as_str() {
                    "always" => log::Sync::Always,
                    "never" => log::Sync::Never,
                    ms => log::Sync::Interval(Duration::from_millis(parse(ms, "fsync")?)),
                };
            }
            "--verbose" => verbose = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument `{other}`")),
        }
    }

    Ok(Settings {
        host,
        port,
        data_dir,
        verbose,
        topics: Topics::from_specs(&specs)?,
        log: cfg,
    })
}

fn parse<T: std::str::FromStr>(raw: &str, what: &str) -> Result<T, String> {
    raw.parse()
        .map_err(|_| format!("`{raw}` is not a valid {what}"))
}

fn describe_sync(sync: log::Sync) -> String {
    match sync {
        log::Sync::Always => "every write (survives power loss)".to_string(),
        log::Sync::Never => "never (survives a process crash, not power loss)".to_string(),
        log::Sync::Interval(every) => {
            format!(
                "every {}ms (loses up to that on power loss)",
                every.as_millis()
            )
        }
    }
}

/// Reads frames until the client goes away. One request at a time: clients
/// pipeline and expect replies in the order they asked.
fn serve(
    mut client: TcpStream,
    broker: &Broker,
    info: &BrokerInfoOwned,
    verbose: bool,
) -> std::io::Result<()> {
    let mut frame = Vec::new();

    loop {
        let mut prefix = [0u8; 4];
        match client.read_exact(&mut prefix) {
            Ok(()) => {}
            // End of stream between frames is how every client exits.
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }

        let len = u32::from_be_bytes(prefix);
        if len == 0 || len > MAX_FRAME_BYTES {
            return Err(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("length prefix of {len} is not a request"),
            ));
        }

        frame.clear();
        frame.resize(len as usize, 0);
        client.read_exact(&mut frame)?;

        if verbose {
            println!("\nframe of {len} bytes:");
            hexdump(&frame);
        }

        match respond(&frame, broker, info, verbose) {
            Reply::Send(body) => {
                if verbose {
                    println!("  replying with {} bytes", body.len());
                }
                client.write_all(&(body.len() as u32).to_be_bytes())?;
                client.write_all(&body)?;
                client.flush()?;
            }
            Reply::Silent => {}
            Reply::Close => {
                if verbose {
                    println!("  nothing to say, closing");
                }
                return Ok(());
            }
        }
    }
}

fn respond(frame: &[u8], broker: &Broker, info: &BrokerInfoOwned, verbose: bool) -> Reply {
    let mut r = Reader::new(frame);
    let header = match RequestHeader::decode(&mut r) {
        Ok(header) => header,
        Err(e) => {
            println!("  unparseable request: {e}");
            return Reply::Close;
        }
    };

    if verbose {
        println!(
            "  {} v{} from {}, correlation_id {}",
            header.api_key.name(),
            header.api_version,
            header.client_id.as_deref().unwrap_or("(none)"),
            header.correlation_id,
        );
    }

    let unsupported = !header.api_key.supports(header.api_version);
    if unsupported && header.api_key != ApiKey::ApiVersions {
        println!(
            "  {} v{} is outside what we advertised",
            header.api_key.name(),
            header.api_version
        );
        return Reply::Close;
    }

    let mut w = Writer::new();
    write_response_header(
        &mut w,
        header.api_key,
        header.api_version,
        header.correlation_id,
    );
    let version = header.api_version;

    match header.api_key {
        ApiKey::ApiVersions if unsupported => api_versions::write_unsupported(&mut w),
        ApiKey::ApiVersions => {
            api_versions::write_response(&mut w, version, codes::NONE, &api_versions::advertised())
        }

        ApiKey::Metadata => {
            let request = match metadata::Request::decode(&mut r, version) {
                Ok(request) => request,
                Err(e) => return bad_body("Metadata", e),
            };
            let broker_info = BrokerInfo {
                node_id: NODE_ID,
                host: &info.host,
                port: info.port,
                cluster_id: CLUSTER_ID,
            };
            metadata::write_response(
                &mut w,
                version,
                &broker_info,
                &broker.topics,
                request.topics.as_deref(),
            );
        }

        ApiKey::Produce => {
            let mut request = match produce::Request::decode(&mut r, version) {
                Ok(request) => request,
                Err(e) => return bad_body("Produce", e),
            };
            if verbose {
                println!(
                    "  {} records, acks={}",
                    produce::record_count(&request),
                    request.acks
                );
            }
            let acks = request.acks;
            let results = produce::apply(broker, &mut request);
            if acks == 0 {
                return Reply::Silent;
            }
            produce::write_response(&mut w, version, &results);
        }

        ApiKey::Fetch => {
            let request = match fetch::Request::decode(&mut r, version) {
                Ok(request) => request,
                Err(e) => return bad_body("Fetch", e),
            };
            let results = fetch::apply(broker, &request);
            fetch::write_response(&mut w, version, &results);
        }

        ApiKey::ListOffsets => {
            let request = match list_offsets::Request::decode(&mut r, version) {
                Ok(request) => request,
                Err(e) => return bad_body("ListOffsets", e),
            };
            let results = list_offsets::apply(broker, &request);
            list_offsets::write_response(&mut w, version, &results);
        }
    }

    Reply::Send(w.into_inner())
}

fn bad_body(api: &str, e: protocol::Error) -> Reply {
    println!("  bad {api} body: {e}");
    Reply::Close
}

fn hexdump(bytes: &[u8]) {
    for (row, chunk) in bytes.chunks(16).enumerate() {
        let hex: Vec<String> = chunk.iter().map(|b| format!("{b:02x}")).collect();
        let text: String = chunk
            .iter()
            .map(|&b| {
                if b == b' ' || b.is_ascii_graphic() {
                    b as char
                } else {
                    '.'
                }
            })
            .collect();
        println!("  {:04x}  {:<47}  {text}", row * 16, hex.join(" "));
    }
}
