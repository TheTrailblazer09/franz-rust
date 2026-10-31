//! franz: a broker that speaks the Apache Kafka wire protocol.

mod api_versions;
mod metadata;
mod protocol;
mod topics;

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};

use crate::metadata::BrokerInfo;
use crate::protocol::{ApiKey, Reader, RequestHeader, Writer, codes, write_response_header};
use crate::topics::Topics;

const HOST: &str = "127.0.0.1";
const PORT: u16 = 9092;
const NODE_ID: i32 = 1;
const CLUSTER_ID: &str = "franz-dev";

fn main() -> std::io::Result<()> {
    let topics = match parse_args() {
        Ok(topics) => topics,
        Err(e) => {
            eprintln!("franz: {e}");
            eprintln!("usage: franz [--topic NAME:PARTITIONS]...");
            std::process::exit(2);
        }
    };

    let listen = format!("{HOST}:{PORT}");
    let listener = TcpListener::bind(&listen)?;
    println!("franz listening on {listen}");
    for t in topics.all() {
        println!("  topic {} with {} partitions", t.name, t.partitions);
    }

    for client in listener.incoming() {
        let client = client?;
        let peer = client.peer_addr()?;
        println!("\n{peer} connected");

        match serve(client, &topics) {
            Ok(()) => println!("{peer} disconnected"),
            Err(e) => println!("{peer} dropped: {e}"),
        }
    }

    Ok(())
}

/// `--topic NAME:PARTITIONS`, repeatable. An unrecognised argument is an
/// error rather than ignored: a typo that silently creates no topics looks
/// exactly like a broker bug from the client side.
fn parse_args() -> Result<Topics, String> {
    let mut specs = Vec::new();
    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--topic" => {
                let spec = args.next().ok_or("--topic needs NAME:PARTITIONS")?;
                specs.push(spec);
            }
            other => return Err(format!("unknown argument `{other}`")),
        }
    }

    Topics::from_specs(&specs)
}

/// Reads frames until the client goes away.
fn serve(mut client: TcpStream, topics: &Topics) -> std::io::Result<()> {
    loop {
        let mut prefix = [0u8; 4];
        match client.read_exact(&mut prefix) {
            Ok(()) => {}
            // End of stream between frames is how every client exits.
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        }

        let len = u32::from_be_bytes(prefix) as usize;
        let mut frame = vec![0u8; len];
        client.read_exact(&mut frame)?;

        println!("\nframe of {len} bytes:");
        hexdump(&frame);

        let Some(body) = respond(&frame, topics) else {
            println!("  nothing to say, closing");
            return Ok(());
        };

        println!("  replying with {} bytes:", body.len());
        hexdump(&body);
        client.write_all(&(body.len() as u32).to_be_bytes())?;
        client.write_all(&body)?;
        client.flush()?;
    }
}

/// Builds the response body for one request, or `None` when there is nothing
/// safe to say and the connection should be dropped.
///
/// Once decoding has failed there is no response shape left to guess at, and a
/// client that keeps writing into a desynchronised connection only corrupts
/// requests that would otherwise have been fine.
fn respond(frame: &[u8], topics: &Topics) -> Option<Vec<u8>> {
    let mut r = Reader::new(frame);
    let header = match RequestHeader::decode(&mut r) {
        Ok(header) => header,
        Err(e) => {
            println!("  unparseable: {e}");
            return None;
        }
    };

    println!(
        "  {} v{} from {}, correlation_id {}, {} bytes of body",
        header.api_key.name(),
        header.api_version,
        header.client_id.as_deref().unwrap_or("(no client_id)"),
        header.correlation_id,
        r.remaining(),
    );

    let mut w = Writer::new();
    write_response_header(
        &mut w,
        header.api_key,
        header.api_version,
        header.correlation_id,
    );

    match header.api_key {
        ApiKey::ApiVersions if !header.api_key.supports(header.api_version) => {
            println!(
                "  v{} is past what we speak, telling it to retry",
                header.api_version
            );
            api_versions::write_unsupported(&mut w);
        }
        ApiKey::ApiVersions => api_versions::write_response(
            &mut w,
            header.api_version,
            codes::NONE,
            &api_versions::advertised(),
        ),
        ApiKey::Metadata => {
            let request = match metadata::Request::decode(&mut r, header.api_version) {
                Ok(request) => request,
                Err(e) => {
                    println!("  bad Metadata body: {e}");
                    return None;
                }
            };
            match &request.topics {
                None => println!("  asked about every topic"),
                Some(names) => println!("  asked about {names:?}"),
            }

            let broker = BrokerInfo {
                node_id: NODE_ID,
                host: HOST,
                port: PORT as i32,
                cluster_id: CLUSTER_ID,
            };
            metadata::write_response(
                &mut w,
                header.api_version,
                &broker,
                topics,
                request.topics.as_deref(),
            );
        }
        other => {
            println!("  {} has no handler yet", other.name());
            return None;
        }
    }

    Some(w.into_inner())
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
