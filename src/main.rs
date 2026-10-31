//! franz: a broker that speaks the Apache Kafka wire protocol.

mod api_versions;
mod protocol;

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};

use crate::protocol::{ApiKey, Reader, RequestHeader, Writer, codes, write_response_header};

const LISTEN: &str = "127.0.0.1:9092";

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(LISTEN)?;
    println!("franz listening on {LISTEN}");

    for client in listener.incoming() {
        let client = client?;
        let peer = client.peer_addr()?;
        println!("\n{peer} connected");

        match serve(client) {
            Ok(()) => println!("{peer} disconnected"),
            Err(e) => println!("{peer} dropped: {e}"),
        }
    }

    Ok(())
}

/// Reads frames until the client goes away.
fn serve(mut client: TcpStream) -> std::io::Result<()> {
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

        let Some(body) = respond(&frame) else {
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
fn respond(frame: &[u8]) -> Option<Vec<u8>> {
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
        ApiKey::ApiVersions => {
            api_versions::write_response(&mut w, header.api_version, codes::NONE)
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
