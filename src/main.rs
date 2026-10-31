//! franz: a broker that speaks the Apache Kafka wire protocol.
//! Step 1 reads messages off the wire and prints them. Nothing is answered
//! yet. The point is to establish the one rule everything later depends on:
//! every Kafka message is a 4-byte big-endian length followed by exactly that
//! many bytes. Read the length, read exactly that many bytes, then parse.
//!

mod protocol;

use std::io::{ErrorKind, Read};
use std::net::{TcpListener, TcpStream};

use crate::protocol::{Reader, RequestHeader};

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
        describe(&frame);
    }
}

/// Says what the client asked for, without acting on it.
fn describe(frame: &[u8]) {
    let mut r = Reader::new(frame);
    match RequestHeader::decode(&mut r) {
        Ok(h) => println!(
            "  {} v{} from {}, correlation_id {}, {} bytes of body",
            h.api_key.name(),
            h.api_version,
            h.client_id.as_deref().unwrap_or("(no client_id)"),
            h.correlation_id,
            r.remaining(),
        ),
        Err(e) => println!("  unparseable: {e}"),
    }
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
