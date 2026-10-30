//! franz: a broker that speaks the Apache Kafka wire protocol.
//!
//! Step 1 reads messages off the wire and prints them. Nothing is answered
//! yet. The point is to establish the one rule everything later depends on:
//! every Kafka message is a 4-byte big-endian length followed by exactly that
//! many bytes. Read the length, read exactly that many bytes, then parse.
//!
//! Getting that wrong is worth understanding before writing any more. A short
//! read treated as a whole message does not fail here — it fails on the *next*
//! message, because the bytes left over are read as that message's length
//! prefix. From then on the connection is garbage and the error points
//! nowhere near the mistake.

use std::io::{ErrorKind, Read};
use std::net::{TcpListener, TcpStream};

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

/// Reads frames until the client goes away. One connection at a time, which is
/// all step 1 needs.
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
