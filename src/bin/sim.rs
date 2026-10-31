//! Deterministic crash simulator for the commit log.
//!
//! Drives the real storage code against a disk that can lose unsynced writes
//! and persist half of a record batch, and checks after every simulated crash
//! that the log is still a contiguous prefix of what was acknowledged.
//!
//! Every run is a seed, so a failure is a seed, and a seed reproduces exactly.

use franz::log::Sync;
use franz::sim::{self, Crash};

const USAGE: &str = "\
usage: sim [options]
  --seeds N     how many seeds to run (default 2000)
  --from N      first seed (default 1)
  --seed N      run one seed and describe it
  --cycles N    crash and recover this many times per seed (default 3)
  --verbose     print every run, not just failures";

fn main() {
    let mut seeds: u64 = 2000;
    let mut from: u64 = 1;
    let mut one: Option<u64> = None;
    let mut cycles: u32 = 3;
    let mut verbose = false;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let mut value = || {
            args.next()
                .unwrap_or_else(|| fail(&format!("{arg} needs a value")))
        };
        match arg.as_str() {
            "--seeds" => seeds = number(&value()),
            "--from" => from = number(&value()),
            "--seed" => one = Some(number(&value())),
            "--cycles" => cycles = number(&value()) as u32,
            "--verbose" => verbose = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                return;
            }
            other => fail(&format!("unknown argument `{other}`")),
        }
    }

    if let Some(seed) = one {
        match sim::run_cycles(seed, cycles) {
            Ok(outcome) => {
                println!("seed {seed}: ok");
                describe(&outcome);
            }
            Err(v) => {
                println!("seed {seed}: VIOLATION");
                println!("  {}", v.what);
                std::process::exit(1);
            }
        }
        return;
    }

    println!(
        "running seeds {from}..{} with {cycles} crash cycle(s) each",
        from + seeds
    );

    let mut torn = 0u64;
    let mut lost_records = 0u64;
    let mut power_loss = 0u64;
    let mut failures = Vec::new();

    for seed in from..from + seeds {
        match sim::run_cycles(seed, cycles) {
            Ok(outcome) => {
                if outcome.truncated_bytes > 0 {
                    torn += 1;
                }
                if outcome.crash == Crash::PowerLoss {
                    power_loss += 1;
                }
                if outcome.sync != Sync::Always && outcome.recovered < outcome.acknowledged {
                    lost_records += 1;
                }
                if verbose {
                    print!("seed {seed}: ok  ");
                    describe(&outcome);
                }
            }
            Err(v) => {
                println!("seed {}: VIOLATION — {}", v.seed, v.what);
                failures.push(v.seed);
            }
        }
    }

    println!();
    println!("{seeds} seeds, {} crash(es) each", cycles);
    println!(
        "  {power_loss} power-loss runs, {} process-kill runs",
        seeds - power_loss
    );
    println!("  {torn} runs had a partial batch on disk for recovery to truncate");
    println!("  {lost_records} runs lost tail records that were never synced");

    if failures.is_empty() {
        println!();
        println!("no violations");
    } else {
        println!();
        println!(
            "{} violation(s): reproduce with --seed {}",
            failures.len(),
            failures[0]
        );
        std::process::exit(1);
    }
}

fn describe(outcome: &sim::Outcome) {
    println!(
        "  crash={:?} fsync={} acknowledged={} recovered={} truncated={}B on_disk={}B",
        outcome.crash,
        sim::describe(outcome.sync),
        outcome.acknowledged,
        outcome.recovered,
        outcome.truncated_bytes,
        outcome.bytes_on_disk,
    );
}

fn number(raw: &str) -> u64 {
    raw.parse()
        .unwrap_or_else(|_| fail(&format!("`{raw}` is not a number")))
}

fn fail(message: &str) -> ! {
    eprintln!("sim: {message}");
    eprintln!("{USAGE}");
    std::process::exit(2);
}
