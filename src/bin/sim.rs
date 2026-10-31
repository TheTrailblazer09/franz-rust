//! Deterministic crash simulator for the commit log.
//!
//! Drives the real storage code against a disk that can lose unsynced writes
//! and persist half of a record batch, and checks after every simulated crash
//! that the log is still a contiguous prefix of what was acknowledged.
//!
//! Every run is a seed, so a failure is a seed, and a seed reproduces exactly.

use franz::log::Sync;
use franz::sim::{self, Crash, Knobs};

const USAGE: &str = "\
usage: sim [options]
  --seeds N     how many seeds to run (default 2000)
  --from N      first seed (default 1)
  --seed N      run one seed and describe it
  --cycles N    crash and recover this many times per seed (default 3)
  --ablate      measure what each durability setting costs, instead of hunting
  --verbose     print every run, not just failures";

fn main() {
    let mut seeds: u64 = 2000;
    let mut from: u64 = 1;
    let mut one: Option<u64> = None;
    let mut cycles: u32 = 3;
    let mut verbose = false;
    let mut ablate = false;

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
            "--ablate" => ablate = true,
            "--verbose" => verbose = true,
            "--help" | "-h" => {
                println!("{USAGE}");
                return;
            }
            other => fail(&format!("unknown argument `{other}`")),
        }
    }

    if ablate {
        ablation(from, seeds);
        return;
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

// --- the ablation ----------------------------------------------------------

/// What one durability setting cost, over many seeds.
struct Cost {
    runs: u32,
    losing: u32,
    torn: u32,
    lost: Vec<i64>,
    acknowledged: i64,
}

impl Cost {
    fn new() -> Self {
        Cost {
            runs: 0,
            losing: 0,
            torn: 0,
            lost: Vec::new(),
            acknowledged: 0,
        }
    }

    fn add(&mut self, outcome: &sim::Outcome) {
        let lost = (outcome.acknowledged - outcome.recovered).max(0);
        self.runs += 1;
        self.acknowledged += outcome.acknowledged;
        if lost > 0 {
            self.losing += 1;
        }
        if outcome.truncated_bytes > 0 {
            self.torn += 1;
        }
        self.lost.push(lost);
    }

    fn at(&mut self, quantile: f64) -> i64 {
        if self.lost.is_empty() {
            return 0;
        }
        self.lost.sort_unstable();
        let index = ((self.lost.len() - 1) as f64 * quantile).round() as usize;
        self.lost[index]
    }

    /// Share of all acknowledged records that did not survive.
    fn loss_rate(&self) -> f64 {
        if self.acknowledged == 0 {
            return 0.0;
        }
        self.lost.iter().sum::<i64>() as f64 / self.acknowledged as f64 * 100.0
    }
}

/// Each durability setting against each crash mode, measured rather than
/// argued about.
///
/// One crash cycle per seed, so "lost" is a number you can read: the records
/// the broker had acknowledged and could not produce afterwards.
fn ablation(from: u64, seeds: u64) {
    // Count-based rather than time-based on purpose. The simulator has no
    // clock, so a run finishes in microseconds and every time-based interval
    // behaves exactly like `never`. That is itself worth knowing, and it is
    // written up in the README rather than papered over here.
    let settings: [(&str, Sync); 5] = [
        ("always", Sync::Always),
        ("every 4 batches", Sync::EveryN(4)),
        ("every 16 batches", Sync::EveryN(16)),
        ("every 64 batches", Sync::EveryN(64)),
        ("never", Sync::Never),
    ];

    for (mode, label) in [
        (Crash::ProcessKill, "process killed (page cache survives)"),
        (Crash::PowerLoss, "power lost (only synced bytes survive)"),
    ] {
        println!();
        println!("{label}, {seeds} seeds each");
        println!(
            "| --fsync | runs losing data | records lost p50 | p99 | worst | of all acked | torn tail |"
        );
        println!("| --- | --- | --- | --- | --- | --- | --- |");

        for (name, sync) in settings {
            let mut cost = Cost::new();
            let mut violations = 0;

            for seed in from..from + seeds {
                let knobs = Knobs {
                    sync: Some(sync),
                    crash: Some(mode),
                    cycles: 1,
                };
                match sim::run_with(seed, knobs) {
                    Ok(outcome) => cost.add(&outcome),
                    Err(_) => violations += 1,
                }
            }

            let note = if violations > 0 {
                format!(" ({violations} VIOLATIONS)")
            } else {
                String::new()
            };
            println!(
                "| `{}` | {:.0}% | {} | {} | {} | {:.2}% | {:.0}%{} |",
                name,
                cost.losing as f64 / cost.runs.max(1) as f64 * 100.0,
                cost.at(0.50),
                cost.at(0.99),
                cost.at(1.0),
                cost.loss_rate(),
                cost.torn as f64 / cost.runs.max(1) as f64 * 100.0,
                note,
            );
        }
    }

    println!();
    println!("A torn tail is a partial record batch left on disk for recovery to truncate.");
}
