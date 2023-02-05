//! Simple DBSP example.
//!
//! This is similar to the motivating example in chapter 1 of the Differential
//! Dataflow book at <https://timelydataflow.github.io/differential-dataflow/chapter_0/chapter_0.html>.  It takes a
//! collection of `Manages` structs that map from an employee ID to the
//! employee's manager, and outputs a collection of `Analysis` structs that
//! also include the employee's second-level manager.

use std::hash::Hash;

use anyhow::Result;
use bincode::{Decode, Encode};
use clap::Parser;
use dbsp::{
    Circuit,
    algebra::F64,
    operator::{Avg, FilterMap, time_series::{RelRange, RelOffset}},
    trace::{BatchReader, Cursor},
    OrdIndexedZSet, OutputHandle, Runtime, Stream,
};
use size_of::SizeOf;
use serde::{de::Error as _, Deserialize, Deserializer};
use indicatif::{ProgressBar, ProgressStyle};

fn primitive_date_from_str<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<time::Date, D::Error> {
    let s: &str = Deserialize::deserialize(d)?;

    let format = time::macros::format_description!("[month]/[day]/[year]");

    match time::Date::parse(s, &format) {
        Ok(o) => Ok(o),
        Err(err) => Err(D::Error::custom(err)),
    }
}
fn primitive_time_from_str<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<time::Time, D::Error> {
    let s: &str = Deserialize::deserialize(d)?;

    let format = time::macros::format_description!("[hour]:[minute]");

    match time::Time::parse(s, &format) {
        Ok(o) => Ok(o),
        Err(err) => Err(D::Error::custom(err)),
    }
}

/*
fn price_from_str<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<u64, D::Error> {
    let s: &str = Deserialize::deserialize(d)?;

    let mut multiplier = s.chars().count() as u32 - 2;
    let mut n: u64 = 0;

    for c in s.chars() {
        if c == '.' {
            continue;
        }
        if c.is_ascii_digit() {
            n += u64::pow(10, multiplier) * c.to_digit(10).unwrap() as u64;
            multiplier -= 1;
        }
        else {
            panic!("Invalid price character: {}", c);
            return Err(D::Error::custom("Invalid price character"));
        }
    }

    Ok(n)
}
 */

/// Some forex data input.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf, Debug, Encode, Decode, Deserialize)]
struct TickerData {
    #[bincode(with_serde)]
    #[serde(deserialize_with = "primitive_date_from_str")]
    date: time::Date,
    #[bincode(with_serde)]
    #[serde(deserialize_with = "primitive_time_from_str")]
    time: time::Time,
    open: F64,
    high: F64,
    low: F64,
    close: F64,
    volume: u64,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf, Debug, Encode, Decode, Deserialize)]
enum Action {
    Buy,
    Sell,
}

/// Some output for the forex stuff.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, SizeOf, Encode, Decode, Deserialize)]
struct Decision {
    action: Action,
    avg_fast: std::option::Option<dbsp::algebra::F64>,
    avg_fast_prev: std::option::Option<dbsp::algebra::F64>,
    avg_slow: std::option::Option<dbsp::algebra::F64>,
    avg_slow_prev: std::option::Option<dbsp::algebra::F64>
}

type Weight = i32;
type AnalysisBatch = OrdIndexedZSet<(&'static str, i64), Decision, i32>;

fn _print_output(output: &OutputHandle<AnalysisBatch>) {
    let output = output.consolidate();
    let mut cursor = output.cursor();
    while cursor.key_valid() {
        let weight = cursor.weight();

        println!("{:?} {:?} {}", cursor.key(), cursor.val(), weight);
        cursor.step_key();
    }
    println!();
}

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Number of threads.
    #[clap(long, default_value = "2")]
    threads: usize,
}

fn main() -> Result<()> {
    let Args { threads } = Args::parse();

    let (mut dbsp, (mut hmanages, output)) = Runtime::init_circuit(threads, |circuit| {
        let (manages, hmanages) = circuit.add_input_zset::<TickerData, Weight>();

        let volumes = manages.map_index(|t| { 
            let timestamp = t.date.with_time(t.time).assume_utc().unix_timestamp();
            ("OIH", (timestamp, t.clone()))
        });

        let make_rolling_avg = |s: &Stream<Circuit<()>, OrdIndexedZSet<_, (i64, TickerData), _>>, from, to| {
            s.partitioned_rolling_aggregate_linear(
                |v| Avg::new(v.close, 1),
                |avg| avg.compute_avg().unwrap(),
                RelRange::new(from, to),
            ).map_index(|(symb, (ts, avg))| ((*symb, *ts), *avg))
        };
        let rolling_avg_close_10_prev = make_rolling_avg(&volumes, RelOffset::Before(2*60), RelOffset::Before(1*60));
        let rolling_avg_close_10 = make_rolling_avg(&volumes, RelOffset::Before(1*60), RelOffset::Before(0));

        let rolling_avg_close_20_prev = make_rolling_avg(&volumes, RelOffset::Before(4*60), RelOffset::Before(2*60));
        let rolling_avg_close_20 = make_rolling_avg(&volumes, RelOffset::Before(2*60), RelOffset::Before(0));

        let avgs = rolling_avg_close_10.join_index::<(), _, _, _, _, _>(
            &rolling_avg_close_20, 
            |&symb_ts, avg_10, avg_20| 
                Some((symb_ts, (*avg_10, *avg_20))
            ));
        let prev_avgs = rolling_avg_close_10_prev.join_index::<(), _, _, _, _, _>(
                &rolling_avg_close_20_prev, 
                |&symb_ts, avg_10, avg_20| 
                Some((symb_ts, (*avg_10, *avg_20))
            ));
    
        let crossover = avgs.join_index::<(), _, _, _, _, _>(
            &prev_avgs, 
            |&symb_ts, (avg_10, avg_20), (prev_avg_10, prev_avg_20)| 
                if avg_10 > avg_20 && prev_avg_10 < prev_avg_20 {
                    Some((symb_ts, Decision {
                        action: Action::Buy,
                        avg_fast: *avg_10, 
                        avg_fast_prev: *prev_avg_10, 
                        avg_slow: *avg_20, 
                        avg_slow_prev: *prev_avg_20
                }))
                }
                else if avg_20 > avg_10 && prev_avg_20 < prev_avg_10 {
                    Some((symb_ts, Decision {
                        action: Action::Sell,
                        avg_fast: *avg_10, 
                        avg_fast_prev: *prev_avg_10, 
                        avg_slow: *avg_20, 
                        avg_slow_prev: *prev_avg_20
                }))
                }
                else {
                    None
                }
            );
        (hmanages, crossover.output())
    })
    .unwrap();
    dbsp.step().unwrap();
    
    let progress_bar = ProgressBar::new(1_975_031);
    progress_bar.set_style(
        ProgressStyle::with_template(
            "{human_pos} / {human_len} [{wide_bar}] {percent:.2} % {per_sec:.2} {eta}",
        )
        .unwrap()
        .progress_chars("=>-"),
    );

    println!("Ingesting data...");
    let mut rdr = csv::ReaderBuilder::new()
        .buffer_capacity(2*1024*1024)
        .has_headers(false)
        .from_path("examples/OIH_adjusted.csv").unwrap();

    let mut batch = Vec::with_capacity(10_000);
    let mut raw_record = csv::ByteRecord::new();
    while rdr.read_byte_record(&mut raw_record)? {
        let record: TickerData = raw_record.deserialize(None)?;
        //println!("{record:?}");
        batch.push((record, 1));

        if batch.len() % 10_000 == 0 {
            progress_bar.inc(batch.len() as u64);
            hmanages.append(&mut batch);
            dbsp.step().unwrap();
            //print_output(&output)
        }
    }
    output.consolidate();
    dbsp.kill().unwrap();
    progress_bar.finish_with_message("Done");

    Ok(())
}