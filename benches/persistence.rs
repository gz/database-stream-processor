use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use dbsp::trace::ord as dram_ord;
use dbsp::trace::ord::persistent as persistent_ord;
use dbsp::trace::BatchReader;
use dbsp::trace::Builder;
use dbsp::trace::Cursor;
use rand::prelude::Distribution;
use rand::Rng;
use zipf::ZipfDistribution;

/// What different sizes we should instantiate for the set.
static BENCHMARK_INPUTS: [u64; 2] = [1 << 12, 1 << 24];

/// Generate a random sequence of operations
///
/// # Arguments
///  - `nop`: Number of operations to generate
///  - `write`: true will Put, false will generate Get sequences
///  - `span`: Maximum key
///  - `distribution`: Supported distribution 'uniform' or 'skewed'
pub fn generate_operations(nop: usize, span: usize, distribution: &'static str) -> Vec<u64> {
    assert!(distribution == "skewed" || distribution == "uniform");

    let mut ops = Vec::with_capacity(nop);

    let skewed = distribution == "skewed";
    let mut t_rng = rand::thread_rng();
    let zipf = ZipfDistribution::new(span, 1.03).unwrap();

    for _idx in 0..nop {
        let id = if skewed {
            // skewed
            zipf.sample(&mut t_rng) as u64
        } else {
            // uniform
            t_rng.gen_range(0..span as u64)
        };
        ops.push(id);
    }

    ops
}

fn persistent_builder(n: u64) -> persistent_ord::zset_batch::OrdZSet<u64, u64> {
    let mut builder = persistent_ord::zset_batch::OrdZSetBuilder::new(());
    for i in 0..n {
        builder.push((i, (), i));
    }
    builder.done()
}

fn dram_builder(n: u64) -> dram_ord::zset_batch::OrdZSet<u64, u64> {
    let mut builder = dram_ord::zset_batch::OrdZSetBuilder::new(());
    for i in 0..n {
        builder.push((i, (), i));
    }
    builder.done()
}

/// Benchmarks the performance overhead of the persistent OrdZSetBuilder
/// compared to the DRAM version.
fn bench_zset_builder(c: &mut Criterion) {
    let mut group = c.benchmark_group("OrdZSetBuilder");
    for i in BENCHMARK_INPUTS.iter() {
        group.bench_with_input(BenchmarkId::new("Persistent", i), i, |b, i| {
            b.iter(|| persistent_builder(*i))
        });
        group.bench_with_input(BenchmarkId::new("DRAM", i), i, |b, i| {
            b.iter(|| dram_builder(*i))
        });
    }
    group.finish();
}

/// Benchmarks the performance overhead of the persistent OrdZSet Cursor
/// compared to the DRAM version for sequential walks through the entire
/// key-space.
fn bench_zset_cursor_seq_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("OrdZSet Cursor Sequential Walk");
    for i in BENCHMARK_INPUTS.iter() {
        group.bench_with_input(BenchmarkId::new("Persistent", i), i, |b, i| {
            let persistent_zset = persistent_builder(*i);
            b.iter(|| {
                let mut cursor = persistent_zset.cursor();
                let mut idx = 0;
                while cursor.key_valid() {
                    assert_eq!(*cursor.key(), idx);
                    cursor.step_key();
                    idx += 1;
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("DRAM", i), i, |b, i| {
            let dram_zset = dram_builder(*i);
            b.iter(|| {
                let mut cursor = dram_zset.cursor();
                let mut idx = 0;
                while cursor.key_valid() {
                    assert_eq!(*cursor.key(), idx);
                    cursor.step_key();
                    idx += 1;
                }
            });
        });
    }
    group.finish();
}

/// Benchmarks the performance overhead of the persistent OrdZSet Cursor
/// compared to the DRAM version for seeking random keys.
///
/// Note: We measure 1000 seeks, while varying the set size
fn seek_template(c: &mut Criterion, random_distribution: &'static str) {
    const NUM_SEEKS: usize = 1000;
    let name = format!("OrdZSet Cursor Seek {} Random", random_distribution);
    let mut group = c.benchmark_group(name);

    for i in BENCHMARK_INPUTS.iter() {
        let random_keys = generate_operations(NUM_SEEKS, *i as usize - 1, random_distribution);

        group.bench_with_input(BenchmarkId::new("Persistent", i), i, |b, i| {
            let persistent_zset = persistent_builder(*i);
            let mut cursor = persistent_zset.cursor();
            b.iter(|| {
                for key in random_keys.iter() {
                    cursor.seek_key(&key);
                    assert!(cursor.key_valid());
                    cursor.rewind_keys();
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("DRAM", i), i, |b, i| {
            let dram_zset = dram_builder(*i);
            let mut cursor = dram_zset.cursor();
            b.iter(|| {
                for key in random_keys.iter() {
                    cursor.seek_key(&key);
                    assert!(cursor.key_valid());
                    cursor.rewind_keys();
                }
            });
        });
    }
    group.finish();
}

/// Seek with uniform input key distribution.
fn bench_zset_cursor_seek_uniform(c: &mut Criterion) {
    seek_template(c, "uniform")
}

/// Seek with skewed input key distribution.
fn bench_zset_cursor_seek_skewed(c: &mut Criterion) {
    seek_template(c, "skewed")
}

criterion_group!(
    benches,
    bench_zset_builder,
    bench_zset_cursor_seq_access,
    bench_zset_cursor_seek_uniform,
    bench_zset_cursor_seek_skewed,
);
criterion_main!(benches);
