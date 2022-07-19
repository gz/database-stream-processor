//! Make sure that our persistent versions have equivalent behaviour as
//! non-persistent versions.

use std::ops::Range;
use std::vec::Vec;

use bincode::Decode;
use bincode::Encode;
use proptest::prelude::*;
use proptest_derive::Arbitrary;

use crate::algebra::MonoidValue;
use crate::trace::cursor::Cursor;
use crate::trace::ord as dram_ord;
use crate::trace::ord::persistent as persistent_ord;
use crate::trace::BatchReader;
use crate::trace::Builder;

/// This is a "complex" key because it defines a custom ordering logic & has a
/// heap allocated [`String`] inside of it. The tests ensure that the RocksDB
/// based data-structure adhere to the same ordering as the DRAM based version
/// which is defined through the [`Ord`] trait.
#[derive(Clone, Debug, Encode, Decode, Arbitrary)]
struct ComplexKey {
    /// We ignore this type for ordering.
    _a: isize,
    /// We use this to define the order of `Self`.
    ord: String,
}

impl PartialEq for ComplexKey {
    fn eq(&self, other: &Self) -> bool {
        self.ord.eq(&other.ord)
    }
}

impl Eq for ComplexKey {}

impl PartialOrd for ComplexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.ord.partial_cmp(&other.ord)
    }
}

impl Ord for ComplexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ord.cmp(&other.ord)
    }
}

/// Mutable commands on [`Cursor`].
#[derive(Debug, Clone)]
enum CursorAction<K: Arbitrary + Clone, V: Arbitrary + Clone> {
    StepKey,
    StepVal,
    SeekKey(K),
    SeekVal(V),
    RewindKeys,
    RewindVals,
    Key,
    Val,
    /// Weight is a mutable operation on on the cursor so we model it as such,
    /// even though in most cases, it doesn't seem to mutate.
    Weight,
    MapTimes,
}

fn action<K: Arbitrary + Clone, V: Arbitrary + Clone>() -> impl Strategy<Value = CursorAction<K, V>>
{
    // Generate possible invocations we can call on the respective cursors.
    prop_oneof![
        Just(CursorAction::StepKey),
        Just(CursorAction::StepVal),
        Just(CursorAction::RewindKeys),
        Just(CursorAction::RewindVals),
        Just(CursorAction::Key),
        Just(CursorAction::Val),
        Just(CursorAction::Weight),
        Just(CursorAction::MapTimes),
        any::<V>().prop_map(CursorAction::SeekVal),
        any::<K>().prop_map(CursorAction::SeekKey),
    ]
}

fn actions<K: Arbitrary + Clone, V: Arbitrary + Clone>(
) -> impl Strategy<Value = Vec<CursorAction<K, V>>> {
    prop::collection::vec(action::<K, V>(), 0..512)
}

fn keys<K: Arbitrary + Clone>(range: Range<usize>) -> impl Strategy<Value = Vec<K>> {
    prop::collection::vec(any::<K>(), range)
}

proptest! {
    // Verify that our [`Cursor`] implementation for the persistent [`OrdZSet`]
    // behaves the same as the non-persistent [`OrdZSet`] cursor.
    #[test]
    fn ord_zet_cursor_equivalence(mut ks in keys::<ComplexKey>(0..512), ops in actions::<ComplexKey, ()>()) {
        // Builder interface wants sorted, unique(?) keys:
        ks.sort_unstable();
        ks.dedup();

        // Instantiate a regular OrdZSet
        let mut model_builder = dram_ord::zset_batch::OrdZSetBuilder::new(());
        for key in ks.iter() {
            model_builder.push((key.clone(), (), 0));
        }

        let model = model_builder.done();
        let mut model_cursor = model.cursor();

        // Instantiate a persistent OrdZSet
        let mut totest_builder = persistent_ord::zset_batch::OrdZSetBuilder::new(());
        for key in ks.iter() {
            totest_builder.push((key.clone(), (), 0));
        }
        let totest = totest_builder.done();
        let mut totest_cursor = totest.cursor();

        // We check the non-mutating cursor interface after every
        // command/mutation.
        fn check_eq_invariants<K,R>(step: usize, model_cursor: &dram_ord::zset_batch::OrdZSetCursor<K, R>, totest_cursor: &persistent_ord::zset_batch::OrdZSetCursor<K, R>)
        where
            K: Ord + Clone + Encode + Decode,
            R: MonoidValue + Encode + Decode
        {
            assert_eq!(model_cursor.key_valid(), totest_cursor.key_valid(), "key_valid() mismatch in step {}", step);
            assert_eq!(model_cursor.val_valid(), totest_cursor.val_valid(), "val_valid() mismatch in step {}", step);
        }

        assert_eq!(totest.len(), model.len());
        for (i, action) in ops.iter().enumerate() {
            match action {
                CursorAction::StepKey => {
                    model_cursor.step_key();
                    totest_cursor.step_key();
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                },
                CursorAction::StepVal => {
                    model_cursor.step_val();
                    totest_cursor.step_val();
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                },
                CursorAction::SeekKey(k) => {
                    model_cursor.seek_key(&k);
                    totest_cursor.seek_key(&k);
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                },
                CursorAction::SeekVal(v) => {
                    model_cursor.seek_val(&v);
                    totest_cursor.seek_val(&v);
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                },
                CursorAction::RewindKeys => {
                    model_cursor.rewind_keys();
                    totest_cursor.rewind_keys();
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                },
                CursorAction::RewindVals => {
                    model_cursor.rewind_vals();
                    totest_cursor.rewind_vals();
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                },
                CursorAction::Key => {
                    if model_cursor.key_valid() {
                        assert_eq!(model_cursor.key(), totest_cursor.key());
                    }
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                },
                CursorAction::Val => {
                    if model_cursor.val_valid() {
                        assert_eq!(model_cursor.val(), totest_cursor.val());
                    }
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                },
                CursorAction::Weight => {
                    if model_cursor.key_valid() {
                        assert_eq!(model_cursor.weight(), totest_cursor.weight());
                    }
                    check_eq_invariants(i, &model_cursor, &totest_cursor);
                }
                CursorAction::MapTimes => {
                    let mut model_invocations = Vec::new();
                    let mut test_invocation = Vec::new();
                    model_cursor.map_times(|v, t| {
                        model_invocations.push((v.clone(), t.clone()));
                    });
                    totest_cursor.map_times(|v, t| {
                        test_invocation.push((v.clone(), t.clone()));
                    });
                    assert_eq!(model_invocations, test_invocation);
                }
            }
        }
    }
}

#[test]
fn zset_duplicates() {
    // Instantiate a regular OrdZSet
    let mut model_builder = dram_ord::zset_batch::OrdZSetBuilder::new(());
    model_builder.push((1, (), 1));
    model_builder.push((1, (), 1));
    model_builder.push((1, (), 2));

    let _model = model_builder.done();
    //eprintln!("{}", model);
}

#[test]
fn zset_display() {
    let mut model_builder = dram_ord::zset_batch::OrdZSetBuilder::new(());
    model_builder.push((1, (), 9));
    model_builder.push((2, (), 4));
    model_builder.push((3, (), 4));

    let model = model_builder.done();
    let display_model = format!("{}", model);

    let mut totest_builder = persistent_ord::zset_batch::OrdZSetBuilder::new(());
    totest_builder.push((1, (), 9));
    totest_builder.push((2, (), 4));
    totest_builder.push((3, (), 4));

    let totest = totest_builder.done();
    let display_totest = format!("{}", totest);
    assert_eq!(display_model, display_totest);
}
