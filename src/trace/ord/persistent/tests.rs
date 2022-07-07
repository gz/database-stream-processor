//! Make sure that our persistent versions have equivalent behaviour as
//! non-persistent versions.

use std::ops::Range;
use std::vec::Vec;

use proptest::prelude::*;

use crate::trace::cursor::Cursor;
use crate::trace::ord::persistent;
use crate::trace::ord::zset_batch::OrdZSetBuilder;
use crate::trace::BatchReader;
use crate::trace::Builder;

/// Commands we issue on [`Cursor`].
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
}

fn action<K: Arbitrary + Clone, V: Arbitrary + Clone>() -> impl Strategy<Value = CursorAction<K, V>>
{
    // Generate a possible action for applying on the vspace,
    // note we currently assume that a frame is either of base-page
    // or large-page size. Arbitrary frames are possible to map
    // but our (simple) vspace can only unmap one page-table
    // entry at a time.
    prop_oneof![
        Just(CursorAction::StepKey),
        //Just(CursorAction::StepVal),
        Just(CursorAction::RewindKeys),
        //Just(CursorAction::RewindVals),
        Just(CursorAction::Key),
        //Just(CursorAction::Val),
        //any::<V>().prop_map(CursorAction::SeekVal),
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
    // Verify that our implementation behaves according to the `ModelAddressSpace`.
    #[test]
    fn ord_zet_cursor_equivalence(mut ks in keys::<usize>(0..512), ops in actions::<usize, ()>()) {
        // Builder interface wants sorted keys
        ks.sort_unstable();

        // Instantiate a regular OrdZSet
        let mut model_builder = OrdZSetBuilder::new(());
        for key in ks.iter() {
            model_builder.push((*key, (), *key));
        }

        let model = model_builder.done();
        let mut model_cursor = model.cursor();

        // Instantiate a persistent OrdZSet
        let mut totest_builder = persistent::zset_batch::OrdZSetBuilder::new(());
        for key in ks.iter() {
            totest_builder.push((*key, (), *key));
        }
        let totest = totest_builder.done();
        let mut totest_cursor = totest.cursor();

        assert_eq!(totest.len(), model.len());
        for action in ops {
            match action {
                CursorAction::StepKey => {
                    model_cursor.step_key();
                    totest_cursor.step_key()
                },
                CursorAction::StepVal => {
                    model_cursor.step_val();
                    totest_cursor.step_val();
                },
                CursorAction::SeekKey(k) => {
                    model_cursor.seek_key(&k);
                    totest_cursor.seek_key(&k);
                },
                CursorAction::SeekVal(v) => {
                    model_cursor.seek_val(&v);
                    totest_cursor.seek_val(&v);
                },
                CursorAction::RewindKeys => {
                    model_cursor.rewind_keys();
                    totest_cursor.rewind_keys();
            },
                CursorAction::RewindVals => {
                    model_cursor.rewind_vals();
                    totest_cursor.rewind_vals();
                },
                CursorAction::Key => {
                    if model_cursor.key_valid() {
                        assert!(totest_cursor.key_valid());
                        assert_eq!(model_cursor.key(), totest_cursor.key());
                    }
                },
                CursorAction::Val => {
                    if model_cursor.val_valid() {
                        assert!(totest_cursor.val_valid());
                        assert_eq!(model_cursor.val(), totest_cursor.val());
                    }
                },
            }
        }
    }
}
