use std::{
    cmp::max,
    convert::TryFrom,
    fmt::{Debug, Display, Write},
    iter::Peekable,
    marker::PhantomData,
    ops::{Add, AddAssign, Neg},
    rc::Rc,
};

use timely::progress::Antichain;

use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, MonoidValue, NegByRef},
    lattice::Lattice,
    trace::{
        layers::{
            ordered_leaf::{OrderedLeaf, OrderedLeafBuilder, OrderedLeafCursor},
            Builder as TrieBuilder, Cursor as TrieCursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchReader, Builder, Cursor, Merger,
    },
    NumEntries, SharedRef,
};

use bincode::{decode_from_slice, Decode, Encode};
use deepsize::DeepSizeOf;
use rocksdb::{DBRawIterator, IteratorMode, Options, WriteBatch, DB};
use uuid::Uuid;

use super::{ReusableEncodeBuffer, BINCODE_CONFIG};

/// An immutable collection of `(key, weight)` pairs without timing information.
// TODO(persistence) probably want to preserve/implement these traits:
// #[derive(Debug, Clone, Eq, PartialEq)]
pub struct OrdZSet<K, R>
where
    K: Ord + Encode + Decode,
    R: Encode,
{
    /// Where all the dataz is.
    db: DB,
    /// The DB only "knows" approximate key count so we store the accurate count here
    keys: usize,
    /// The underlying file-path for mtbl::Reader
    path: String,
    pub lower: Antichain<()>,
    pub upper: Antichain<()>,
    _t: PhantomData<(K, R)>,
}

/*
impl<K, R> Display for OrdZSet<K, R>
where
    K: Ord + Clone + Display,
    R: Eq + HasZero + AddAssignByRef + Clone + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, R> From<OrderedLeaf<K, R>> for OrdZSet<K, R>
where
    K: Ord,
{
    fn from(layer: OrderedLeaf<K, R>) -> Self {
        Self {
            layer,
            lower: Antichain::from_elem(()),
            upper: Antichain::new(),
        }
    }
}

impl<K, R> From<OrderedLeaf<K, R>> for Rc<OrdZSet<K, R>>
where
    K: Ord,
{
    fn from(layer: OrderedLeaf<K, R>) -> Self {
        Rc::new(From::from(layer))
    }
}

impl<K, R> TryFrom<Rc<OrdZSet<K, R>>> for OrdZSet<K, R>
where
    K: Ord,
{
    type Error = Rc<OrdZSet<K, R>>;

    fn try_from(batch: Rc<OrdZSet<K, R>>) -> Result<Self, Self::Error> {
        Rc::try_unwrap(batch)
    }
}

impl<K, R> DeepSizeOf for OrdZSet<K, R>
where
    K: DeepSizeOf + Ord,
    R: DeepSizeOf,
{
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        self.layer.deep_size_of()
    }
}
 */
impl<K, R> NumEntries for OrdZSet<K, R>
where
    K: Ord + Clone + Encode + Decode,
    R: Eq + HasZero + AddAssignByRef + Clone + Encode + Decode,
{
    fn num_entries_shallow(&self) -> usize {
        self.keys
    }

    fn num_entries_deep(&self) -> usize {
        self.keys
    }

    const CONST_NUM_ENTRIES: Option<usize> = <OrderedLeaf<K, R>>::CONST_NUM_ENTRIES;
}

impl<K, R> HasZero for OrdZSet<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    fn zero() -> Self {
        Self::empty(())
    }

    fn is_zero(&self) -> bool {
        self.is_empty()
    }
}

/*
impl<K, R> SharedRef for OrdZSet<K, R>
where
    K: Ord + Clone,
    R: Clone,
{
    type Target = Self;

    fn try_into_owned(self) -> Result<Self::Target, Self> {
        Ok(self)
    }
}

impl<K, R> NegByRef for OrdZSet<K, R>
where
    K: Ord + Clone,
    R: MonoidValue + NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
            lower: self.lower.clone(),
            upper: self.upper.clone(),
        }
    }
}

impl<K, R> Neg for OrdZSet<K, R>
where
    K: Ord + Clone,
    R: MonoidValue + Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
            lower: self.lower,
            upper: self.upper,
        }
    }
}

// TODO: by-value merge
impl<K, R> Add<Self> for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        let lower = self.lower().meet(rhs.lower());
        let upper = self.upper().join(rhs.upper());

        Self {
            layer: self.layer.add(rhs.layer),
            lower,
            upper,
        }
    }
}

impl<K, R> AddAssign<Self> for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    fn add_assign(&mut self, rhs: Self) {
        self.lower = self.lower().meet(rhs.lower());
        self.upper = self.upper().join(rhs.upper());
        self.layer.add_assign(rhs.layer);
    }
}

impl<K, R> AddAssignByRef for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
        self.lower = self.lower().meet(rhs.lower());
        self.upper = self.upper().join(rhs.upper());
    }
}

impl<K, R> AddByRef for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            layer: self.layer.add_by_ref(&rhs.layer),
            lower: self.lower().meet(rhs.lower()),
            upper: self.upper().join(rhs.upper()),
        }
    }
}
 */

impl<K, R> BatchReader for OrdZSet<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    type Key = K;
    type Val = ();
    type Time = ();
    type R = R;
    type Cursor<'s> = OrdZSetCursor<'s, K, R>;

    fn cursor(&self) -> Self::Cursor<'_> {
        let mut db_iter = self.db.raw_iterator();
        db_iter.seek_to_first();

        let cur_key_weight = if db_iter.valid() {
            // TODO: code-repetition
            if let (Some(k), Some(v)) = (db_iter.key(), db_iter.value()) {
                let (key, _) = decode_from_slice::<'_, K, _>(&k, BINCODE_CONFIG)
                    .expect("Can't decode_from_slice");
                let (weight, _) =
                    decode_from_slice(&v, BINCODE_CONFIG).expect("Can't decode current value");
                Some((key, weight))
            } else {
                unreachable!("Why does db_iter.valid() not imply that key is Some(k)?")
            }
        } else {
            None
        };

        OrdZSetCursor {
            empty: (),
            valid: true,
            db_iter,
            cur_key_weight,
            cursor_upper_bound: None,
            tmp_key: ReusableEncodeBuffer(Vec::new()),
        }
    }
    fn len(&self) -> usize {
        self.keys
    }
    fn lower(&self) -> &Antichain<()> {
        &self.lower
    }
    fn upper(&self) -> &Antichain<()> {
        &self.upper
    }
}

impl<K, R> Batch for OrdZSet<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    type Batcher = MergeBatcher<K, (), (), R, Self>;
    type Builder = OrdZSetBuilder<K, R>;
    type Merger = OrdZSetMerger<K, R>;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        unimplemented!()
    }

    fn recede_to(&mut self, _frontier: &()) {}
}
/// State for an in-progress merge.
pub struct OrdZSetMerger<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    _t: PhantomData<(K, R)>,
}

impl<K, R> Merger<K, (), (), R, OrdZSet<K, R>> for OrdZSetMerger<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    fn new(batch1: &OrdZSet<K, R>, batch2: &OrdZSet<K, R>) -> Self {
        unimplemented!()
    }
    fn done(self) -> OrdZSet<K, R> {
        unimplemented!()
    }
    fn work(&mut self, source1: &OrdZSet<K, R>, source2: &OrdZSet<K, R>, fuel: &mut isize) {
        unimplemented!()
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdZSetStorage<K, V> {
    valid: bool,
    current_key: Option<K>,
    current_val: Option<V>,
}

impl<K, R> OrdZSetStorage<K, R>
where
    K: Ord + Clone + Encode + Decode,
    R: MonoidValue + Encode + Decode,
{
    fn advance_one(&self) {
        unimplemented!()

        /*let (k, v) = self.cursor.next().unwrap();

        let (key, len) = decode_from_slice(&k, BINCODE_CONFIG).expect("Can't deserialize");
        let (obj, len) = decode_from_slice(&k, BINCODE_CONFIG).expect("Can't deserialize");

        Box::leak(obj)*/
    }
}

/// A cursor for navigating a single layer.
pub struct OrdZSetCursor<'s, K, R> {
    empty: (),
    valid: bool,
    db_iter: DBRawIterator<'s>,
    cur_key_weight: Option<(K, R)>,
    cursor_upper_bound: Option<K>,
    tmp_key: ReusableEncodeBuffer,
}

impl<'s, K, R> OrdZSetCursor<'s, K, R>
where
    K: Ord + Clone + Encode + Decode,
    R: MonoidValue + Encode + Decode,
{
    /// Loads the current key and weight from RocksDB into the
    /// [`OrdZSetCursor`].
    ///
    /// # Panics
    /// - In case the `db_iter` is invalid.
    fn update_current_key_weight(&mut self) {
        assert!(self.db_iter.valid());
        if let (Some(k), Some(v)) = (self.db_iter.key(), self.db_iter.value()) {
            let (key, _) = decode_from_slice(&k, BINCODE_CONFIG).expect("Can't decode current key");
            let (weight, _) =
                decode_from_slice(&v, BINCODE_CONFIG).expect("Can't decode current value");
            self.cur_key_weight = Some((key, weight));
        } else {
            // This holds according to the RocksDB C++ API docs
            unreachable!("db_iter.valid() implies Some(key)")
        }
    }
}

impl<K, V> Debug for OrdZSetCursor<'_, K, V>
where
    K: Ord + Clone + Encode + Decode,
    V: MonoidValue + Encode + Decode,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OrdZSetCursor")
    }
}

impl<'s, K, R> Cursor<'s, K, (), (), R> for OrdZSetCursor<'s, K, R>
where
    K: Ord + Clone + Encode + Decode,
    R: MonoidValue + Encode + Decode,
{
    type Storage = OrdZSet<K, R>;

    fn key(&self) -> &K {
        &self.cur_key_weight.as_ref().unwrap().0
    }

    fn val(&self) -> &() {
        unsafe { ::std::mem::transmute(&self.empty) }
    }

    fn values<'a>(&mut self, vals: &mut Vec<(&'a (), R)>)
    where
        's: 'a,
    {
        // It's technically ok to call this on a batch with value type `()`, but
        // shouldn't happen in practice.
        unimplemented!();
    }

    fn map_times<L: FnMut(&(), &R)>(&mut self, mut logic: L) {
        if self.key_valid() {
            logic(&(), &self.cur_key_weight.as_ref().unwrap().1);
        }
    }

    fn weight(&mut self) -> R {
        self.cur_key_weight.as_ref().unwrap().1.clone()
    }

    fn key_valid(&self) -> bool {
        self.cur_key_weight.is_some()
    }

    fn val_valid(&self) -> bool {
        self.valid
    }

    fn step_key(&mut self) {
        self.valid = true;
        if self.db_iter.valid() {
            // We can only call next if we're in a valid state
            self.db_iter.next();

            if self.db_iter.valid() {
                self.update_current_key_weight();
            } else {
                // We need to "record" that we reached the end by storing the
                // last key in the `cursor_upper_bound`.
                //
                // Otherwise something like  keys = [1], operations = [StepKey,
                // SeekKey(0)] would result in key_valid() returning true (see
                // `seek_key`)
                self.cursor_upper_bound = self.cur_key_weight.take().map(|(k, _)| k);
            }
        }
    }

    fn seek_key(&mut self, key: &K) {
        self.valid = true;
        if let Some((cur_key, _weight)) = self.cur_key_weight.as_ref() {
            if cur_key >= key {
                // The rocksdb seek call will start from the beginning, whereas
                // [`OrderedLeafCursor`] will start to seek from the current
                // position. We can fix the discrepency here since we know the
                // batch is ordered: If we're seeking something that's behind
                // us, we can just skip the seek call.
                return;
            }
        }
        if let Some(cursor_upper_bound) = self.cursor_upper_bound.as_ref() {
            if cursor_upper_bound >= key {
                // We also need to keep track of the last seeked key in case we didn't
                // find one, e.g., consider keys = [4], operations = [SeekKey(5),
                // SeekKey(0)] should ensure to key_valid() == false after the second
                // seek completes:
                return;
            }
        }

        let encoded_key = self.tmp_key.encode(key).expect("Can't encode `key`");
        self.db_iter.seek(encoded_key);
        self.cursor_upper_bound = Some(key.clone());

        if self.db_iter.valid() {
            self.update_current_key_weight();
        } else {
            self.cur_key_weight = None;
        }
    }

    fn step_val(&mut self) {
        self.valid = false;
    }

    fn seek_val(&mut self, _val: &()) {}

    fn rewind_keys(&mut self) {
        self.cursor_upper_bound = None;
        self.db_iter.seek_to_first();
        if self.db_iter.valid() {
            self.update_current_key_weight();
        } else {
            self.cur_key_weight = None;
        }
        self.valid = true;
    }

    fn rewind_vals(&mut self) {
        self.valid = true;
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct OrdZSetBuilder<K, R>
where
    K: Ord,
    R: MonoidValue,
{
    db: DB,
    path: String,
    /// The DB only "knows" approximate key count so we store the accurate count here.
    batch: WriteBatch,
    tmp_key: ReusableEncodeBuffer,
    tmp_val: ReusableEncodeBuffer,
    _t: PhantomData<(K, R)>,
}

impl<K, R> Builder<K, (), (), R, OrdZSet<K, R>> for OrdZSetBuilder<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    fn new(_time: ()) -> Self {
        let uuid = Uuid::new_v4();
        let tbl_path = format!("/tmp/{}.db", uuid.to_string());
        let db = DB::open_default(tbl_path.clone()).unwrap();

        OrdZSetBuilder {
            db,
            path: tbl_path,
            batch: WriteBatch::default(),
            tmp_key: ReusableEncodeBuffer(Vec::new()),
            tmp_val: ReusableEncodeBuffer(Vec::new()),
            _t: PhantomData,
        }
    }

    fn with_capacity(_time: (), _cap: usize) -> Self {
        // We would need a `with_capacity` method on [`rocksdb::WriteBatch`] for
        // this to be useful
        Self::new(_time)
    }

    #[inline]
    fn push(&mut self, (key, (), diff): (K, (), R)) {
        let encoded_key = self.tmp_key.encode(&key).expect("Can't encode `key`");
        let encoded_val = self.tmp_val.encode(&diff).expect("Can't encode `diff`");
        self.batch.put(encoded_key, encoded_val);
    }

    #[inline(never)]
    fn done(self) -> OrdZSet<K, R> {
        let keys = self.batch.len();
        self.db
            .write(self.batch)
            .expect("Could not write batch to db");

        OrdZSet {
            db: self.db,
            path: self.path,
            keys,
            lower: Antichain::from_elem(()),
            upper: Antichain::new(),
            _t: PhantomData,
        }
    }
}