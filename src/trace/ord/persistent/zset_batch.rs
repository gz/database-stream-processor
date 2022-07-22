use std::{
    cmp::Ordering,
    fmt::{Debug, Display, Formatter},
    marker::PhantomData,
    sync::Arc,
};

use once_cell::sync::Lazy;
use timely::progress::Antichain;

use crate::{
    algebra::{AddAssignByRef, HasZero, MonoidValue},
    trace::{ord::merge_batcher::MergeBatcher, Batch, BatchReader, Builder, Cursor, Merger},
    NumEntries,
};

use bincode::{decode_from_slice, Decode, Encode};
use deepsize::DeepSizeOf;
use rocksdb::{
    BoundColumnFamily, Cache, DBCompressionType, DBRawIterator, Options, WriteBatch, DB,
};
use uuid::Uuid;

use super::{ReusableEncodeBuffer, BINCODE_CONFIG};

const DB_DRAM_CACHE_SIZE: usize = 2 * 1024 * 1024 * 1024;

static DB_PATH: Lazy<String> = Lazy::new(|| {
    let uuid = Uuid::new_v4();
    format!("/tmp/{}.db", uuid.to_string())
});

static DB_OPTS: Lazy<Options> = Lazy::new(|| {
    let cache = Cache::new_lru_cache(DB_DRAM_CACHE_SIZE).expect("Can't create cache for DB");
    let mut global_opts = Options::default();
    global_opts.create_if_missing(true);
    global_opts.set_compression_type(DBCompressionType::None);
    global_opts.set_row_cache(&cache);
    global_opts.set_max_open_files(9000);
    global_opts
});

static LEVEL_DB: Lazy<DB> = Lazy::new(|| {
    // Open the database, or create it if it doesn't exist.
    DB::open(&*DB_OPTS, DB_PATH.clone()).unwrap()
});

/// An immutable collection of `(key, weight)` pairs without timing information.
// TODO(persistence) probably want to preserve/implement these traits:
// #[derive(Debug, Clone, Eq, PartialEq)]
pub struct OrdZSet<K, R>
where
    K: Ord + Encode + Decode,
    R: Encode,
{
    /// Where all the dataz is.
    cf: Arc<BoundColumnFamily<'static>>,
    _cf_name: String,
    _cf_options: Options,
    /// The DB only "knows" approximate key count so we store the accurate count here
    keys: usize,
    pub lower: Antichain<()>,
    pub upper: Antichain<()>,
    _t: PhantomData<(K, R)>,
}

impl<K, R> Display for OrdZSet<K, R>
where
    K: Ord + Clone + Display + Encode + Decode + 'static,
    R: Eq + HasZero + AddAssignByRef + Clone + Display + MonoidValue + Encode + Decode,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut cursor: OrdZSetCursor<K, R> = self.cursor();
        writeln!(f, "layer:")?;
        while cursor.key_valid() {
            while cursor.val_valid() {
                let weight = cursor.weight();

                writeln!(
                    f,
                    "{}",
                    textwrap::indent(format!("{} -> {}", cursor.key(), weight).as_str(), "    ")
                )?;
                cursor.step_val();
            }
            cursor.step_key();
        }
        writeln!(f, "")?;
        Ok(())
    }
}

/*
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
*/

impl<K, R> DeepSizeOf for OrdZSet<K, R>
where
    K: DeepSizeOf + Ord + Encode + Decode,
    R: DeepSizeOf + Encode + Decode,
{
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        let mem_usage = rocksdb::perf::get_memory_usage_stats(Some(&[&*LEVEL_DB]), None)
            .expect("failed to get memory usage");

        // For `MemUsageStats`: it looks like `mem_table_total` includes
        // `mem_table_unflushed`, not sure about `mem_table_readers_total` but I
        // assume it's the same, so we only care about `cache_total` and
        // `mem_table_total`
        (mem_usage.cache_total + mem_usage.mem_table_total)
            .try_into()
            .unwrap()
    }
}

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

    const CONST_NUM_ENTRIES: Option<usize> = None;
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
        let mut db_iter = LEVEL_DB.raw_iterator_cf(&self.cf);
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
    result: OrdZSetBuilder<K, R>,
}

impl<K, R> Merger<K, (), (), R, OrdZSet<K, R>> for OrdZSetMerger<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    fn new(batch1: &OrdZSet<K, R>, batch2: &OrdZSet<K, R>) -> Self {
        OrdZSetMerger {
            result: OrdZSetBuilder::new(()),
        }
    }
    fn done(self) -> OrdZSet<K, R> {
        self.result.done()
    }
    fn work(&mut self, source1: &OrdZSet<K, R>, source2: &OrdZSet<K, R>, fuel: &mut isize) {
        // https://docs.rs/rocksdb/latest/rocksdb/struct.DBWithThreadMode.html#method.ingest_external_file_cf_opts
        // https://docs.rs/rocksdb/latest/rocksdb/struct.DBWithThreadMode.html#method.live_files
        // https://docs.rs/rocksdb/latest/rocksdb/struct.LiveFile.html
        // https://docs.rs/rocksdb/latest/rocksdb/struct.IngestExternalFileOptions.html
        unimplemented!()
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
    cf: Arc<BoundColumnFamily<'static>>,
    cf_name: String,
    cf_options: Options,
    /// The DB only "knows" approximate key count so we store the accurate count here.
    batch: WriteBatch,
    tmp_key: ReusableEncodeBuffer,
    tmp_val: ReusableEncodeBuffer,
    _t: PhantomData<(K, R)>,
}

/// Wrapper for custom key comparison. It works by deserializing the keys and
/// then comparing it as opposed to the byte-wise comparison which is the
/// default in RocksDB.
fn comparator<K: Decode + Ord>(a: &[u8], b: &[u8]) -> Ordering {
    let (key_a, _) =
        decode_from_slice::<'_, K, _>(a, BINCODE_CONFIG).expect("Can't decode_from_slice");
    let (key_b, _) =
        decode_from_slice::<'_, K, _>(b, BINCODE_CONFIG).expect("Can't decode_from_slice");
    key_a.cmp(&key_b)
}

impl<K, R> Builder<K, (), (), R, OrdZSet<K, R>> for OrdZSetBuilder<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    fn new(_time: ()) -> Self {
        // Create a new column family for the OrdZSet.
        let cf_name = Uuid::new_v4().to_string();
        let mut cf_options = Options::default();
        cf_options.set_comparator("OrdZSet comparator", comparator::<K>);
        LEVEL_DB
            .create_cf(cf_name.as_str(), &cf_options)
            .expect("Can't create column family?");
        let cf = LEVEL_DB
            .cf_handle(cf_name.as_str())
            .expect("Can't find just created column family?");

        OrdZSetBuilder {
            cf,
            cf_name,
            cf_options,
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

        self.batch.put_cf(&self.cf, encoded_key, encoded_val);
    }

    #[inline(never)]
    fn done(self) -> OrdZSet<K, R> {
        let keys = self.batch.len();
        LEVEL_DB
            .write(self.batch)
            .expect("Could not write batch to db");

        OrdZSet {
            cf: self.cf,
            _cf_name: self.cf_name,
            _cf_options: self.cf_options,
            keys,
            lower: Antichain::from_elem(()),
            upper: Antichain::new(),
            _t: PhantomData,
        }
    }
}
