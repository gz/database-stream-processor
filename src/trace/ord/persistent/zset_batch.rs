use std::{
    cmp::max,
    convert::TryFrom,
    fmt::{Debug, Display},
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

use bincode::{decode_from_slice, error::EncodeError, Decode, Encode};
use deepsize::DeepSizeOf;
use mtbl::{Sorter, Write};
use uuid::Uuid;

static BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

/// An immutable collection of `(key, weight)` pairs without timing information.
// TODO(persistence) probably want to preserve/implement these traits:
// #[derive(Debug, Clone, Eq, PartialEq)]
pub struct OrdZSet<K, R>
where
    K: Ord + Encode + Decode,
    R: Encode,
{
    /// The underlying file-path for mtbl::Reader
    path: String,
    /// Where all the dataz is.
    reader: mtbl::Reader,

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
        self.reader.count_entries().try_into().unwrap()
    }

    fn num_entries_deep(&self) -> usize {
        self.reader.count_entries().try_into().unwrap()
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
    type Cursor = OrdZSetCursor<K, R>;

    fn cursor(&self) -> Self::Cursor {
        OrdZSetCursor {
            empty: (),
            valid: true,
            cursor: OrdZSetIter::new(&self.reader).peekable(),
            _t: PhantomData,
        }
    }
    fn len(&self) -> usize {
        self.reader.count_entries().try_into().unwrap()
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

/// An iterator that steps through a section of an MTBL. This is a low-level
/// struct that interacts with the mtbl library directly.
pub struct OrdZSetIter {
    mtbl_iter: *mut mtbl_sys::mtbl_iter,
}

impl Debug for OrdZSetIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OrdZSetIter")
    }
}

/// We implement our own iterator (as opposed to using mtbl::Iter) because mtbl
/// has a lifetime on Iter whereas we get the source storage as part of the
/// cursor API, so we don't need the lifetime.
///
/// Well that's not quite true because one could easily supply a wrong storage
/// not matching the iterator if the program has a bug, but then I think this
/// API seems flawed in the first place as the original OrdZSet also doesn't
/// protect against this :(
impl OrdZSetIter {
    /// Create an iterator for an mtbl_source.
    pub fn new(mtbl: &mtbl::Reader) -> OrdZSetIter {
        use mtbl::Read;

        OrdZSetIter {
            mtbl_iter: unsafe { mtbl_sys::mtbl_source_iter(*mtbl.raw_mtbl_source()) },
        }
    }
}

impl<'a> Iterator for OrdZSetIter {
    /// A key, value pair.
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            use core::ptr;
            use core::slice;
            use libc::size_t;

            let mut keyptr: *const u8 = ptr::null();
            let mut keylen: size_t = 0;
            let mut valptr: *const u8 = ptr::null();
            let mut vallen: size_t = 0;
            let res = mtbl_sys::mtbl_iter_next(
                self.mtbl_iter,
                &mut keyptr,
                &mut keylen,
                &mut valptr,
                &mut vallen,
            );
            match res {
                mtbl_sys::MtblRes::mtbl_res_success => Some((
                    slice::from_raw_parts(keyptr, keylen).to_vec(),
                    slice::from_raw_parts(valptr, vallen).to_vec(),
                )),
                mtbl_sys::MtblRes::mtbl_res_failure => None,
            }
        }
    }
}

impl<'a> Drop for OrdZSetIter {
    fn drop(&mut self) {
        unsafe {
            mtbl_sys::mtbl_iter_destroy(&mut self.mtbl_iter);
        }
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
        let (k, v) = self.cursor.next().unwrap();

        let (key, len) = decode_from_slice(&k, BINCODE_CONFIG).expect("Can't deserialize");
        let (obj, len) = decode_from_slice(&k, BINCODE_CONFIG).expect("Can't deserialize");

        Box::leak(obj)
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdZSetCursor<K, V> {
    valid: bool,
    empty: (),
    cursor: Peekable<OrdZSetIter>,
    _t: PhantomData<(K, V)>,
}

impl<K, R> Cursor<K, (), (), R> for OrdZSetCursor<K, R>
where
    K: Ord + Clone + Encode + Decode,
    R: MonoidValue + Encode + Decode,
{
    type Storage = OrdZSetStorage<K, R>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K {
        let (obj, len) = decode_from_slice(&self.cursor.next().unwrap().0, BINCODE_CONFIG)
            .expect("Can't deserialize");
        // FIXME: this is a hack to get around the fact that deserialize the
        // object and storage doesn't actually hold the content
        Box::leak(obj)
    }

    fn val<'a>(&self, _storage: &'a Self::Storage) -> &'a () {
        unsafe { ::std::mem::transmute(&self.empty) }
    }

    fn map_times<L: FnMut(&(), &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        unimplemented!()
    }

    fn weight(&mut self, storage: &Self::Storage) -> R {
        unimplemented!()
    }

    fn key_valid(&self, storage: &Self::Storage) -> bool {
        self.cursor.peek().is_some()
    }

    fn val_valid(&self, _storage: &Self::Storage) -> bool {
        self.cursor.peek().is_some()
    }

    fn step_key(&mut self, storage: &Self::Storage) {
        self.cursor.next();
    }

    fn seek_key(&mut self, storage: &Self::Storage, key: &K) {
        unimplemented!()
    }

    fn step_val(&mut self, _storage: &Self::Storage) {
        unimplemented!()
    }

    fn seek_val(&mut self, _storage: &Self::Storage, _val: &()) {}

    fn rewind_keys(&mut self, storage: &Self::Storage) {
        unimplemented!()
    }

    fn rewind_vals(&mut self, _storage: &Self::Storage) {
        unimplemented!()
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct OrdZSetBuilder<K, R>
where
    K: Ord,
    R: MonoidValue,
{
    builder: mtbl::Writer,
    path: String,
    key_tmp: ReusableEncodeBuffer,
    val_tmp: ReusableEncodeBuffer,
    _t: PhantomData<(K, R)>,
}

struct ReusableEncodeBuffer(Vec<u8>);

impl bincode::enc::write::Writer for &mut ReusableEncodeBuffer {
    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.0.extend(bytes);
        Ok(())
    }
}

impl<K, R> Builder<K, (), (), R, OrdZSet<K, R>> for OrdZSetBuilder<K, R>
where
    K: Ord + Clone + Encode + Decode + 'static,
    R: MonoidValue + Encode + Decode,
{
    fn new(_time: ()) -> Self {
        let uuid = Uuid::new_v4();
        let tbl_path = format!("/tmp/{}.mtbl", uuid.to_string());

        let writer = mtbl::WriterOptions::new()
            .compression(mtbl::CompressionType::MTBL_COMPRESSION_NONE)
            .create_from_path(tbl_path.as_str())
            .expect("can't create writer");

        OrdZSetBuilder {
            builder: writer,
            path: tbl_path,
            key_tmp: ReusableEncodeBuffer(Vec::new()),
            val_tmp: ReusableEncodeBuffer(Vec::new()),
            _t: PhantomData,
        }
    }

    fn with_capacity(_time: (), cap: usize) -> Self {
        // TODO(persistence): maybe pre-allocate file size for `with_capacity`
        // calls, does it matter?
        Self::new(_time)
    }

    #[inline]
    fn push(&mut self, (key, (), diff): (K, (), R)) {
        self.key_tmp.0.clear();
        self.val_tmp.0.clear();
        bincode::encode_into_writer(key, &mut self.key_tmp, BINCODE_CONFIG)
            .expect("Can't serialize data");
        bincode::encode_into_writer(diff, &mut self.val_tmp, BINCODE_CONFIG)
            .expect("Can't serialize data");

        self.builder
            .add(&self.key_tmp.0, &self.val_tmp.0)
            .expect("Couldn't add key/val pair to file");
    }

    #[inline(never)]
    fn done(self) -> OrdZSet<K, R> {
        // We drop this to force writing of the mtbl file.
        drop(self.builder);

        let reader = mtbl::ReaderOptions::new()
            .verify_checksums(false)
            .madvise_random(false)
            .open_from_path(&self.path)
            .expect("Can't open the written mtbl file");

        OrdZSet {
            path: self.path,
            reader,
            lower: Antichain::from_elem(()),
            upper: Antichain::new(),
            _t: PhantomData,
        }
    }
}
