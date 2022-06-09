use std::alloc::Layout;
use std::fs::File;
use std::fs::OpenOptions;
use std::marker::PhantomData;
use std::mem::align_of;
use std::mem::size_of;
use std::path::Path;
use std::ptr;

use memmap2::MmapMut;
use static_assertions;

/// Version of the file-format which is stored on disk.
const FILE_FORMAT_VERSION: u32 = 1;

/// Identifies the file as a [`PersistentVec`].
const MAGIC_FILE_HEADER: [u8; 8] = [
    'd' as u8, 'b' as u8, 's' as u8, 'p' as u8, 'r' as u8, 'o' as u8, 'c' as u8, '\0' as u8,
];

/// Errors when dealing with [`PersistentVec`].
#[derive(Debug)]
pub enum Error {
    /// The header of the file is invalid (magic byte mismatch).
    InvalidFileHeader,
    /// The file header version does not match program's supported version.
    InvalidFileFormatVersion,
    /// The path we're trying to open is not a valid file.
    NotAFile,
    /// The size of the file is too small to fit the FileHeader (corrupted?)
    FileTooSmall,
    /// File size did not add up with information found in the header.
    HeaderInfoToFileSizeMismatch,
    /// File not even large enough to read the [`FileHeader`].
    InvalidFileSize,
    /// We encountered an OS I/O error while doing file operations.
    IO(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IO(e)
    }
}

/// Header that is pre-pended to [`PersistentVec`] data in files.
#[derive(Debug)]
#[repr(C)]
struct FileHeader {
    magic_bytes: [u8; 8],
    file_format_version: u32,
    capacity: usize,
    len: usize,
    padding_after_header: usize,
}
// Since `FileHeader` is at the beginning of the file-region (which is 4K aligned)
// we make sure it needs less than 4K alignment for a safe cast:
static_assertions::const_assert!(align_of::<FileHeader>() <= 4096);
static_assertions::assert_eq_size!(usize, u64);
// If you need to change any of these asserts, you might want to increase `FILE_FORMAT_VERSION`:
//static_assertions::assert_eq_size!(FileHeader<()>, [u8; 20]);
//static_assertions::const_assert_eq!(offset_of!(FileHeader<()>, magic_bytes), 0);
//static_assertions::const_assert_eq!(offset_of!(FileHeader<()>, endianness), 8);
//static_assertions::const_assert_eq!(offset_of!(FileHeader<()>, padding_after_header), 12);
//static_assertions::const_assert_eq!(offset_of!(FileHeader<()>, file_format_version), 16);
//static_assertions::const_assert_eq!(offset_of!(FileHeader<()>, default_data), 20);

pub struct PersistentVec<T> {
    file: File,
    mm: MmapMut,
    _marker: PhantomData<T>,
}

impl<T> PersistentVec<T> {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let meta_data = file.metadata()?;
        if !meta_data.is_file() {
            return Err(Error::NotAFile);
        }
        let file_len: usize = meta_data.len().try_into().unwrap();
        if file_len < std::mem::size_of::<FileHeader>() {
            return Err(Error::InvalidFileSize);
        }

        let mut mm = unsafe { MmapMut::map_mut(&file)? };
        let header = unsafe {
            // Safety:
            // - is aligned:
            assert!(mm.as_ptr() as usize % align_of::<FileHeader>() == 0);
            // - Memory area fits FileHeader: Yes, checked above
            // - Is initialized: Yes, FileHeader is plain-old-data
            &mut *(mm.as_mut_ptr() as *mut FileHeader)
        };
        log::info!("Header: {:#?}", header);
        if header.magic_bytes != MAGIC_FILE_HEADER {
            return Err(Error::InvalidFileHeader);
        }
        if header.file_format_version != FILE_FORMAT_VERSION {
            return Err(Error::InvalidFileFormatVersion);
        }
        if size_of::<FileHeader>() + header.padding_after_header + header.capacity * size_of::<T>()
            != file_len
        {
            return Err(Error::HeaderInfoToFileSizeMismatch);
        }
        assert!(
            header.capacity > header.len,
            "capacity should be larger than len"
        );

        Ok(Self {
            file,
            mm,
            _marker: PhantomData,
        })
    }

    pub fn try_with_path_capacity<P: AsRef<Path>>(path: P, cap: usize) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let fh = FileHeader {
            magic_bytes: MAGIC_FILE_HEADER,
            file_format_version: FILE_FORMAT_VERSION,
            capacity: cap,
            len: 0,
            padding_after_header: Layout::new::<FileHeader>()
                .padding_needed_for(Layout::new::<T>().align()),
        };

        let file_size = size_of::<FileHeader>() + fh.padding_after_header + cap * size_of::<T>();
        log::info!(
            "file_size is {} size_of::<FileHeader>()={} cap={} size_of::<T>()={} fh={:#?}",
            file_size,
            size_of::<FileHeader>(),
            cap,
            size_of::<T>(),
            fh,
        );
        file.set_len(file_size.try_into().unwrap())?;
        let mut mm = unsafe { MmapMut::map_mut(&file)? };

        let mut pvec = Self {
            file,
            mm,
            _marker: PhantomData,
        };
        *pvec.header_mut() = fh;
        Ok(pvec)
    }

    fn header(&self) -> &FileHeader {
        unsafe { &*(self.mm.as_ptr() as *mut FileHeader) }
    }

    fn header_mut(&mut self) -> &mut FileHeader {
        unsafe { &mut *(self.mm.as_mut_ptr() as *mut FileHeader) }
    }

    /// Returns start of data-region (relative to file position 0) in bytes.
    fn data_offset_in_file(&self) -> usize {
        size_of::<FileHeader>() + self.header().padding_after_header
    }

    /// Returns the number of elements in the vector, also referred to as its ‘length’.
    fn len(&self) -> usize {
        self.header().len
    }

    /// Returns the number of elements the vector can hold.
    fn capacity(&self) -> usize {
        self.header().capacity
    }

    fn as_ptr(&self) -> *const T {
        let region_data_offset = self.data_offset_in_file();
        unsafe {
            let start_ptr = self.mm.as_ptr().add(region_data_offset) as *const T;
            start_ptr
        }
    }

    fn as_mut_ptr(&mut self) -> *mut T {
        let region_data_offset = self.data_offset_in_file();
        unsafe {
            let start_ptr = self.mm.as_ptr().add(region_data_offset) as *mut T;
            start_ptr
        }
    }

    fn as_slice(&self) -> &[T] {
        let start = self.data_offset_in_file();
        let len_in_bytes = self.len() * size_of::<T>();

        unsafe {
            // Safety:
            // - ptr is in bounds:
            assert!(self.mm.as_ptr() as usize + start < self.mm.len());
            let ptr: *const T = self.mm.as_ptr().add(start) as *const T;
            // - `ptr` is properly aligned:
            assert!(ptr as usize % align_of::<T>() == 0);
            // - memory region is valid / large enough:
            assert!(start + len_in_bytes <= self.mm.len());
            // - T's are properly initialized: Yes, contract `len` field in FileHeader
            std::slice::from_raw_parts(ptr, self.header().len)
        }
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        let start = self.data_offset_in_file();
        let len_in_bytes = self.len() * size_of::<T>();

        unsafe {
            // Safety:
            // - ptr is in bounds:
            assert!(self.mm.as_ptr() as usize + start < self.mm.len());
            let ptr: *mut T = self.mm.as_mut_ptr().add(start) as *mut T;
            // - `ptr` is properly aligned:
            assert!(ptr as usize % align_of::<T>() == 0);
            // - memory region is valid / large enough:
            assert!(start + len_in_bytes <= self.mm.len());
            // - T's are properly initialized: Yes, contract `len` field in FileHeader
            std::slice::from_raw_parts_mut(ptr, self.header().len)
        }
    }

    pub fn push(&mut self, value: T) {
        if self.capacity() == self.len() {
            panic!("Can't add more than capacity");
        }

        unsafe {
            let end = self.as_mut_ptr().add(self.len());
            ptr::write(end, value);
            self.header_mut().len += 1;
        }
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.len() == 0 {
            None
        } else {
            unsafe {
                self.header_mut().len -= 1;
                Some(ptr::read(self.as_ptr().add(self.len())))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Can create and after restore a vector, preserving header data.
    #[test]
    fn can_load_store() {
        let _r = env_logger::try_init();

        let path = "/tmp/test_pvec_can_load_store.dbr";
        let p = PersistentVec::<usize>::try_with_path_capacity(path, 10)
            .expect("create persistent vec");
        drop(p);

        let p = PersistentVec::<usize>::load_from_file(path).expect("loaded persistent vec");
        assert_eq!(p.capacity(), 10);
        assert_eq!(p.len(), 0);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn can_push_pop() {
        let _r = env_logger::try_init();

        let path = "/tmp/test_pvec_can_push_pop.dbr";
        let mut p = PersistentVec::<usize>::try_with_path_capacity(path, 10)
            .expect("create persistent vec");
        p.push(7);
        p.push(2);
        p.push(1);
        assert_eq!(p.len(), 3);
        assert_eq!(p.pop(), Some(1));
        assert_eq!(p.pop(), Some(2));
        assert_eq!(p.pop(), Some(7));
        assert_eq!(p.pop(), None);
        assert_eq!(p.len(), 0);
        p.push(7);
        p.push(2);
        p.push(1);
        assert_eq!(p.len(), 3);
        drop(p);

        let mut p = PersistentVec::<usize>::load_from_file(path).expect("loaded persistent vec");
        assert_eq!(p.len(), 3);
        assert_eq!(p.pop(), Some(1));
        assert_eq!(p.pop(), Some(2));
        assert_eq!(p.pop(), Some(7));
        assert_eq!(p.pop(), None);
        assert_eq!(p.len(), 0);
        p.push(7);
        p.push(2);
        p.push(1);
        assert_eq!(p.len(), 3);

        let _ = std::fs::remove_file(path);
    }
}
