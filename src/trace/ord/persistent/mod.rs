use bincode::config::{BigEndian, Fixint};
use bincode::enc::write::Writer;
use bincode::error::EncodeError;
use bincode::Encode;

/// Configuration we use for encodings/decodings to/from RocksDB data.
static BINCODE_CONFIG: bincode::config::Configuration<BigEndian, Fixint> =
    bincode::config::standard()
        .with_fixed_int_encoding()
        .with_big_endian();

/// A buffer that holds an encoded value.
///
/// Useful to keep around in structs where serialization happens repeatedly as
/// it can avoid repeated [`Vec`] allocations.
#[derive(Default)]
struct ReusableEncodeBuffer(Vec<u8>);

impl ReusableEncodeBuffer {
    /// Encodes `val` into the buffer owned by this struct.
    ///
    /// # Returns
    /// - An error if encoding failed.
    /// - A reference to the buffer where `val` was encoded into. Makes sure the
    ///   buffer won't change until the reference out-of-scope again.
    fn encode<T: Encode>(&mut self, val: &T) -> Result<&[u8], EncodeError> {
        self.0.clear();
        bincode::encode_into_writer(val, &mut *self, BINCODE_CONFIG)?;
        Ok(&self.0)
    }
}

impl Writer for &mut ReusableEncodeBuffer {
    /// Allows bincode to write into the buffer.
    ///
    /// # Note
    /// Client needs to ensure that the buffer is cleared in advance if we store
    /// something new. When possible use the [`encode`] method instead which
    /// takes care of that.
    fn write(&mut self, bytes: &[u8]) -> Result<(), EncodeError> {
        self.0.extend(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
mod zset_batch;
