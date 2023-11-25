use std::cmp::min;
use std::ops::{Deref, DerefMut, Index, IndexMut, Range};

pub(super) const BUFFER_SIZE: usize = 8 * 64 * 1024;

pub(super) struct ChunkerBuf {
    pub pos: usize,
    pub clen: usize, // current length
    pub chunk_len: usize,
    buf: Vec<u8>, // chunker buffer, fixed size: WTR_BUF_LEN
}

impl ChunkerBuf {
    pub fn new() -> Self {
        let mut buf = vec![0u8; BUFFER_SIZE];
        buf.shrink_to_fit();

        Self {
            pos: 0,
            clen: 0,
            buf,
            chunk_len: 0,
        }
    }

    /// Moves unchecked data in the buffer to the front, resetting position and current length.
    ///
    /// Should only be called after a successful write to destination.
    pub fn reset_position(&mut self) {
        let left_len = self.clen - self.pos;
        let copy_range = self.pos..self.clen;

        self.buf.copy_within(copy_range, 0);
        self.clen = left_len;
        self.pos = 0;
    }

    /// Checks if the buffer has bytes that must be checked.
    pub fn has_something(&self) -> bool {
        self.pos < self.clen
    }

    /// Appends data from the `buf` to the inner buffer. If there is not enough place to fit the entire `buf`, copies only the amount that will fit.
    ///
    /// Returns how many bytes were copied from `buf` to the inner buffer.
    pub fn append(&mut self, buf: &[u8]) -> usize {
        let in_len = min(BUFFER_SIZE - self.clen, buf.len());
        assert!(in_len > 0);

        let copy_range = self.clen..self.clen + in_len;
        self.buf[copy_range].copy_from_slice(&buf[..in_len]);
        self.clen += in_len;

        in_len
    }

    /// Returns the maximum possible size of the chunk that can be written at the moment, considering inner buffer's position and length, and the current chunk length, if any.
    ///
    /// Does not take any maximum chunk size information into account.
    pub fn possible_size(&self) -> usize {
        self.clen - self.pos + self.chunk_len
    }
}

impl Index<Range<usize>> for ChunkerBuf {
    type Output = [u8];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        &self.buf[index]
    }
}

impl Index<usize> for ChunkerBuf {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        &self.buf[index]
    }
}

impl Deref for ChunkerBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl IndexMut<usize> for ChunkerBuf {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.buf[index]
    }
}

impl IndexMut<Range<usize>> for ChunkerBuf {
    fn index_mut(&mut self, index: Range<usize>) -> &mut Self::Output {
        &mut self.buf[index]
    }
}

impl DerefMut for ChunkerBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}
