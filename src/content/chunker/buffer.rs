use std::cmp::min;
use std::ops::{Deref, DerefMut, Index, IndexMut, Range};

pub(super) const BUFFER_SIZE: usize = 8 * 64 * 1024;

pub(super) struct ChunkerBuf {
    pub(super) pos: usize,
    pub(super) clen: usize,
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
        }
    }

    pub fn reset_position(&mut self) {
        let left_len = self.clen - self.pos;
        let copy_range = self.pos..self.clen;

        self.buf.copy_within(copy_range, 0);
        self.clen = left_len;
        self.pos = 0;
    }

    pub fn copy_in(&mut self, buf: &[u8], in_len: usize) {
        let copy_range = self.clen..self.clen + in_len;
        self.buf[copy_range].copy_from_slice(&buf[..in_len]);
        self.clen += in_len;
    }

    pub fn has_something(&self) -> bool {
        self.pos < self.clen
    }

    pub fn append(&mut self, buf: &[u8]) -> usize {
        let in_len = min(BUFFER_SIZE - self.clen, buf.len());
        assert!(in_len > 0);

        let copy_range = self.clen..self.clen + in_len;
        self.buf[copy_range].copy_from_slice(&buf[..in_len]);
        self.clen += in_len;

        in_len
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
