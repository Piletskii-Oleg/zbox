use crate::content::chunker::buffer::ChunkerBuf;
use crate::content::chunker::Chunking;
use fastcdc::v2020::{FastCDC, Normalization};
use std::fmt::{self, Debug};
use std::ops::Range;

const MIN_SIZE: usize = 2 * 1024; // minimal chunk size, 2k
const AVG_SIZE: usize = 2 * 1024; // average chunk size, 2k
const MAX_SIZE: usize = 32 * 1024; // maximum chunk size, 32k

const NORMALIZATION_LEVEL: Normalization = Normalization::Level2;

pub struct FastChunker;

impl FastChunker {
    pub fn new() -> Self {
        FastChunker
    }
}

impl Chunking for FastChunker {
    fn next_write_range(
        &mut self,
        buf: &mut ChunkerBuf,
    ) -> Option<Range<usize>> {
        let (_, cut_point) = FastCDC::with_level(
            buf,
            MIN_SIZE as u32,
            AVG_SIZE as u32,
            MAX_SIZE as u32,
            NORMALIZATION_LEVEL,
        )
        .cut(buf.pos, buf.clen - buf.pos);

        buf.chunk_len = cut_point - buf.pos;
        let write_range = buf.pos..buf.pos + buf.chunk_len;

        buf.pos = cut_point;

        Some(write_range)
    }
}

impl Debug for FastChunker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FastChunker")
    }
}
