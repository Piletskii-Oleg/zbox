use crate::content::chunker::buffer::ChunkerBuf;
use fastcdc::v2020::{FastCDC, Normalization};
use std::cmp::min;
use std::fmt::{self, Debug};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};

const MIN_SIZE: usize = 2 * 1024; // minimal chunk size, 2k
const AVG_SIZE: usize = 2 * 1024; // average chunk size, 2k
const MAX_SIZE: usize = 32 * 1024; // maximum chunk size, 32k

const NORMALIZATION_LEVEL: Normalization = Normalization::Level2;

// writer buffer length
const BUFFER_SIZE: usize = 8 * MAX_SIZE;

pub(super) struct FastChunker<W: Write + Seek> {
    dst: W,
    chunk_len: usize,
    roll_hash: u64,
    buf: ChunkerBuf,
}

impl<'a, W: Write + Seek> FastChunker<W> {
    pub(super) fn new(dst: W) -> Self {
        let mut buf = vec![0u8; BUFFER_SIZE];
        buf.shrink_to_fit();

        FastChunker {
            dst,
            chunk_len: MIN_SIZE,
            roll_hash: 0,
            buf: ChunkerBuf::new(MIN_SIZE),
        }
    }

    pub(super) fn into_inner(mut self) -> IoResult<W> {
        self.flush()?;
        Ok(self.dst)
    }
}

impl<W: Write + Seek> Write for FastChunker<W> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // copy source data into chunker buffer
        let in_len = min(BUFFER_SIZE - self.buf.clen, buf.len());
        assert!(in_len > 0);
        self.buf.copy_in(buf, in_len);

        while self.buf.has_something() {
            self.buf.pos -= MIN_SIZE;

            let (hash, cut_point) = FastCDC::with_level(
                &*self.buf, // is &* necessary?
                MIN_SIZE as u32,
                AVG_SIZE as u32,
                MAX_SIZE as u32,
                NORMALIZATION_LEVEL,
            )
            .cut(self.buf.pos, self.buf.clen - self.buf.pos);

            self.roll_hash = hash;
            self.chunk_len = cut_point - self.buf.pos;
            self.buf.pos = cut_point;

            // write the chunk to destination writer,
            // ensure it is consumed in whole
            let write_range = self.buf.pos - self.chunk_len..self.buf.pos;
            let written = self.dst.write(&self.buf[write_range])?;
            assert_eq!(written, self.chunk_len);

            // not enough space in buffer, copy remaining to
            // the head of buffer and reset buf position
            if self.buf.pos + MAX_SIZE >= BUFFER_SIZE {
                self.buf.reset_position();
            }

            // jump to next start sliding position
            self.buf.pos += MIN_SIZE;
            self.chunk_len = MIN_SIZE;
        }

        Ok(in_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        // flush remaining data to destination
        let p = self.buf.pos - self.chunk_len;
        if p < self.buf.clen {
            self.chunk_len = self.buf.clen - p;
            let write_range = p..p + self.chunk_len;
            let _ = self.dst.write(&self.buf[write_range])?;
        }

        // reset chunker
        self.buf.pos = MIN_SIZE;
        self.buf.clen = 0;
        self.chunk_len = MIN_SIZE;
        self.roll_hash = 0;

        self.dst.flush()
    }
}

impl<W: Write + Seek> Debug for FastChunker<W> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Chunker()")
    }
}

impl<W: Write + Seek> Seek for FastChunker<W> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.dst.seek(pos)
    }
}
