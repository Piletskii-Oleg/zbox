use crate::content::chunker::buffer::{ChunkerBuf, BUFFER_SIZE};
use std::cmp::min;
use std::io::{Seek, SeekFrom, Write};

const KB: usize = 1024;
const MIN_CHUNK_SIZE: usize = 2 * KB;
const NORMAL_CHUNK_SIZE: usize = MIN_CHUNK_SIZE + 8 * KB;
const MAX_CHUNK_SIZE: usize = 64 * KB;

const WINDOW_SIZE: usize = 8;

const BYTE: usize = 0xAA;
const MASK_S: usize = 0x2F;
const MASK_L: usize = 0x2C;

const LEST: usize = 64;

pub(super) struct UltraChunker<W: Write + Seek> {
    dst: W,
    out_window: [u8; WINDOW_SIZE],
    in_window: [u8; WINDOW_SIZE],
    distance_map: Vec<Vec<usize>>,
    chunk_len: usize,
    distance: usize,
    equal_window_count: usize,
    buf: ChunkerBuf,
}

fn distance_map() -> Vec<Vec<usize>> {
    (0u8..=255u8)
        .map(|byte| {
            (0u8..=255u8)
                .map(|this_byte| (byte ^ this_byte).count_ones() as usize)
                .collect()
        })
        .collect()
}

impl<W: Write + Seek> UltraChunker<W> {
    pub fn new(dst: W) -> Self {
        Self {
            dst,
            out_window: [0u8; WINDOW_SIZE],
            in_window: [0u8; WINDOW_SIZE],
            distance_map: distance_map(),
            chunk_len: MIN_CHUNK_SIZE,
            distance: 0,
            equal_window_count: 0,
            buf: ChunkerBuf::new(),
        }
    }

    pub(super) fn into_inner(mut self) -> std::io::Result<W> {
        self.flush()?;
        Ok(self.dst)
    }

    fn calculate_new_distance(&mut self) {
        self.distance = self
            .out_window
            .iter()
            .map(|&byte| self.distance_map[BYTE][byte as usize])
            .sum();
    }

    fn slide_one_byte(&mut self, index: usize) {
        let old = self.out_window[index];
        let new = self.in_window[index];

        self.distance += self.distance_map[BYTE][new as usize];
        self.distance -= self.distance_map[BYTE][old as usize];
    }

    fn write_to_dst(&mut self) -> std::io::Result<usize> {
        let write_range = self.buf.pos - self.chunk_len..self.buf.pos;
        let written = self.dst.write(&self.buf[write_range])?;
        assert_eq!(written, self.chunk_len);

        if self.buf.pos + MAX_CHUNK_SIZE >= BUFFER_SIZE {
            self.buf.reset_position();
        }

        self.buf.pos += MIN_CHUNK_SIZE;
        self.chunk_len = MIN_CHUNK_SIZE;
        Ok(written)
    }

    fn generate_chunk(&mut self) -> Option<std::io::Result<usize>> {
        let out_range = self.buf.pos..self.buf.pos + 8;
        self.out_window.copy_from_slice(&self.buf[out_range]);
        self.buf.pos += 8;
        self.chunk_len += 8;
        self.calculate_new_distance();

        if let Some(result) = self.try_get_chunk(NORMAL_CHUNK_SIZE, MASK_S) {
            return Some(result);
        }

        if let Some(result) = self.try_get_chunk(MAX_CHUNK_SIZE, MASK_L) {
            return Some(result);
        }

        if self.chunk_len >= MAX_CHUNK_SIZE {
            return Some(self.write_to_dst());
        }

        None
    }

    fn try_get_chunk(
        &mut self,
        size_limit: usize,
        mask: usize,
    ) -> Option<std::io::Result<usize>> {
        while self.chunk_len < size_limit {
            if self.buf.pos + 8 > self.buf.clen {
                return None;
            }

            let in_range = self.buf.pos..self.buf.pos + 8;
            self.in_window.copy_from_slice(&self.buf[in_range]);

            if self.in_window == self.out_window {
                self.equal_window_count += 1;
                if self.equal_window_count == LEST {
                    self.chunk_len += 8;
                    self.buf.pos += 8;
                    return Some(self.write_to_dst());
                } else {
                    self.buf.pos += 8;
                    self.chunk_len += 8;
                    continue;
                }
            }

            self.equal_window_count = 0;
            for j in 0..8 {
                if (self.distance & mask) == 0 {
                    return Some(self.write_to_dst());
                }
                self.slide_one_byte(j);
            }

            self.out_window.copy_from_slice(&self.in_window);
            self.buf.pos += 8;
            self.chunk_len += 8;
        }
        None
    }
}

impl<W: Write + Seek> Write for UltraChunker<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let in_len = min(BUFFER_SIZE - self.buf.clen, buf.len());
        assert!(in_len > 0);
        self.buf.copy_in(buf, in_len);

        while self.buf.has_something() {
            self.generate_chunk()
                .map_or_else(|| Ok(0), |inner_result| inner_result)?;
        }

        Ok(in_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        // flush remaining data to destination
        let p = self.buf.pos - self.chunk_len;
        if p < self.buf.clen {
            self.chunk_len = self.buf.clen - p;
            let write_range = p..p + self.chunk_len;
            let _ = self.dst.write(&self.buf[write_range])?;
        }

        // reset chunker
        self.buf.pos = MIN_CHUNK_SIZE;
        self.buf.clen = 0;
        self.chunk_len = MIN_CHUNK_SIZE;

        self.dst.flush()
    }
}

impl<W: Write + Seek> Seek for UltraChunker<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.dst.seek(pos)
    }
}
