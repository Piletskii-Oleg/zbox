use crate::content::chunker::buffer::ChunkerBuf;
use crate::content::chunker::Chunking;
use std::cmp::min;
use std::fmt;
use std::fmt::Debug;
use std::ops::Range;

const KB: usize = 1024;
const MIN_CHUNK_SIZE: usize = 2 * KB;
const NORMAL_CHUNK_SIZE: usize = MIN_CHUNK_SIZE + 8 * KB;
const MAX_CHUNK_SIZE: usize = 64 * KB;

const WINDOW_SIZE: usize = 8;

const BYTE: usize = 0xAA;
const MASK_S: usize = 0x2F;
const MASK_L: usize = 0x2C;

const LEST: usize = 64;

pub(super) struct UltraChunker {
    out_window: [u8; WINDOW_SIZE],
    in_window: [u8; WINDOW_SIZE],
    distance_map: Vec<Vec<usize>>,
    chunk_len: usize,
    distance: usize,
    equal_window_count: usize,
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

impl UltraChunker {
    pub fn new() -> Self {
        Self {
            out_window: [0u8; WINDOW_SIZE],
            in_window: [0u8; WINDOW_SIZE],
            distance_map: distance_map(),
            chunk_len: 0,
            distance: 0,
            equal_window_count: 0,
        }
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

    fn generate_chunk(&mut self, buf: &mut ChunkerBuf) -> Option<usize> {
        if buf.chunk_len < MIN_CHUNK_SIZE {
            let add = min(MIN_CHUNK_SIZE, buf.clen - buf.pos);
            buf.pos += add;
            buf.chunk_len += add;
            return None;
        }

        let out_range = buf.pos..buf.pos + 8;
        self.out_window.copy_from_slice(&buf[out_range]);
        buf.pos += 8;
        buf.chunk_len += 8;
        self.calculate_new_distance();

        if let Some(result) = self.try_get_chunk(buf, NORMAL_CHUNK_SIZE, MASK_S)
        {
            return Some(result);
        }

        if let Some(result) = self.try_get_chunk(buf, MAX_CHUNK_SIZE, MASK_L) {
            return Some(result);
        }

        if buf.chunk_len >= MAX_CHUNK_SIZE {
            return Some(buf.chunk_len);
        }

        None
    }

    fn try_get_chunk(
        &mut self,
        buf: &mut ChunkerBuf,
        size_limit: usize,
        mask: usize,
    ) -> Option<usize> {
        while buf.chunk_len < size_limit {
            if buf.pos + 8 > buf.clen {
                return None;
            }

            let in_range = buf.pos..buf.pos + 8;
            self.in_window.copy_from_slice(&buf[in_range]);

            if self.in_window == self.out_window {
                self.equal_window_count += 1;
                if self.equal_window_count == LEST {
                    buf.chunk_len += 8;
                    buf.pos += 8;
                    return Some(buf.chunk_len);
                } else {
                    buf.pos += 8;
                    buf.chunk_len += 8;
                    continue;
                }
            }

            self.equal_window_count = 0;
            for j in 0..8 {
                if (self.distance & mask) == 0 {
                    return Some(buf.chunk_len);
                }
                self.slide_one_byte(j);
            }

            self.out_window.copy_from_slice(&self.in_window);
            buf.pos += 8;
            buf.chunk_len += 8;
        }
        None
    }
}

impl Chunking for UltraChunker {
    fn next_write_range(
        &mut self,
        buf: &mut ChunkerBuf,
    ) -> Option<Range<usize>> {
        if let Some(length) = self.generate_chunk(buf) {
            let write_range = buf.pos - length..buf.pos;

            buf.chunk_len = 0;

            Some(write_range)
        } else {
            None
        }
    }

    fn remaining_range(&self, buf: &ChunkerBuf) -> Range<usize> {
        buf.pos - buf.chunk_len..buf.clen
    }
}

impl Debug for UltraChunker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UltraChunker()")
    }
}
