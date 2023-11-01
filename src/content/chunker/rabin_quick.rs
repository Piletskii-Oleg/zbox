use crate::content::chunker::buffer::{ChunkerBuf, BUFFER_SIZE};
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};

// taken from pcompress implementation
// https://github.com/moinakg/pcompress
const PRIME: u64 = 153_191u64;
const MASK: u64 = 0x00ff_ffff_ffffu64;
const MIN_SIZE: usize = 16 * 1024; // minimal chunk size, 16k
const AVG_SIZE: usize = 32 * 1024; // average chunk size, 32k
const MAX_SIZE: usize = 64 * 1024; // maximum chunk size, 64k

const MIN_BUF_SIZE: usize = 5 * MAX_SIZE;

// Irreducible polynomial for Rabin modulus, from pcompress
const FP_POLY: u64 = 0xbfe6_b8a5_bf37_8d83u64;

// since we will skip MIN_SIZE when sliding window, it only
// needs to target (AVG_SIZE - MIN_SIZE) cut length,
// note the (AVG_SIZE - MIN_SIZE) must be 2^n
const CUT_MASK: u64 = (AVG_SIZE - MIN_SIZE - 1) as u64;

// rolling hash window constants
const WIN_SIZE: usize = 16; // must be 2^n
const WIN_MASK: usize = WIN_SIZE - 1;
const WIN_SLIDE_OFFSET: usize = 64;
const WIN_SLIDE_POS: usize = MIN_SIZE - WIN_SLIDE_OFFSET;

pub(super) struct QuickChunker<W: Write + Seek> {
    pub(super) dst: W,
    buf: ChunkerBuf,
    params: ChunkerParams, // chunker parameters
    chunk_len: usize,
    win_idx: usize,
    roll_hash: u64,
    win: [u8; WIN_SIZE], // rolling hash circle window
    front: HashMap<[u8; 3], usize>,
    back: HashMap<[u8; 3], usize>,
}

/// Pre-calculated chunker parameters
#[derive(Clone, Deserialize, Serialize)]
struct ChunkerParams {
    poly_pow: u64,     // poly power
    out_map: Vec<u64>, // pre-computed out byte map, length is 256
    ir: Vec<u64>,      // irreducible polynomial, length is 256
}

impl<W: Write + Seek> QuickChunker<W> {
    pub(super) fn new(dst: W) -> QuickChunker<W> {
        QuickChunker {
            dst,
            buf: ChunkerBuf::new(WIN_SLIDE_POS),
            params: ChunkerParams::new(),
            chunk_len: WIN_SLIDE_POS,
            win_idx: 0,
            roll_hash: 0,
            win: [0u8; WIN_SIZE],
            front: HashMap::new(),
            back: HashMap::new(),
        }
    }

    pub(super) fn into_inner(mut self) -> IoResult<W> {
        self.flush()?;
        Ok(self.dst)
    }

    fn check_chunk(&mut self) -> Option<usize> {
        if self.buf.pos + 3 > self.buf.clen {
            return None;
        }

        let front_range = self.buf.pos..self.buf.pos + 3;
        if let Some(front_length) = self.front.get(&self.buf[front_range]) {
            if self.buf.pos + front_length > self.buf.clen {
                return None;
            }

            let end_range = self.buf.pos + front_length - 3..self.buf.pos + front_length;
            if let Some(end_length) = self.back.get(&self.buf[end_range]) {
                if *front_length == *end_length {
                    return Some(*front_length);
                }
            }
        }
        None
    }

    fn add_front_back(&mut self) {
        let front_range = self.buf.pos - self.chunk_len..self.buf.pos - self.chunk_len + 3;
        let mut front_win = [0u8; 3];
        front_win.copy_from_slice(&self.buf[front_range]);
        self.front.insert(front_win, self.chunk_len);

        let end_range = self.buf.pos - 3..self.buf.pos;
        let mut end_win = [0u8; 3];
        end_win.copy_from_slice(&self.buf[end_range]);
        self.back.insert(end_win, self.chunk_len);
    }

    fn write_repeated_chunks(&mut self) -> IoResult<()> {
        while let Some(jump) = self.check_chunk() {
            self.buf.pos += jump;
            self.chunk_len = jump;
            let write_range =
                self.buf.pos - self.chunk_len..self.buf.pos;
            let written = self.dst.write(&self.buf[write_range])?;
            assert_eq!(written, self.chunk_len);
        }
        Ok(())
    }
}

impl<W: Write + Seek> Write for QuickChunker<W> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // copy source data into chunker buffer
        let in_len = min(BUFFER_SIZE - self.buf.clen, buf.len());
        assert!(in_len > 0);
        self.buf.copy_in(buf, in_len);

        if self.buf.clen < MIN_BUF_SIZE {
            return Ok(in_len)
        }

        while self.buf.has_something() {
            // get current byte and pushed out byte
            let ch = self.buf[self.buf.pos];
            let out = self.win[self.win_idx] as usize;
            let pushed_out = self.params.out_map[out];

            // calculate Rabin rolling hash
            self.roll_hash = (self.roll_hash * PRIME) & MASK;
            self.roll_hash += u64::from(ch);
            self.roll_hash = self.roll_hash.wrapping_sub(pushed_out) & MASK;

            // forward circle window
            self.win[self.win_idx] = ch;
            self.win_idx = (self.win_idx + 1) & WIN_MASK;

            self.chunk_len += 1;
            self.buf.pos += 1;

            if self.chunk_len >= MIN_SIZE {
                let chksum = self.roll_hash ^ self.params.ir[out];

                // reached cut point, chunk can be produced now
                if (chksum & CUT_MASK) == 0 || self.chunk_len >= MAX_SIZE {
                    // write the chunk to destination writer,
                    // ensure it is consumed in whole
                    let write_range =
                        self.buf.pos - self.chunk_len..self.buf.pos;
                    let written = self.dst.write(&self.buf[write_range])?;
                    assert_eq!(written, self.chunk_len);

                    self.add_front_back();

                    // not enough space in buffer, copy remaining to
                    // the head of buffer and reset buf position
                    if self.buf.pos + MAX_SIZE >= BUFFER_SIZE {
                        self.buf.reset_position();
                    }

                    self.write_repeated_chunks()?;

                    // jump to next start sliding position
                    self.buf.pos += WIN_SLIDE_POS;
                    self.chunk_len = WIN_SLIDE_POS;
                }
            }
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
        self.buf.pos = WIN_SLIDE_POS;
        self.buf.clen = 0;
        self.chunk_len = WIN_SLIDE_POS;
        self.win_idx = 0;
        self.roll_hash = 0;
        self.win = [0u8; WIN_SIZE];

        self.dst.flush()
    }
}

impl<W: Write + Seek> Seek for QuickChunker<W> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.dst.seek(pos)
    }
}

impl ChunkerParams {
    fn new() -> Self {
        let mut cp = ChunkerParams::default();

        // calculate poly power, it is actually PRIME ^ WIN_SIZE
        for _ in 0..WIN_SIZE {
            cp.poly_pow = (cp.poly_pow * PRIME) & MASK;
        }

        // pre-calculate out map table and irreducible polynomial
        // for each possible byte, copy from PCompress implementation
        for i in 0..256 {
            cp.out_map[i] = (i as u64 * cp.poly_pow) & MASK;

            let (mut term, mut pow, mut val) = (1u64, 1u64, 1u64);
            for _ in 0..WIN_SIZE {
                if (term & FP_POLY) != 0 {
                    val += (pow * i as u64) & MASK;
                }
                pow = (pow * PRIME) & MASK;
                term *= 2;
            }
            cp.ir[i] = val;
        }

        cp
    }
}

impl Debug for ChunkerParams {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ChunkerParams()")
    }
}

impl Default for ChunkerParams {
    fn default() -> Self {
        let mut ret = ChunkerParams {
            poly_pow: 1,
            out_map: vec![0u64; 256],
            ir: vec![0u64; 256],
        };
        ret.out_map.shrink_to_fit();
        ret.ir.shrink_to_fit();
        ret
    }
}
