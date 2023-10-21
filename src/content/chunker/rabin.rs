use std::cmp::min;
use std::fmt::{self, Debug};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut, Index, IndexMut, Range};
use serde::{Deserialize, Serialize};

// taken from pcompress implementation
// https://github.com/moinakg/pcompress
const PRIME: u64 = 153_191u64;
const MASK: u64 = 0x00ff_ffff_ffffu64;
const MIN_SIZE: usize = 16 * 1024; // minimal chunk size, 16k
const AVG_SIZE: usize = 32 * 1024; // average chunk size, 32k
const MAX_SIZE: usize = 64 * 1024; // maximum chunk size, 64k

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

// writer buffer length
const WTR_BUF_LEN: usize = 8 * MAX_SIZE;

pub(super) struct RabinChunker<W: Write + Seek> {
    pub(super) dst: W,
    buf: ChunkerBuf,
    params: ChunkerParams, // chunker parameters
    chunk_len: usize,
    win_idx: usize,
    roll_hash: u64,
    win: [u8; WIN_SIZE], // rolling hash circle window
}

/// Pre-calculated chunker parameters
#[derive(Clone, Deserialize, Serialize)]
struct ChunkerParams {
    poly_pow: u64,     // poly power
    out_map: Vec<u64>, // pre-computed out byte map, length is 256
    ir: Vec<u64>,      // irreducible polynomial, length is 256
}

struct ChunkerBuf {
    pos: usize,
    clen: usize,
    buf: Vec<u8>, // chunker buffer, fixed size: WTR_BUF_LEN
}

impl<'a, W: Write + Seek> RabinChunker<W> {
    pub(super) fn new(dst: W) -> RabinChunker<W> {
        RabinChunker {
            dst,
            buf: ChunkerBuf::new(),
            params: ChunkerParams::new(),
            chunk_len: WIN_SLIDE_POS,
            win_idx: 0,
            roll_hash: 0,
            win: [0u8; WIN_SIZE],
        }
    }

    pub(super) fn into_inner(mut self) -> IoResult<W> {
        self.flush()?;
        Ok(self.dst)
    }
}

impl<W: Write + Seek> Write for RabinChunker<W> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // copy source data into chunker buffer
        let in_len = min(WTR_BUF_LEN - self.buf.clen, buf.len());
        assert!(in_len > 0);
        self.buf.copy_into(buf, in_len);

        while self.buf.has_enough_space() {
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

                    // not enough space in buffer, copy remaining to
                    // the head of buffer and reset buf position
                    if self.buf.pos + MAX_SIZE >= WTR_BUF_LEN {
                        self.buf.reset_position();
                    }

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
            let _ = self.dst.write(&self.buf.buf[write_range])?;
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

impl<W: Write + Seek> Seek for RabinChunker<W> {
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

impl ChunkerBuf {
    fn new() -> Self {
        let mut buf = vec![0u8; WTR_BUF_LEN];
        buf.shrink_to_fit();

        Self {
            pos: WIN_SLIDE_POS,
            clen: 0,
            buf,
        }
    }

    fn reset_position(&mut self) {
        let left_len = self.clen - self.pos;
        let copy_range = self.pos..self.clen;

        self.buf.copy_within(copy_range, 0);
        self.clen = left_len;
        self.pos = 0;
    }

    fn copy_into(&mut self, buf: &[u8], in_len: usize) {
        let copy_range = self.clen..self.clen + in_len;
        self.buf[copy_range].copy_from_slice(&buf[..in_len]);
        self.clen += in_len;
    }

    fn has_enough_space(&self) -> bool {
        self.pos < self.clen
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