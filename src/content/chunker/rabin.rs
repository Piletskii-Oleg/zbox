use crate::content::chunker::buffer::ChunkerBuf;
use crate::content::chunker::Chunking;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::fmt::{self, Debug};
use std::io::Write;
use std::ops::Range;

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

pub(super) struct RabinChunker {
    params: ChunkerParams, // chunker parameters
}

/// Pre-calculated chunker parameters
#[derive(Clone, Deserialize, Serialize)]
struct ChunkerParams {
    poly_pow: u64,     // poly power
    out_map: Vec<u64>, // pre-computed out byte map, length is 256
    ir: Vec<u64>,      // irreducible polynomial, length is 256
}

impl RabinChunker {
    pub(super) fn new() -> RabinChunker {
        RabinChunker {
            params: ChunkerParams::new(),
        }
    }
}

impl Chunking for RabinChunker {
    fn next_write_range(
        &mut self,
        buf: &mut ChunkerBuf,
    ) -> Option<(Range<usize>, usize)> {
        let search_range = buf.pos..buf.clen;
        if let Some(length) = find_border(&buf[search_range], &self.params) {
            let write_range = buf.pos..buf.pos + length;

            buf.pos += length;

            Some((write_range, length))
        } else {
            None
        }
    }

    fn remaining_range(&self, buf: &ChunkerBuf) -> Range<usize> {
        buf.pos..buf.clen
    }
}

fn find_border(buf: &[u8], params: &ChunkerParams) -> Option<usize> {
    if buf.len() < MIN_SIZE {
        return Some(buf.len());
    }

    let remaining = min(MAX_SIZE, buf.len());
    let mut pos = WIN_SLIDE_POS;
    let mut chunk_len = WIN_SLIDE_POS;

    let mut win = [0u8; WIN_SIZE];
    let mut win_idx = 0;
    let mut roll_hash = 0;

    while pos < remaining {
        let ch = buf[pos];
        let out = win[win_idx] as usize;
        let pushed_out = params.out_map[out];

        // calculate Rabin rolling hash
        roll_hash = (roll_hash * PRIME) & MASK;
        roll_hash += u64::from(ch);
        roll_hash = roll_hash.wrapping_sub(pushed_out) & MASK;

        // forward circle window
        win[win_idx] = ch;
        win_idx = (win_idx + 1) & WIN_MASK;

        chunk_len += 1;
        pos += 1;

        if chunk_len >= MIN_SIZE {
            let chksum = roll_hash ^ params.ir[out];

            if (chksum & CUT_MASK) == 0 || chunk_len >= MAX_SIZE {
                return Some(chunk_len);
            }
        }
    }

    None
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
