use rand::prelude::{Distribution, ThreadRng};
use rand_distr::Normal;
use std::cmp::min;
use std::fmt::{self, Debug};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};

use crate::content::chunker::buffer::{ChunkerBuf, BUFFER_SIZE};

// leap-based cdc constants
const MIN_CHUNK_SIZE: usize = 1024 * 16;
const MAX_CHUNK_SIZE: usize = 1024 * 64;

const WINDOW_PRIMARY_COUNT: usize = 22;
const WINDOW_SECONDARY_COUNT: usize = 2;
const WINDOW_COUNT: usize = WINDOW_PRIMARY_COUNT + WINDOW_SECONDARY_COUNT;

const WINDOW_SIZE: usize = 180;
const WINDOW_MATRIX_SHIFT: usize = 42; // WINDOW_MATRIX_SHIFT * 4 < WINDOW_SIZE - 5
const MATRIX_WIDTH: usize = 8;
const MATRIX_HEIGHT: usize = 255;

enum PointStatus {
    Satisfied,
    Unsatisfied(usize),
}

/// Chunker
pub struct LeapChunker<W: Write + Seek> {
    dst: W, // destination writer
    chunk_len: usize,
    ef_matrix: Vec<Vec<u8>>,
    buf: ChunkerBuf, // chunker buffer, fixed size: BUFFER_SIZE
}

impl<W: Write + Seek> LeapChunker<W> {
    pub fn new(dst: W) -> Self {
        let ef_matrix = generate_ef_matrix();

        LeapChunker {
            dst,
            chunk_len: MIN_CHUNK_SIZE,
            ef_matrix,
            buf: ChunkerBuf::new(MIN_CHUNK_SIZE),
        }
    }

    pub fn into_inner(mut self) -> IoResult<W> {
        self.flush()?;
        Ok(self.dst)
    }

    fn is_point_satisfied(&self) -> PointStatus {
        // primary check, T<=x<M where T is WINDOW_SECONDARY_COUNT and M is WINDOW_COUNT
        for i in WINDOW_SECONDARY_COUNT..WINDOW_COUNT {
            if !self.is_window_qualified(
                &self.buf[self.buf.pos - i - WINDOW_SIZE..self.buf.pos - i],
            ) {
                // window is WINDOW_SIZE bytes long and moves to the left
                let leap = WINDOW_COUNT - i;
                return PointStatus::Unsatisfied(leap);
            }
        }

        //secondary check, 0<=x<T bytes
        for i in 0..WINDOW_SECONDARY_COUNT {
            if !self.is_window_qualified(
                &self.buf[self.buf.pos - i - WINDOW_SIZE..self.buf.pos - i],
            ) {
                let leap = WINDOW_COUNT - WINDOW_SECONDARY_COUNT - i;
                return PointStatus::Unsatisfied(leap);
            }
        }

        PointStatus::Satisfied
    }

    fn is_window_qualified(&self, window: &[u8]) -> bool {
        (0..5)
            .map(|index| window[WINDOW_SIZE - 1 - index * WINDOW_MATRIX_SHIFT]) // init array
            .enumerate()
            .map(|(index, byte)| self.ef_matrix[byte as usize][index]) // get elements from ef_matrix
            .fold(0, |acc, value| acc ^ (value as usize)) // why is acc of type usize?
            != 0
    }

    fn write_to_dst(&mut self) -> IoResult<usize> {
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
}

impl<W: Write + Seek> Write for LeapChunker<W> {
    // consume bytes stream, output chunks
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        // copy source data into chunker buffer
        let in_len = min(BUFFER_SIZE - self.buf.clen, buf.len());
        assert!(in_len > 0);
        self.buf.copy_in(buf, in_len);

        while self.buf.has_something() {
            if self.chunk_len >= MAX_CHUNK_SIZE {
                self.write_to_dst()?;
            } else {
                match self.is_point_satisfied() {
                    PointStatus::Satisfied => {
                        self.write_to_dst()?;
                    }
                    PointStatus::Unsatisfied(leap) => {
                        self.buf.pos += leap;
                        self.chunk_len += leap;
                    }
                };
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
        self.buf.pos = MIN_CHUNK_SIZE;
        self.chunk_len = MIN_CHUNK_SIZE;
        self.buf.clen = 0;

        self.dst.flush()
    }
}

impl<W: Write + Seek> Debug for LeapChunker<W> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Chunker()")
    }
}

impl<W: Write + Seek> Seek for LeapChunker<W> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.dst.seek(pos)
    }
}

fn generate_ef_matrix() -> Vec<Vec<u8>> {
    let base_matrix = (0..=255)
        .map(|index| vec![index; 5])
        .collect::<Vec<Vec<u8>>>(); // 256x5 matrix that looks like ((0,0,0,0,0), (1,1,1,1,1)..)

    let matrix_h = generate_matrix();
    let matrix_g = generate_matrix();

    let e_matrix = transform_base_matrix(&base_matrix, &matrix_h);
    let f_matrix = transform_base_matrix(&base_matrix, &matrix_g);

    let ef_matrix = e_matrix
        .iter()
        .zip(f_matrix.iter())
        .map(concatenate_bits_in_rows)
        .collect();
    ef_matrix
}

fn transform_base_matrix(
    base_matrix: &[Vec<u8>],
    additional_matrix: &[Vec<f64>],
) -> Vec<Vec<bool>> {
    base_matrix
        .iter()
        .map(|row| transform_byte_row(row[0], additional_matrix))
        .collect::<Vec<Vec<bool>>>()
}

fn concatenate_bits_in_rows(
    (row_x, row_y): (&Vec<bool>, &Vec<bool>),
) -> Vec<u8> {
    row_x
        .iter()
        .zip(row_y.iter())
        .map(concatenate_bits)
        .collect()
}

fn concatenate_bits((x, y): (&bool, &bool)) -> u8 {
    match (*x, *y) {
        (true, true) => 3,
        (true, false) => 2,
        (false, true) => 1,
        (false, false) => 0,
    }
}

fn transform_byte_row(byte: u8, matrix: &[Vec<f64>]) -> Vec<bool> {
    let mut new_row = vec![0u8; 5];
    (0..255)
        .map(|index| multiply_rows(byte, &matrix[index]))
        .enumerate()
        .for_each(|(index, value)| {
            if value > 0.0 {
                new_row[index / 51] += 1;
            }
        });

    new_row
        .iter()
        .map(|&number| if number % 2 == 0 { false } else { true })
        .collect::<Vec<bool>>()
}

fn multiply_rows(byte: u8, numbers: &[f64]) -> f64 {
    numbers
        .iter()
        .enumerate()
        .map(|(index, number)| {
            if (byte >> index) & 1 == 1 {
                *number
            } else {
                -(*number)
            }
        })
        .sum()
}

fn generate_matrix() -> Vec<Vec<f64>> {
    let normal = Normal::new(0.0, 1.0).unwrap();
    let mut rng = rand::thread_rng();

    (0..MATRIX_HEIGHT)
        .map(|_| generate_row(&normal, &mut rng))
        .collect()
}

fn generate_row(normal: &Normal<f64>, rng: &mut ThreadRng) -> Vec<f64> {
    (0..MATRIX_WIDTH).map(|_| normal.sample(rng)).collect()
}
