use std::cmp::min;
use rand::prelude::{Distribution, ThreadRng};
use rand_distr::Normal;
use std::fmt::{self, Debug};
use std::io::Write;
use std::ops::Range;

use crate::content::chunker::buffer::ChunkerBuf;
use crate::content::chunker::Chunking;

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
pub struct LeapChunker {
    chunk_len: usize,
    ef_matrix: Vec<Vec<u8>>,
}

impl LeapChunker {
    pub fn new() -> Self {
        Self {
            chunk_len: 0,
            ef_matrix: generate_ef_matrix(),
        }
    }

    fn is_point_satisfied(&self, buf: &ChunkerBuf) -> PointStatus {
        let lower_bound = min(WINDOW_SECONDARY_COUNT, buf.clen-buf.pos);
        let upper_bound = min(WINDOW_COUNT, buf.clen - buf.pos);

        // primary check, T<=x<M where T is WINDOW_SECONDARY_COUNT and M is WINDOW_COUNT
        for i in lower_bound..upper_bound {
            if !self.is_window_qualified(
                &buf[buf.pos - i - WINDOW_SIZE..buf.pos - i],
            ) {
                // window is WINDOW_SIZE bytes long and moves to the left
                let leap = WINDOW_COUNT - i;
                return PointStatus::Unsatisfied(leap);
            }
        }

        //secondary check, 0<=x<T bytes
        for i in 0..lower_bound {
            if !self.is_window_qualified(
                &buf[buf.pos - i - WINDOW_SIZE..buf.pos - i],
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
}

impl Chunking for LeapChunker {
    fn next_write_range(
        &mut self,
        buf: &mut ChunkerBuf,
    ) -> Option<(Range<usize>, usize)> {
        if self.chunk_len < MIN_CHUNK_SIZE {
            let add = min(MIN_CHUNK_SIZE, buf.clen - buf.pos);
            buf.pos += add;
            self.chunk_len += add;
            return None;
        }

        if self.chunk_len > MAX_CHUNK_SIZE {
            let write_range = buf.pos - self.chunk_len..buf.pos;
            let length = self.chunk_len;

            self.chunk_len = 0;

            Some((write_range, length))
        } else {
            match self.is_point_satisfied(buf) {
                PointStatus::Satisfied => {
                    let write_range = buf.pos - self.chunk_len..buf.pos;
                    let length = self.chunk_len;

                    self.chunk_len = 0;

                    Some((write_range, length))
                }
                PointStatus::Unsatisfied(leap) => {
                    buf.pos += leap;
                    self.chunk_len += leap;
                    None
                }
            }
        }
    }

    fn remaining_range(&self, buf: &ChunkerBuf) -> Range<usize> {
        buf.pos - self.chunk_len..buf.clen
    }
}

impl Debug for LeapChunker {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Chunker()")
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
