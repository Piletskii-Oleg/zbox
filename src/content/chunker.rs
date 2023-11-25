mod buffer;
mod fast;
mod leap;
mod rabin;
mod supercdc;
mod ultra;

use crate::content::chunker::buffer::{ChunkerBuf, BUFFER_SIZE};
use crate::content::chunker::leap::LeapChunker;
use std::fmt::{self, Debug};
use std::io::{Result as IoResult, Seek, SeekFrom, Write};
use std::ops::Range;

const MAX_SIZE: usize = 1024 * 64;

/// Trait that should be implemented by all chunking algorithm implementations that
/// are to be used with the Zbox chunker.
trait Chunking {
    /// Advances the buffer position and finds the next chunking cut-point, returning a range in the `buf`
    /// which corresponds to the found chunk.
    ///
    /// # Constraints
    /// After the method has been called, the following constraints must be held:
    ///
    /// Buffer's field `chunk_len` must be equal to the resulting range's length
    ///
    /// Buffer's field `pos` must be equal to the range's end-point
    /// # Return
    /// `None` should be returned if the found chunk length is less than minimum, or if buffer's end-point has been reached but no chunk was found,
    /// because buffer will be filled with more data at the next iteration, unless it is the end of file. In that case chunking will be done automatically.
    ///
    /// Otherwise Some(range) should be returned.
    /// # Buffer
    /// This method takes `buf` as a parameter because it must be instantiated in the Zbox chunker and not in the algorithm implementation
    /// for it to be possible to easily use different algorithms.
    fn next_write_range(
        &mut self,
        buf: &mut ChunkerBuf,
    ) -> Option<Range<usize>>;
}

/// Chunker
pub struct Chunker<W: Write + Seek> {
    dst: W,
    buffer: ChunkerBuf,
    chunker: Box<dyn Chunking>,
}

impl<W: Write + Seek> Chunker<W> {
    pub fn new(dst: W) -> Self {
        Self {
            dst,
            buffer: ChunkerBuf::new(),
            chunker: Box::new(LeapChunker::new()),
        }
    }

    pub fn into_inner(mut self) -> IoResult<W> {
        self.flush()?;
        Ok(self.dst)
    }

    fn with_chunker(dst: W, chunker: Box<dyn Chunking>) -> Self {
        Self {
            dst,
            buffer: ChunkerBuf::new(),
            chunker
        }
    }
}

impl<W: Write + Seek> Write for Chunker<W> {
    // consume bytes stream, output chunks
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let in_len = self.buffer.append(buf);

        while self.buffer.has_something() {
            if let Some(write_range) =
                self.chunker.next_write_range(&mut self.buffer)
            {
                assert_eq!(write_range.end, self.buffer.pos);

                let written = self.dst.write(&self.buffer[write_range])?;
                assert_eq!(written, self.buffer.chunk_len);

                self.buffer.chunk_len = 0;

                if self.buffer.pos + MAX_SIZE >= BUFFER_SIZE {
                    self.buffer.reset_position();
                }
            } else if self.buffer.clen - self.buffer.pos + self.buffer.chunk_len < MAX_SIZE {
                break;
            }
        }

        Ok(in_len)
    }

    fn flush(&mut self) -> IoResult<()> {
        let remaining_range = self.buffer.pos - self.buffer.chunk_len..self.buffer.clen;
        if remaining_range.len() > 0 {
            let _ = self.dst.write(&self.buffer[remaining_range])?;
        }

        self.buffer.pos = 0;
        self.buffer.clen = 0;
        self.buffer.chunk_len = 0;

        self.dst.flush()
    }
}

impl<W: Write + Seek> Debug for Chunker<W> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Chunker()")
    }
}

impl<W: Write + Seek> Seek for Chunker<W> {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.dst.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{copy, Cursor, Result as IoResult, Seek, SeekFrom, Write};
    use std::time::Instant;

    use super::*;
    use crate::base::crypto::{Crypto, RandomSeed, RANDOM_SEED_SIZE};
    use crate::base::init_env;
    use crate::base::utils::speed_str;
    use crate::content::chunk::Chunk;
    use crate::content::chunker::fast::FastChunker;
    use crate::content::chunker::rabin::RabinChunker;
    use crate::content::chunker::supercdc::SuperChunker;
    use crate::content::chunker::ultra::UltraChunker;

    const MIN_CHUNK_SIZE: usize = 2048;

    #[derive(Debug, Clone)]
    struct Sinker {
        len: usize,
        chks: Vec<Chunk>,
    }

    impl Write for Sinker {
        fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
            self.chks.push(Chunk::new(self.len, buf.len()));
            self.len += buf.len();
            Ok(buf.len())
        }

        fn flush(&mut self) -> IoResult<()> {
            // verify
            let sum = self.chks.iter().fold(0, |sum, ref t| sum + t.len);
            assert_eq!(sum, self.len);
            for i in 0..(self.chks.len() - 2) {
                assert_eq!(
                    self.chks[i].pos + self.chks[i].len,
                    self.chks[i + 1].pos
                );
            }

            Ok(())
        }
    }

    impl Seek for Sinker {
        fn seek(&mut self, _: SeekFrom) -> IoResult<u64> {
            Ok(0)
        }
    }

    #[derive(Debug, Clone)]
    struct VoidSinker {}

    impl Write for VoidSinker {
        fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> IoResult<()> {
            Ok(())
        }
    }

    impl Seek for VoidSinker {
        fn seek(&mut self, _: SeekFrom) -> IoResult<u64> {
            Ok(0)
        }
    }

    #[test]
    fn chunker() {
        init_env();

        const DATA_LEN: usize = 765 * 1024;

        let inner_chunkers: Vec<Box<dyn Chunking>> = vec![
            Box::new(RabinChunker::new()),
            Box::new(FastChunker::new()),
            Box::new(SuperChunker::new()),
            Box::new(UltraChunker::new()),
            Box::new(LeapChunker::new()),
        ];

        for (index, chunker) in inner_chunkers.into_iter().enumerate() {
            let mut data = vec![0u8; DATA_LEN];
            Crypto::random_buf(&mut data);
            let mut cur = Cursor::new(data);
            let sinker = Sinker {
                len: 0,
                chks: Vec::new(),
            };

            println!("{}", index);
            let mut ckr = Chunker::with_chunker(sinker, chunker);
            let result = copy(&mut cur, &mut ckr);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), DATA_LEN as u64);
            ckr.flush().unwrap();
        }
    }

    #[test]
    fn chunker_perf() {
        init_env();

        // perpare test data
        const DATA_LEN: usize = 100 * 1024 * 1024;
        let mut data = vec![0u8; DATA_LEN];
        let seed = RandomSeed::from(&[0u8; RANDOM_SEED_SIZE]);
        Crypto::random_buf_deterministic(&mut data, &seed);
        let mut cur = Cursor::new(data);
        let sinker = VoidSinker {};

        // test chunker performance
        let mut ckr = Chunker::new(sinker);
        let now = Instant::now();
        copy(&mut cur, &mut ckr).unwrap();
        ckr.flush().unwrap();
        let time = now.elapsed();

        println!("Chunker perf: {}", speed_str(&time, DATA_LEN));
    }

    #[test]
    #[ignore]
    fn file_dedup_ratio() {
        let path = std::path::Path::new("../rust-chunking/ubuntu.iso");
        chunker_draw_sizes(path.to_str().unwrap());
    }

    fn chunker_draw_sizes(path: &str) {
        use plotters::prelude::*;
        let vec = std::fs::read(path).unwrap();

        init_env();

        let mut sinker = Sinker {
            len: 0,
            chks: Vec::new(),
        };

        {
            let mut cur = Cursor::new(vec.clone());
            let mut ckr = Chunker::new(&mut sinker);
            copy(&mut cur, &mut ckr).unwrap();
            ckr.flush().unwrap();
        }

        const ADJUSTMENT: usize = 256;

        let mut chunks: HashMap<usize, u32> = HashMap::new();
        for chunk in sinker.chks {
            chunks
                .entry(chunk.len / ADJUSTMENT * ADJUSTMENT)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        let root_area =
            SVGBackend::new("chart.svg", (600, 400)).into_drawing_area();
        root_area.fill(&WHITE).unwrap();

        let mut ctx = ChartBuilder::on(&root_area)
            .set_label_area_size(LabelAreaPosition::Left, 40)
            .set_label_area_size(LabelAreaPosition::Bottom, 40)
            .caption("Chunk Size Distribution", ("sans-serif", 50))
            .build_cartesian_2d(
                (MIN_CHUNK_SIZE
                    ..(*chunks.keys().max().unwrap() as f64 * 1.02) as usize)
                    .into_segmented(),
                0u32..(*chunks.values().max().unwrap() as f64 * 1.02) as u32,
            )
            .unwrap();

        ctx.configure_mesh().draw().unwrap();

        ctx.draw_series(chunks.iter().map(|(&size, &count)| {
            let x0 = SegmentValue::Exact(size);
            let x1 = SegmentValue::Exact(size + ADJUSTMENT);
            let mut bar = Rectangle::new([(x0, count), (x1, 0)], RED.filled());
            bar
        }))
        .unwrap();
    }
}
