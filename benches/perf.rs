use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion,
};
use std::io::{Read, Seek, SeekFrom};
use std::{env, fs, io};
use zbox::{ChunkingAlgorithm, File, OpenOptions, Repo, RepoOpener};

const PATH: &str = "create.zip";

pub fn performance_benchmark(c: &mut Criterion) {
    zbox::init_env();

    let data = fs::read(PATH).unwrap();

    let mut group = c.benchmark_group("Chunkers");
    for chunker in algorithms() {
        group.bench_function(
            BenchmarkId::new("write", chunker_string(chunker)),
            |b| {
                b.iter_batched(
                    || {
                        let mut repo = create_repo();
                        let mut file = create_file(&mut repo, chunker);
                        (file, repo)
                    },
                    |(mut file, repo)| {
                        write(&mut file, &data).unwrap();
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        let mut read_repo = create_repo();
        let mut read_file = create_file(&mut read_repo, chunker);
        read_file.write_once(&data).unwrap();
        group.bench_function(
            BenchmarkId::new("read", chunker_string(chunker)),
            |b| {
                b.iter_batched_ref(
                    || Vec::with_capacity(data.len()),
                    |mut buf| {
                        read(&mut read_file, &mut buf).unwrap();
                    },
                    BatchSize::LargeInput,
                )
            },
        );
    }
}

fn chunker_string(chunking_algorithm: ChunkingAlgorithm) -> String {
    format!("{:?}", chunking_algorithm)
}

fn create_repo() -> Repo {
    let mut dir = env::temp_dir();
    dir.push("zbox_perf_test");
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir(&dir).unwrap();
    let mut repo = RepoOpener::new()
        .create(true)
        .open(&format!("file://{}/repo", dir.display()), "pwd")
        .unwrap();
    repo
}

fn create_file(repo: &mut Repo, chunker: ChunkingAlgorithm) -> File {
    let file_path = "file://{}/repo/file";
    if repo.path_exists(file_path).unwrap() {
        repo.remove_file(file_path).unwrap();
    }

    let mut file = OpenOptions::new()
        .create(true)
        .dedup_chunk(true)
        .chunking_algorithm(chunker)
        .open(repo, "/file")
        .unwrap();
    file
}

fn write(file: &mut File, data: &[u8]) -> zbox::Result<()> {
    file.write_once(data)
}

fn read(file: &mut File, buf: &mut Vec<u8>) -> io::Result<usize> {
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_to_end(buf)
}

fn algorithms() -> Vec<ChunkingAlgorithm> {
    vec![
        ChunkingAlgorithm::Fast,
        ChunkingAlgorithm::Leap,
        ChunkingAlgorithm::Rabin,
        ChunkingAlgorithm::Super,
        ChunkingAlgorithm::Ultra,
    ]
}

criterion_group!(benches, performance_benchmark);
criterion_main!(benches);
