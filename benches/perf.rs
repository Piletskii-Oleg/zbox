use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion,
};
use std::io::{Read, Seek, SeekFrom};
use std::ops::Deref;
use std::{env, fs, io};
use zbox::{ChunkingAlgorithm, File, OpenOptions, Repo, RepoOpener};

const CREATE_PATH: &str = "create.zip";

struct Dataset {
    data: Vec<u8>,
    name: &'static str,
    size: usize,
}

impl Dataset {
    fn new(path: &str, name: &'static str) -> Self {
        let data = fs::read(path).unwrap();
        let size = data.len();
        Dataset { data, name, size }
    }
}

impl Deref for Dataset {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub fn performance_benchmark(c: &mut Criterion) {
    zbox::init_env();

    let create = Dataset::new(CREATE_PATH, "create");
    let mail = Dataset::new("mail", "mail");
    let datasets = vec![create, mail];

    let mut group = c.benchmark_group("Chunkers");
    for dataset in datasets {
        let zero_data = vec![0u8; dataset.len()];
        for chunker in algorithms() {
            group.bench_function(
                BenchmarkId::new("write", bench_string(chunker, &dataset)),
                |b| {
                    b.iter_batched(
                        || {
                            let mut repo = create_repo("write-test");
                            let file = create_file(
                                &mut repo,
                                chunker,
                                "write-test",
                                "file",
                                &zero_data,
                            );
                            (file, repo)
                        },
                        |(mut file, _)| {
                            write(&mut file, &dataset).unwrap();
                        },
                        BatchSize::LargeInput,
                    )
                },
            );

            let mut read_repo = create_repo("read-test");
            let mut read_file = create_file(
                &mut read_repo,
                chunker,
                "read-test",
                "file",
                &zero_data,
            );
            read_file.write_once(&dataset).unwrap();
            group.bench_function(
                BenchmarkId::new("read", bench_string(chunker, &dataset)),
                |b| {
                    b.iter_batched_ref(
                        || Vec::with_capacity(dataset.len()),
                        |mut buf| {
                            read(&mut read_file, &mut buf).unwrap();
                        },
                        BatchSize::LargeInput,
                    )
                },
            );

            let mut copy_repo = create_repo("copy-test");
            let mut from_file = create_file(
                &mut copy_repo,
                chunker,
                "copy-test",
                "from",
                &zero_data,
            );
            from_file.write_once(&dataset).unwrap();
            let mut to_file = create_file(
                &mut copy_repo,
                chunker,
                "copy-test",
                "to",
                &zero_data,
            );
            to_file.write_once(b"www").unwrap();
            group.bench_function(
                BenchmarkId::new("copy", bench_string(chunker, &dataset)),
                |b| {
                    b.iter_batched(
                        || {
                            read_file.seek(SeekFrom::Start(0)).unwrap();
                        },
                        |_| copy(&mut copy_repo, "from", "to").unwrap(),
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }
}

fn bench_string(algorithm: ChunkingAlgorithm, dataset: &Dataset) -> String {
    let mb_size = dataset.size / 1024 / 1024;
    format!("{} ({} MB)/{:?}", dataset.name, mb_size, algorithm)
}

fn create_repo(path: &str) -> Repo {
    let mut dir = env::temp_dir();
    dir.push(path);
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir(&dir).unwrap();
    let repo = RepoOpener::new()
        .create(true)
        .open(&format!("file://{}/repo", dir.display()), "pwd")
        .unwrap();
    repo
}

fn create_file(
    repo: &mut Repo,
    chunker: ChunkingAlgorithm,
    repo_name: &str,
    file_name: &str,
    zero_data: &[u8],
) -> File {
    let file_path = format!("file:///tmp/{}/repo/{}", repo_name, file_name);
    if repo.path_exists(&file_path).unwrap() {
        repo.remove_file(&file_path).unwrap();
    }

    let mut file = OpenOptions::new()
        .create(true)
        .dedup_chunk(true)
        .chunking_algorithm(chunker)
        .open(repo, format!("/{}", file_name))
        .unwrap();
    file.write_once(zero_data).unwrap();
    file
}

fn write(file: &mut File, data: &[u8]) -> zbox::Result<()> {
    file.write_once(data)
}

fn read(file: &mut File, buf: &mut Vec<u8>) -> io::Result<usize> {
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_to_end(buf)
}

fn copy(repo: &mut Repo, from_file: &str, to_file: &str) -> zbox::Result<()> {
    let from = format!("/{}", from_file);
    let to = format!("/{}", to_file);
    repo.copy(from, to)
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
