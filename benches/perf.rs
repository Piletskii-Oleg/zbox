use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkGroup, BenchmarkId,
    Criterion,
};
use std::io::{Read, Seek, SeekFrom};
use std::{env, fs, io};
use zbox::{ChunkingAlgorithm, File, OpenOptions, Repo, RepoOpener};

struct Dataset<'a> {
    path: &'a str,
    name: &'a str,
}

impl<'a> Dataset<'a> {
    fn new(path: &'a str, name: &'a str) -> Self {
        Dataset { path, name }
    }
}

#[derive(Copy, Clone)]
struct Parameters<'a> {
    dataset: &'a Dataset<'a>,
    chunker: ChunkingAlgorithm,
    storage: &'a str,
}

pub fn performance_benchmark(c: &mut Criterion) {
    zbox::init_env();

    let datasets = vec![
        Dataset::new("create.zip", "create"),
        Dataset::new("mail.tar", "mail"),
        Dataset::new("linux.tar", "linux"),
    ];

    let storages = vec!["file", "mem"];

    let mut group = c.benchmark_group("Chunkers");
    group.sample_size(100);

    for dataset in datasets {
        for chunker in algorithms() {
            for storage in &storages {
                let parameters = Parameters {
                    dataset: &dataset,
                    chunker,
                    storage,
                };

                bench_write_once(&mut group, parameters);
                bench_read_to_end(&mut group, parameters);
                bench_copy(&mut group, parameters);
            }
        }
    }
}

fn bench_copy(group: &mut BenchmarkGroup<WallTime>, parameters: Parameters) {
    let storage = parameters.storage;
    let chunker = parameters.chunker;
    let dataset = parameters.dataset;

    let (data, zero_data) = read_dataset(&dataset);

    let mut copy_repo = create_repo("copy-test", storage);
    let mut from_file =
        create_file(&mut copy_repo, chunker, "copy-test", "from", &zero_data);
    from_file.write_once(&data).unwrap();
    let mut to_file =
        create_file(&mut copy_repo, chunker, "copy-test", "to", &zero_data);
    to_file.write_once(b"www").unwrap();
    group.bench_function(
        BenchmarkId::new("copy", bench_string(parameters, data.len())),
        |b| {
            b.iter_batched(
                || {},
                |_| copy(&mut copy_repo, "from", "to").unwrap(),
                BatchSize::SmallInput,
            )
        },
    );
}

fn read_dataset(dataset: &&Dataset) -> (Vec<u8>, Vec<u8>) {
    let data = fs::read(&dataset.path).unwrap();
    let zero_data = vec![0u8; data.len()];
    (data, zero_data)
}

fn bench_read_to_end(
    group: &mut BenchmarkGroup<WallTime>,
    parameters: Parameters,
) -> File {
    let storage = parameters.storage;
    let chunker = parameters.chunker;
    let dataset = parameters.dataset;

    let (data, zero_data) = read_dataset(&dataset);

    let mut read_repo = create_repo("read-test", storage);
    let mut read_file =
        create_file(&mut read_repo, chunker, "read-test", "file", &zero_data);
    read_file.write_once(&data).unwrap();
    group.bench_function(
        BenchmarkId::new("read", bench_string(parameters, data.len())),
        |b| {
            b.iter_batched_ref(
                || Vec::with_capacity(data.len()),
                |mut buf| {
                    read_to_end(&mut read_file, &mut buf).unwrap();
                },
                BatchSize::LargeInput,
            )
        },
    );
    read_file
}

fn bench_write_once(
    group: &mut BenchmarkGroup<WallTime>,
    parameters: Parameters,
) {
    let storage = parameters.storage;
    let chunker = parameters.chunker;
    let dataset = parameters.dataset;

    let (data, zero_data) = read_dataset(&dataset);

    group.bench_function(
        BenchmarkId::new("write", bench_string(parameters, data.len())),
        |b| {
            b.iter_batched(
                || {
                    let mut repo = create_repo("write-test", storage);
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
                    write_once(&mut file, &data).unwrap();
                },
                BatchSize::LargeInput,
            )
        },
    );
}

fn bench_string(parameters: Parameters, size: usize) -> String {
    let storage = parameters.storage;
    let algorithm = parameters.chunker;
    let dataset = parameters.dataset;

    let mb_size = size / 1024 / 1024;
    format!(
        "{} ({} MB)/{} storage/{:?}",
        dataset.name, mb_size, storage, algorithm
    )
}

fn create_repo(path: &str, storage: &str) -> Repo {
    let mut dir = env::temp_dir();
    dir.push(path);
    if dir.exists() {
        fs::remove_dir_all(&dir).unwrap();
    }
    fs::create_dir(&dir).unwrap();
    let repo = RepoOpener::new()
        .create(true)
        .open(&format!("{}://{}/repo", storage, dir.display()), "pwd")
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
    let storage = repo_storage(repo);
    let file_path =
        format!("{}:///tmp/{}/repo/{}", storage, repo_name, file_name);
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

fn repo_storage(repo: &Repo) -> String {
    repo.info()
        .unwrap()
        .uri()
        .split("://")
        .next()
        .unwrap()
        .to_string()
}

fn write_once(file: &mut File, data: &[u8]) -> zbox::Result<()> {
    file.write_once(data)
}

fn read_to_end(file: &mut File, buf: &mut Vec<u8>) -> io::Result<usize> {
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
