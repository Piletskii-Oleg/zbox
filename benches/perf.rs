use criterion::measurement::WallTime;
use criterion::{
    criterion_group, criterion_main, BatchSize, BenchmarkGroup, BenchmarkId,
    Criterion, Throughput,
};
use std::io::{Read, Seek, SeekFrom};
use std::{fs, io};
use zbox::{ChunkingAlgorithm, File, OpenOptions, Repo, RepoOpener};

struct Dataset<'a> {
    path: &'a str,
    name: &'a str,
    size: usize
}

impl<'a> Dataset<'a> {
    fn new(path: &'a str, name: &'a str) -> Self {
        let size = {
            let data = fs::read(path).unwrap();
            data.len()
        };
        Dataset { path, name, size }
    }
}

#[derive(Copy, Clone)]
struct Parameters<'a> {
    dataset: &'a Dataset<'a>,
    chunker: ChunkingAlgorithm,
    storage: &'a str,
}

pub fn performance_benchmark(c: &mut Criterion) {
    zbox::
    init_env();

    let datasets = vec![
        //Dataset::new("create.zip", "create"),
        //Dataset::new("mail.tar", "mail"),
        Dataset::new("linux.tar", "linux"),
    ];

    let storages = vec![
        "file",
        //"mem"
    ];

    let mut group = c.benchmark_group("Chunkers");
    group.sample_size(50);

    for dataset in datasets {
        for chunker in algorithms() {
            for storage in &storages {
                let parameters = Parameters {
                    dataset: &dataset,
                    chunker,
                    storage,
                };

                group.throughput(Throughput::Bytes(dataset.size as u64));
                bench_read_to_end(&mut group, parameters);
                bench_write_once(&mut group, parameters);
                bench_copy(&mut group, parameters);
            }
        }
    }
}

fn bench_copy(group: &mut BenchmarkGroup<WallTime>, parameters: Parameters) {
    let storage = parameters.storage;
    let chunker = parameters.chunker;
    let dataset = parameters.dataset;

    group.bench_function(
        BenchmarkId::new("copy", bench_string(parameters)),
        |b| {
            b.iter_batched(
                || {
                    let mut copy_repo = create_repo("copy-test", storage);
                    let from_file = {
                        let data = read_dataset(&dataset);
                        let mut from_file =
                            create_file(&mut copy_repo, chunker, "copy-test", "from", data.len());
                        from_file.write_once(&data).unwrap();
                        from_file
                    };
                    let mut to_file =
                        create_file(&mut copy_repo, chunker, "copy-test", "to", parameters.dataset.size);
                    to_file.write_once(b"www").unwrap();
                    copy_repo
                },
                |mut copy_repo| copy(&mut copy_repo, "from", "to").unwrap(),
                BatchSize::SmallInput,
            )
        },
    );

    Repo::destroy(&format!("{}:///tmp/{}/repo", storage, "copy-test")).unwrap();
}

fn read_dataset(dataset: &Dataset) -> Vec<u8> {
    let data = fs::read(&dataset.path).unwrap();
    data
}

fn bench_read_to_end(
    group: &mut BenchmarkGroup<WallTime>,
    parameters: Parameters,
) {
    let storage = parameters.storage;
    let chunker = parameters.chunker;
    let dataset = parameters.dataset;

    let mut read_repo = create_repo("read-test", storage);
    let mut read_file = {
        let data = read_dataset(&dataset);
        let mut read_file = create_file(&mut read_repo, chunker, "read-test", "file", parameters.dataset.size);
        read_file.write_once(&data).unwrap();
        read_file
    };

    group.bench_function(
        BenchmarkId::new("read", bench_string(parameters)),
        |b| {
            b.iter_batched_ref(
                || Vec::with_capacity(parameters.dataset.size),
                |mut buf| {
                    read_to_end(&mut read_file, &mut buf).unwrap();
                },
                BatchSize::LargeInput,
            )
        },
    );

    Repo::destroy(&format!("{}:///tmp/{}/repo", storage, "read-test")).unwrap();
}

fn bench_write_once(
    group: &mut BenchmarkGroup<WallTime>,
    parameters: Parameters,
) {
    let storage = parameters.storage;
    let chunker = parameters.chunker;
    let dataset = parameters.dataset;

    let data = read_dataset(&dataset);

    group.bench_function(
        BenchmarkId::new("write", bench_string(parameters)),
        |b| {
            b.iter_batched(
                || {
                    let mut repo = create_repo("write-test", storage);
                    let file = create_file(
                        &mut repo,
                        chunker,
                        "write-test",
                        "file",
                        data.len(),
                    );
                    (file, repo)
                },
                |(mut file, repo)| {
                    write_once(&mut file, &data).unwrap();
                },
                BatchSize::PerIteration,
            )
        },
    );

    Repo::destroy(&format!("{}:///tmp/{}/repo", storage, "write-test")).unwrap();
}

fn bench_string(parameters: Parameters) -> String {
    let storage = parameters.storage;
    let algorithm = parameters.chunker;
    let dataset = parameters.dataset;

    format!(
        "{}/{}/{:?}",
        dataset.name, storage, algorithm
    )
}

fn create_repo(repo_name: &str, storage: &str) -> Repo {
    let repo_path = format!("{}:///tmp/{}/repo", storage, repo_name);
    if Repo::exists(&repo_path).unwrap() {
        Repo::destroy(&repo_path).unwrap();
    }

    let repo = RepoOpener::new()
        .create_new(true)
        .open(&repo_path, "pwd")
        .unwrap();
    repo
}

fn create_file(
    repo: &mut Repo,
    chunker: ChunkingAlgorithm,
    repo_name: &str,
    file_name: &str,
    len: usize,
) -> File {
    let storage = repo_storage(repo);
    let file_path =
        format!("{}:///tmp/{}/repo/{}", storage, repo_name, file_name);
    if repo.path_exists(&file_path).unwrap() {
        repo.remove_file(&file_path).unwrap();
    }

    let mut file = OpenOptions::new()
        .create_new(true)
        .dedup_chunk(true)
        .chunking_algorithm(chunker)
        .open(repo, format!("/{}", file_name))
        .unwrap();

    //file.set_len(len).unwrap();
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
        ChunkingAlgorithm::Leap,
        ChunkingAlgorithm::Fast,
        ChunkingAlgorithm::Rabin,
        ChunkingAlgorithm::Super,
        ChunkingAlgorithm::Ultra,
    ]
}

criterion_group!(benches, performance_benchmark);
criterion_main!(benches);
