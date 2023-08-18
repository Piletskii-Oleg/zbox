#![allow(dead_code)]
#![cfg(feature = "test-perf")]

extern crate rand;
extern crate rand_xorshift;
extern crate zbox;

use std::env;
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::ptr;
use std::time::{Duration, Instant};

use rand::{RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use zbox::{init_env, File, OpenOptions, Repo, RepoOpener};

const DATA_LEN: usize = 60 * 1024 * 1024;
const FILE_LEN: usize = DATA_LEN / ROUND;
const ROUND: usize = 3;
const TX_ROUND: usize = 30;

#[inline]
fn time_str(duration: &Duration) -> String {
    format!("{}.{}s", duration.as_secs(), duration.subsec_nanos())
}

fn speed_str(duration: &Duration) -> String {
    let secs = duration.as_secs() as f32
        + duration.subsec_nanos() as f32 / 1_000_000_000.0;
    let speed = DATA_LEN as f32 / (1024.0 * 1024.0) / secs;
    format!("{:.2} MB/s", speed)
}

fn tps_str(duration: &Duration) -> String {
    if duration.eq(&Duration::default()) {
        return format!("N/A");
    }
    let secs = duration.as_secs() as f32
        + duration.subsec_nanos() as f32 / 1_000_000_000.0;
    let speed = TX_ROUND as f32 / secs;
    format!("{:.0} tx/s", speed)
}

fn print_result(
    read_time: &Duration,
    write_time: &Duration,
    tx_time: &Duration,
) {
    println!(
        "read: {}, write: {}, tps: {}",
        speed_str(&read_time),
        speed_str(&write_time),
        tps_str(&tx_time),
    );
}

fn make_test_data() -> Vec<u8> {
    // print!(
    //     "\nMaking {} MB pseudo random test data...",
    //     DATA_LEN / 1024 / 1024
    // );
    io::stdout().flush().unwrap();
    let mut buf = vec![0u8; DATA_LEN];
    let mut rng = XorShiftRng::from_seed([42u8; 16]);
    rng.fill_bytes(&mut buf);
    //println!("done\n");
    buf
}

fn test_baseline(data: &Vec<u8>, dir: &Path) -> BaselineResult {
    let mut buf = vec![0u8; FILE_LEN];
    let tx_time = Duration::default();

    // test memcpy speed
    let now = Instant::now();
    for i in 0..ROUND {
        unsafe {
            ptr::copy_nonoverlapping(
                (&data[i * FILE_LEN..(i + 1) * FILE_LEN]).as_ptr(),
                (&mut buf[..]).as_mut_ptr(),
                FILE_LEN,
            );
        }
    }
    let memcpy_time = now.elapsed();
    let memcpy = OnePerfResult {
        read_time: memcpy_time,
        write_time: memcpy_time,
        tx_time };

    // test os file system speed
    let now = Instant::now();
    for i in 0..ROUND {
        let path = dir.join(format!("file_{}", i));
        let mut file = fs::File::create(&path).unwrap();
        file.write_all(&data[i * FILE_LEN..(i + 1) * FILE_LEN])
            .unwrap();
        file.flush().unwrap();
    }
    let write_time = now.elapsed();

    let now = Instant::now();
    for i in 0..ROUND {
        let path = dir.join(format!("file_{}", i));
        let mut file = fs::File::open(&path).unwrap();
        file.read_to_end(&mut buf).unwrap();
    }
    let read_time = now.elapsed();
    let file_system = OnePerfResult{read_time, write_time, tx_time};

    BaselineResult {memcpy, file_system}
}

fn make_files(repo: &mut Repo) -> Vec<File> {
    let mut files: Vec<File> = Vec::new();
    for i in 0..ROUND {
        let filename = format!("/file_{}", i);
        let file = OpenOptions::new()
            .create(true)
            .open(repo, filename)
            .unwrap();
        files.push(file);
    }
    files
}

#[derive(Copy, Clone)]
struct OnePerfResult {
    read_time: Duration,
    write_time: Duration,
    tx_time: Duration,
}

#[derive(Copy, Clone)]
struct PerfResult{
    no_compress: OnePerfResult,
    compress: OnePerfResult,
}

#[derive(Copy, Clone)]
struct BaselineResult{
    memcpy: OnePerfResult,
    file_system: OnePerfResult,
}

fn test_perf(repo: &mut Repo, files: &mut Vec<File>, data: &[u8]) -> OnePerfResult {
    io::stdout().flush().unwrap();

    // write
    let now = Instant::now();
    for i in 0..ROUND {
        let data = &data[i * FILE_LEN..(i + 1) * FILE_LEN];
        files[i].write_once(&data[..]).unwrap();
    }
    let write_time = now.elapsed();

    // read
    let mut buf = Vec::new();
    let now = Instant::now();
    for i in 0..ROUND {
        files[i].seek(SeekFrom::Start(0)).unwrap();
        let read = files[i].read_to_end(&mut buf).unwrap();
        assert_eq!(read, FILE_LEN);
    }
    let read_time = now.elapsed();

    // tx
    let mut dirs = Vec::new();
    for i in 0..TX_ROUND {
        dirs.push(format!("/dir{}", i));
    }
    let now = Instant::now();
    for i in 0..TX_ROUND {
        repo.create_dir(&dirs[i]).unwrap();
    }
    let tx_time = now.elapsed();
    for i in 0..TX_ROUND {
        repo.remove_dir(&dirs[i]).unwrap();
    }

    OnePerfResult {read_time, write_time, tx_time }
}

fn test_mem_perf(data: &[u8]) -> PerfResult {
    let mut repo = RepoOpener::new()
        .create(true)
        .open("mem://perf", "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);
    let no_compress = test_perf(&mut repo, &mut files, data);

    let mut repo = RepoOpener::new()
        .create(true)
        .compress(true)
        .open("mem://perf2", "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);
    let compress = test_perf(&mut repo, &mut files, data);

    PerfResult {no_compress, compress}
}

fn test_file_perf(data: &[u8], dir: &Path) -> PerfResult {
    let mut repo = RepoOpener::new()
        .create_new(true)
        .open(&format!("file://{}/repo", dir.display()), "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);
    let no_compress = test_perf(&mut repo, &mut files, data);

    let mut repo = RepoOpener::new()
        .create_new(true)
        .compress(true)
        .open(&format!("file://{}/repo2", dir.display()), "pwd")
        .unwrap();
    let mut files = make_files(&mut repo);
    let compress = test_perf(&mut repo, &mut files, data);

    PerfResult {no_compress, compress}
}

#[test]
fn my_perf_test() {
    init_env();

    const TEST_COUNT: usize = 30;

    let mut base_results = Vec::with_capacity(TEST_COUNT);
    let mut mem_results = Vec::with_capacity(TEST_COUNT);
    let mut file_results = Vec::with_capacity(TEST_COUNT);
    for test_num in 0..TEST_COUNT {
        let mut dir = env::temp_dir();
        dir.push(format!("zbox_perf_test_{test_num}"));
        if dir.exists() {
            fs::remove_dir_all(&dir).unwrap();
        }
        fs::create_dir(&dir).unwrap();

        let data = make_test_data();
        base_results.push(test_baseline(&data, &dir));
        mem_results.push(test_mem_perf(&data));
        file_results.push(test_file_perf(&data, &dir));
        fs::remove_dir_all(&dir).unwrap();
    }

    println!("---------------------------------------------");
    println!("Baseline performance test");
    println!("---------------------------------------------");
    handle_base_results(&base_results);

    println!("---------------------------------------------");
    println!("Memory storage performance test");
    println!("---------------------------------------------");
    handle_results(&mem_results);

    println!("---------------------------------------------");
    println!("File storage performance test");
    println!("---------------------------------------------");
    handle_results(&file_results);
}

fn handle_base_results(results: &[BaselineResult]) {
    println!("memcpy");
    println!("---------------------------------------------");
    let no_compress = results.iter().map(|res| res.memcpy).collect::<Vec<OnePerfResult>>();
    let no_compress_average = get_average_results(&no_compress);
    print_result(&no_compress_average.read_time, &no_compress_average.write_time, &no_compress_average.tx_time);

    println!("---------------------------------------------");
    println!("file system");
    println!("---------------------------------------------");
    let compress = results.iter().map(|res| res.file_system).collect::<Vec<OnePerfResult>>();
    let compress_average = get_average_results(&compress);
    print_result(&compress_average.read_time, &compress_average.write_time, &compress_average.tx_time);
}

fn handle_results(results: &[PerfResult]) {
    println!("No compress");
    println!("---------------------------------------------");
    let no_compress = results.iter().map(|res| res.no_compress).collect::<Vec<OnePerfResult>>();
    let no_compress_average = get_average_results(&no_compress);
    print_result(&no_compress_average.read_time, &no_compress_average.write_time, &no_compress_average.tx_time);

    println!("---------------------------------------------");
    println!("Compress");
    println!("---------------------------------------------");
    let compress = results.iter().map(|res| res.compress).collect::<Vec<OnePerfResult>>();
    let compress_average = get_average_results(&compress);
    print_result(&compress_average.read_time, &compress_average.write_time, &compress_average.tx_time);
}

fn get_average_results(times: &[OnePerfResult]) -> OnePerfResult {
    let read_average = times.iter()
        .map(|res| res.read_time)
        .map(|time| time.as_secs_f64())
        .sum::<f64>() / times.len() as f64;

    let write_average = times.iter()
        .map(|res| res.write_time)
        .map(|time| time.as_secs_f64())
        .sum::<f64>() / times.len() as f64;

    let tx_average = times.iter()
        .map(|res| res.tx_time)
        .map(|time| time.as_secs_f64())
        .sum::<f64>() / times.len() as f64;

    OnePerfResult {
        read_time: Duration::from_secs_f64(read_average),
        write_time: Duration::from_secs_f64(write_average),
        tx_time: Duration::from_secs_f64(tx_average),
    }
}
