use criterion::{criterion_main, BenchmarkId, Criterion, Throughput};
use crossbeam_utils::sync::WaitGroup;
use env_logger::Env;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsServer};
use log::error;
use std::time::Duration;
use std::{env, iter, thread};
use tempfile::TempDir;

fn write_queued_kvstore(c: &mut Criterion) {
    env_logger::init_from_env(Env::default().default_filter_or("error"));
    let mut group = c.benchmark_group("pool");
    group.sample_size(11);
    for i in 1..6 {
        group.bench_with_input(BenchmarkId::new("ss", 2), &i, |b, &i| {
            b.iter(|| {
                let temp_dir = TempDir::new().unwrap();
                let engine = KvStore::open(temp_dir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(i as usize).unwrap();
                let mut server = KvsServer::new(engine, pool);
                thread::spawn(move || server.run("127.0.0.1:4000"));

                thread::sleep(Duration::from_secs(1));

                let client_pool = SharedQueueThreadPool::new(100).unwrap();
                let wg = WaitGroup::new();
                for i in 0..100 {
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        match KvsClient::new("127.0.0.1:4000") {
                            Ok(mut client) => {
                                if let Err(e) = client.set(format!("key{}", i), "value".to_owned())
                                {
                                    error!("set {:?}", e);
                                }
                            }
                            Err(e) => error!("connect {:?}", e),
                        }
                        drop(wg);
                    });
                }
                wg.wait();
            })
        });
    }
    group.finish();
}

fn read_queued_kvstore(c: &mut Criterion) {
    env_logger::init_from_env(Env::default().default_filter_or("error"));
    let mut group = c.benchmark_group("read_queued_kvstore");

        group.bench_with_input(BenchmarkId::new("read", 1), &1, |b, &i| {
            let engine = KvStore::open(env::current_dir().unwrap()).unwrap();
            let pool = SharedQueueThreadPool::new(1000).unwrap();
            let mut server = KvsServer::new(engine, pool);
            thread::spawn(move || server.run("127.0.0.1:4000"));
            thread::sleep(Duration::from_secs(1));

            let client_pool = SharedQueueThreadPool::new(10).unwrap();
            let wg = WaitGroup::new();
            for i in 0..100 {
                let wg = wg.clone();
                client_pool.spawn(move || {
                    let mut client = KvsClient::new("127.0.0.1:4000").unwrap();
                    client.set(format!("key{}", i), "value".to_owned()).unwrap();
                    drop(wg);
                });
            }
            wg.wait();

            b.iter(|| {
                let wg = WaitGroup::new();
                for i in 0..100 {
                    let wg = wg.clone();
                    client_pool.spawn(move || {
                        let mut client = KvsClient::new("127.0.0.1:4000").unwrap();
                        let res = client.get(format!("key{}", i));
                        assert_eq!(res.unwrap(), Some("value".to_owned()));
                        drop(wg);
                    })
                }
                wg.wait();
            })
        });
    group.finish();
}

criterion::criterion_group!(benches, read_queued_kvstore, write_queued_kvstore);
criterion_main!(benches);
