extern crate bencher;
use msg_store::Uuid;
use msg_store_db_plugin::{Db, Bytes};
use msg_store_plugin_leveldb::Leveldb;
use std::{
    fs::{remove_dir_all,create_dir_all},
    path::Path,
    time::Instant
};

const TMP_PATH: &'static str = "/tmp/msg-store-plugin-leveldb";

fn dir_setup() {
    let tmp_path = Path::new(TMP_PATH);
    if tmp_path.exists() {
        remove_dir_all(tmp_path).unwrap();
    }
    create_dir_all(tmp_path).unwrap();
}

fn dir_teardown() {
    let tmp_path = Path::new(TMP_PATH);
    if tmp_path.exists() {
        remove_dir_all(tmp_path).unwrap();
    }
}

fn writes() {
    dir_setup();
    let mut level = Leveldb::new(Path::new(TMP_PATH)).unwrap();
    let msgs = (0..100_000).map(|i| {
        let msg = Bytes::copy_from_slice("hello, world!".as_bytes());
        let ln = msg.len() as u32;
        (Uuid::from_string(&format!("1-1-{}", i)).unwrap(), msg, ln)
    }).collect::<Vec<(Uuid, Bytes, u32)>>();
    let start = Instant::now();
    let _list = msgs.into_iter().map(|(uuid, msg, ln)| {
        level.add(uuid, msg.clone(), ln).unwrap();
        ()
    }).collect::<Vec<()>>();
    let end = start.elapsed().as_secs_f64();
    println!("Total duration: {}, writes/sec: {}", end, (100_000.0)/end);
    dir_teardown();
}

// fn a(bench: &mut Bencher) {
//     bench.iter(|| {
//         (0..1000).fold(0, |x, y| x + y)
//     })
// }

// fn b(bench: &mut Bencher) {
//     const N: usize = 1024;
//     bench.iter(|| {
//         vec![0u8; N]
//     });
 
//     bench.bytes = N as u64;
// }

fn main() {
    writes();
}

// benchmark_group!(benches, writes);
// benchmark_main!(benches);