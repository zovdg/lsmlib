use slmlib::lsm::{self, KVStore};

fn main() {
    env_logger::init();

    let before_recovery = std::time::Instant::now();
    let mut lsm = lsm::OpenOptions::new()
        .merge_window(5)
        .open("tiny_lsm_bench")
        .unwrap();
    dbg!(before_recovery.elapsed());

    /*
    if let Some((k, _v)) = lsm.iter().next_back() {
        println!("max key recovered: {:?}", u64::from_le_bytes(*k));
    } else {
        println!("starting from scratch");
    }
    */

    let before_writes = std::time::Instant::now();
    for i in 1_u64..1_000_000_000 {
        lsm.put(i.to_le_bytes().to_vec(), [0; 100].to_vec())
            .unwrap();
        if i % 1_000_000 == 0 {
            log::info!(
                "{:.2} million wps - stats: xxx",
                i as f64 / (before_writes.elapsed().as_micros() + 1) as f64,
                // lsm.stats(),
            )
        }
    }
    // lsm.flush().unwrap();
    dbg!(before_writes.elapsed());

    std::thread::sleep(std::time::Duration::from_secs(200));
}
