use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use storage::concurrency::watermark::Watermark;

#[test]
fn empty_watermark_is_the_next_tid() {
    let wm = Watermark::new(5);
    assert_eq!(wm.get(), 5);

    let tids: Vec<_> = (0..3).map(|_| wm.begin()).collect();
    assert_eq!(tids, [5, 6, 7]);
    for tid in tids {
        wm.end(tid);
    }
    assert_eq!(wm.get(), 8);
}

#[test]
fn ending_a_transaction_above_the_minimum_keeps_the_watermark() {
    let wm = Watermark::new(1);
    let t1 = wm.begin();
    let t2 = wm.begin();
    let t3 = wm.begin();

    wm.end(t2);
    assert_eq!(wm.get(), t1);
    wm.end(t3);
    assert_eq!(wm.get(), t1);
}

#[test]
fn ending_the_minimum_moves_to_the_next_smallest() {
    let wm = Watermark::new(1);
    let t1 = wm.begin();
    let t2 = wm.begin();
    let t3 = wm.begin();
    assert_eq!(wm.get(), t1);

    wm.end(t1);
    assert_eq!(wm.get(), t2);
    // A transaction begun now is above every active one.
    let t4 = wm.begin();
    assert_eq!(wm.get(), t2);
    wm.end(t2);
    assert_eq!(wm.get(), t3);
    wm.end(t3);
    assert_eq!(wm.get(), t4);
    wm.end(t4);
    assert_eq!(wm.get(), t4 + 1);
}

#[test]
#[should_panic(expected = "transaction 2 is not active")]
fn ending_an_unknown_transaction_panics() {
    let wm = Watermark::new(1);
    wm.begin();
    wm.end(2);
}

#[test]
#[should_panic(expected = "transaction 1 is not active")]
fn ending_a_transaction_twice_panics() {
    let wm = Watermark::new(1);
    let tid = wm.begin();
    wm.end(tid);
    wm.end(tid);
}

#[test]
#[should_panic(expected = "the first Tid must be at least 1")]
fn starting_at_the_invalid_tid_panics() {
    Watermark::new(0);
}

#[test]
#[should_panic(expected = "transaction ids are exhausted")]
fn begin_panics_before_the_top_bit() {
    let wm = Watermark::new((1 << 63) - 1);
    wm.begin();
}

#[test]
fn concurrent_begin_and_end() {
    const THREADS: u64 = 8;
    const ITERS: u64 = 10_000;

    let wm = Arc::new(Watermark::new(1));
    let done = Arc::new(AtomicBool::new(false));

    let monitor = {
        let wm = Arc::clone(&wm);
        let done = Arc::clone(&done);
        thread::spawn(move || {
            let mut last = wm.get();
            while !done.load(Ordering::Relaxed) {
                let now = wm.get();
                assert!(now >= last, "watermark went from {last} to {now}");
                last = now;
            }
        })
    };

    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            let wm = Arc::clone(&wm);
            thread::spawn(move || {
                for _ in 0..ITERS {
                    let tid = wm.begin();
                    let mark = wm.get();
                    assert!(mark <= tid, "watermark {mark} is above active {tid}");
                    wm.end(tid);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    monitor.join().unwrap();

    // Every active Tid is below the counter, so reaching it means none is
    // left active.
    assert_eq!(wm.get(), 1 + THREADS * ITERS);
}
