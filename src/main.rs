use matching_engine::{MatchingEngine, OrderType, PRECISION, Side};
use rand::prelude::*;
use std::time::Instant;

fn main() {
    println!("Start matching engine latency & throughput stress testing...\n");

    let mut engine = MatchingEngine::new();
    engine.set_position_limit(1, 1_000_000 * PRECISION); // 给 user 1 很大限额
    engine.set_rate_limit(1, 1_000_000_000);
    // -------------------------------
    // 场景1：纯挂单（maker only）压测
    // -------------------------------
    println!("=== Scenario 1: 100,000 PostOnly Orders ===");
    run_benchmark(&mut engine, 100_000, "maker_only", |id, price| {
        (OrderType::PostOnly, Side::Bid, price, 1 * PRECISION)
    });

    // 清空引擎，准备下一个场景
    let mut engine = MatchingEngine::new();
    engine.set_position_limit(1, 1_000_000 * PRECISION);

    // -------------------------------
    // 场景2：纯吃单（taker）压测
    // -------------------------------
    println!(
        "\n=== Scenario 2: First, place 20,000 orders for maker, then place 100,000 orders for taker. ==="
    );

    // 先挂一些深度
    let mut rng = rand::thread_rng();
    for i in 1..=20_000 {
        let price_offset = i as u64;
        let price = if i % 2 == 0 {
            (50000 + price_offset) * PRECISION
        } else {
            (50000 - price_offset) * PRECISION
        };
        let _ = engine.submit(
            "BTCUSDT",
            i as u64,
            1,
            OrderType::Limit,
            if i % 2 == 0 { Side::Bid } else { Side::Ask },
            price,
            10 * PRECISION,
        );
    }

    run_benchmark(&mut engine, 100_000, "taker", |id, _| {
        let side = if id % 2 == 0 { Side::Ask } else { Side::Bid };
        (OrderType::Limit, side, 50000 * PRECISION, 1 * PRECISION)
    });

    // -------------------------------
    // 场景3：混合场景（50% maker + 50% taker）
    // -------------------------------
    println!("\n=== Scenario 3: 100,000 mixed orders of 50/50 ===");
    let mut engine = MatchingEngine::new();
    engine.set_position_limit(1, 1_000_000 * PRECISION);

    run_benchmark(&mut engine, 100_000, "mixed 50/50", |id, price| {
        let is_maker = id % 2 == 0;
        let otype = if is_maker {
            OrderType::PostOnly
        } else {
            OrderType::Limit
        };
        let side = if id % 3 == 0 { Side::Ask } else { Side::Bid };
        (otype, side, price, 1 * PRECISION)
    });

    println!("\nstress test completed");
}

fn run_benchmark<F>(engine: &mut MatchingEngine, count: usize, name: &str, order_factory: F)
where
    F: Fn(usize, u64) -> (OrderType, Side, u64, u64),
{
    let mut rng = rand::thread_rng();
    let mut times = Vec::with_capacity(count);

    let start = Instant::now();

    for i in 0..count {
        let order_id = (i + 1_000_000) as u64; // 避免 ID 冲突

        let base_price = 50000 + rng.gen_range(-200..=200) as u64;
        let price = base_price * PRECISION;

        let (otype, side, _, qty) = order_factory(i, base_price);

        let t0 = Instant::now();
        let result = engine.submit("BTCUSDT", order_id, 1, otype, side, price, qty);
        let duration = t0.elapsed();

        times.push(duration);

        if result.is_err() {
            eprintln!("Order {} fail: {:?}", order_id, result.err());
        }
    }

    let total_duration = start.elapsed();
    let qps = count as f64 / total_duration.as_secs_f64();

    times.sort_by_key(|d| d.as_nanos());
    let avg_ns = times.iter().map(|d| d.as_nanos()).sum::<u128>() as f64 / count as f64;
    let p50_ns = times[count / 2].as_nanos() as f64;
    let p99_ns = times[(count as f64 * 0.99) as usize].as_nanos() as f64;
    let p999_ns = times[(count as f64 * 0.999) as usize].as_nanos() as f64;

    println!("┌───────────────────────────────┐");
    println!("│ stress testing scenarios: {:<20} │", name);
    println!("├───────────────────────────────┤");
    println!("│ Order Quantity  : {:>10}  │", count);
    println!(
        "│ Total time      : {:>8.3} s  │",
        total_duration.as_secs_f64()
    );
    println!("│ Throughput (QPS : {:>10.0}  │", qps);
    println!("│ Average delay   : {:>8.1} μs │", avg_ns / 1000.0);
    println!("│ P50 Delay       : {:>8.1} μs │", p50_ns / 1000.0);
    println!("│ P99 Delay       : {:>8.1} μs │", p99_ns / 1000.0);
    println!("│ P99.9 Delay     : {:>8.1} μs │", p999_ns / 1000.0);
    println!("└───────────────────────────────┘");
}
