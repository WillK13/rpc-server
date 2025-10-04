//! Open-loop load generator for the Simple RPC server.
//! Usage:
//!   cargo run --bin loadgen -- [addr] [rps] [duration_secs]
//! Example:
//!   cargo run --bin loadgen -- 127.0.0.1:8080 200 30
//!
//! Mixed workload (approx):
//!   - 50% hash_compute on 256B
//!   - 20% sort_array on 1k i32s
//!   - 10% matrix_multiply 16x16
//!   - 20% compress_data zlib on 512B
//!
//! Prints summary stats and writes CSV to results/loadgen.csv

use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::{interval, MissedTickBehavior};
use tokio::sync::mpsc;
use tracing::{info, warn};

// Minimal copy of the client to avoid cross-bin linking.
mod client_shim {
    pub use simple_rpc_rust::{RpcRequest, RpcResponse, read_frame, write_frame};
    pub use anyhow::{Result, anyhow};
    pub use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
    //pub use serde_json::json;
    pub use tokio::net::TcpStream;
    pub use tokio::io::AsyncWriteExt;
    pub use uuid::Uuid;

    pub struct RpcClient {
        sock: TcpStream,
    }
    impl RpcClient {
        pub async fn connect(addr: &str) -> Result<Self> {
            let sock = TcpStream::connect(addr).await?;
            sock.set_nodelay(true)?;
            Ok(Self { sock })
        }
                async fn call_raw(&mut self, func: &str, params: serde_json::Value) -> Result<serde_json::Value> {
            let req = RpcRequest {
                request_id: uuid::Uuid::new_v4().to_string(),
                func: func.to_string(),
                params,
            };
            let v = serde_json::to_value(&req)?;
            write_frame(&mut self.sock, &v).await?;
            self.sock.flush().await?;

            loop {
                let resp_v = read_frame(&mut self.sock).await?;
                let resp: RpcResponse = serde_json::from_value(resp_v)?;

                match resp {
                    RpcResponse::Accepted { .. } => {
                        // two-phase ack; keep waiting for the final result
                        continue;
                    }
                    RpcResponse::Completed { ok, result, error, .. } => {
                        if ok {
                            return Ok(result.unwrap_or(serde_json::json!(null)));
                        } else {
                            return Err(anyhow::anyhow!(error.unwrap_or_else(|| "server error".into())));
                        }
                    }
                    RpcResponse::Error { error, .. } => {
                        return Err(anyhow::anyhow!(error));
                    }
                }
            }
        }

        pub async fn hash_compute(&mut self, data: &[u8]) -> Result<String> {
            let params = serde_json::json!({ "data_base64": B64.encode(data) });
            let v = self.call_raw("hash_compute", params).await?;
            Ok(v.get("hex").and_then(|x| x.as_str()).unwrap_or_default().to_string())
        }
        pub async fn sort_array(&mut self, values: Vec<i32>) -> Result<Vec<i32>> {
            let params = serde_json::json!({ "values": values });
            let v = self.call_raw("sort_array", params).await?;
            let arr = v.get("values").ok_or_else(|| anyhow::anyhow!("missing values"))?;
            Ok(serde_json::from_value(arr.clone())?)
        }
        pub async fn matrix_multiply(&mut self, n: usize, a: Vec<f64>, b: Vec<f64>) -> Result<Vec<f64>> {
            let params = serde_json::json!({ "n": n, "a": a, "b": b });
            let v = self.call_raw("matrix_multiply", params).await?;
            let arr = v.get("c").ok_or_else(|| anyhow::anyhow!("missing c"))?;
            Ok(serde_json::from_value(arr.clone())?)
        }
        pub async fn compress_data(&mut self, algo: &str, data: &[u8]) -> Result<Vec<u8>> {
            let params = serde_json::json!({ "algo": algo, "data_base64": B64.encode(data) });
            let v = self.call_raw("compress_data", params).await?;
            let s = v.get("compressed_base64").and_then(|x| x.as_str()).ok_or_else(|| anyhow::anyhow!("missing compressed_base64"))?;
            Ok(B64.decode(s.as_bytes())?)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args: Vec<String> = env::args().collect();
    let addr = args.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1:8080");
    let rps: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100);
    let duration_secs: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(30);

    info!("Loadgen addr={addr} rps={rps} duration={duration_secs}s");

    // small pool of persistent connections; round-robin each request
    let pool_size = ((rps as f64).sqrt().ceil() as usize).max(4).min(64);
    let mut pool = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        pool.push(Arc::new(Mutex::new(client_shim::RpcClient::connect(addr).await?)));
    }

    // collect latencies (ms)
    let (tx, mut rx) = mpsc::unbounded_channel::<f64>();

    // open-loop ticker
    let mut tick = interval(Duration::from_nanos(1_000_000_000 / rps.max(1)));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let end_time = Instant::now() + Duration::from_secs(duration_secs);
    let mut i = 0usize;

    // deterministic RNG for the op mix
    let rng = Arc::new(tokio::sync::Mutex::new(StdRng::seed_from_u64(0xC0FFEE)));

    while Instant::now() < end_time {
        tick.tick().await;

        let cli = pool[i % pool_size].clone();
        i += 1;
        let txc = tx.clone();
        let rngc = rng.clone();

        tokio::spawn(async move {
            // choose operation
            let mut rng = rngc.lock().await;
            let p: f64 = rng.gen();
            drop(rng);

            let start = Instant::now();
            let res: Result<()> = async {
                let mut c = cli.lock().await;

                if p < 0.5 {
                    // 50% hash (256B)
                    let mut data = vec![0u8; 256];
                    for (i, b) in data.iter_mut().enumerate() {
                        *b = (i as u8).wrapping_mul(31).wrapping_add(7);
                    }
                    let _ = c.hash_compute(&data).await?;

                } else if p < 0.7 {
                    // 20% sort (1k ints) — SAFE MATH (no overflow)
                    let mut vals = vec![0i32; 1000];
                    for (i, v) in vals.iter_mut().enumerate() {
                        // do the LCG-like math in u64, then downcast
                        let x = ((i as u64 * 1_103_515_245u64 + 12_345u64) >> 8) as u32;
                        *v = (x as i32) ^ 0x5a5a5a5a;
                    }
                    let _ = c.sort_array(vals).await?;

                } else if p < 0.8 {
                    // 10% matmul 16x16
                    let n = 16usize;
                    let mut a = vec![0.0f64; n*n];
                    let mut b = vec![0.0f64; n*n];
                    for i in 0..n*n { a[i] = (i as f64).sin(); b[i] = (i as f64).cos(); }
                    let _ = c.matrix_multiply(n, a, b).await?;

                } else {
                    // 20% compress zlib (512B)
                    let mut data = vec![0u8; 512];
                    for (i, b) in data.iter_mut().enumerate() {
                        *b = (i as u8).wrapping_mul(17).wrapping_add(3);
                    }
                    let _ = c.compress_data("zlib", &data).await?;
                }

                Ok(())
            }.await;


            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            let _ = txc.send(elapsed);
            if let Err(e) = res {
                warn!("request error: {e}");
            }
        });
    }

    drop(tx);
    let mut lats = Vec::<f64>::new();
    while let Some(ms) = rx.recv().await { lats.push(ms); }
    lats.sort_by(|a,b| a.partial_cmp(b).unwrap());

    if lats.is_empty() {
        println!("No samples collected.");
        return Ok(());
    }

    fn pct(v: &Vec<f64>, p: f64) -> f64 {
        let n = v.len();
        let idx = ((p/100.0) * (n as f64 - 1.0)).round() as usize;
        v[idx]
    }
    let sum: f64 = lats.iter().copied().sum();
    let avg = sum / lats.len() as f64;
    let p50 = pct(&lats, 50.0);
    let p95 = pct(&lats, 95.0);
    let p99 = pct(&lats, 99.0);

    println!("samples={}, avg_ms={:.3}, p50={:.3}, p95={:.3}, p99={:.3}", lats.len(), avg, p50, p95, p99);

    std::fs::create_dir_all("results")?;
    let mut f = std::fs::File::create("results/loadgen.csv")?;
    use std::io::Write;
    writeln!(f, "latency_ms")?;
    for x in &lats { writeln!(f, "{:.6}", x)?; }
    println!("Wrote results/loadgen.csv");
    Ok(())
}
