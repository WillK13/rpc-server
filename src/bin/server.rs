//! RPC server exposing hash_compute, sort_array, matrix_multiply, compress_data.

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use flate2::{write::ZlibEncoder, Compression};
use hex::ToHex;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{info, warn};
use simple_rpc_rust::{RpcRequest, resp_ok, resp_err, resp_accepted, read_frame, write_frame};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let addr = std::env::var("RPC_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = TcpListener::bind(&addr).await?;
    info!("RPC server listening on {addr}");

    loop {
        let (sock, peer) = listener.accept().await?;
        info!("Accepted connection from {peer}");
        tokio::spawn(async move {
            if let Err(e) = handle_client(sock).await {
                warn!("Client {} closed with error: {e:#}", peer);
            } else {
                info!("Client {} closed", peer);
            }
        });
    }
}

async fn handle_client(sock: TcpStream) -> anyhow::Result<()> {
    // Split the socket into independent reader / writer halves
    let (mut rd, mut wr) = sock.into_split();

    // Channel for serialized writes from this connection
    let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();

    // Dedicated writer task: take frames from the channel and write them in order
    let _writer_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = write_frame(&mut wr, &msg).await {
                // Stop on write error (client disconnected, etc.)
                return Err::<(), anyhow::Error>(e.into());
            }
            if let Err(e) = wr.flush().await {
                return Err::<(), anyhow::Error>(e.into());
            }
        }
        Ok(())
    });

    // Main read/dispatch loop
    loop {
        let val = match read_frame(&mut rd).await {
            Ok(v) => v,
            Err(e) => {
                // EOF or framing/JSON error -> end this connection
                return Err(e.into());
            }
        };

        let req: RpcRequest = match serde_json::from_value(val) {
            Ok(r) => r,
            Err(e) => {
                // Cannot recover the request_id to respond; close connection
                tracing::error!("Malformed request: {e}");
                return Err(anyhow::anyhow!("malformed request"));
            }
        };

        // 1) Immediately acknowledge
        let _ = tx.send(resp_accepted(&req.request_id));

        // 2) Offload the work; when done, send Completed/Error
        let request_id = req.request_id.clone();
        let func = req.func.clone();
        let params = req.params.clone();
        let tx2 = tx.clone();

        tokio::spawn(async move {
            // Run the operation (matrix multiply can still use spawn_blocking inside)
            let res = match func.as_str() {
                "hash_compute" => op_hash_compute(params).await,
                "sort_array" => op_sort_array(params).await,
                "matrix_multiply" => op_matrix_multiply(params).await,
                "compress_data" => op_compress_data(params).await,
                other => Err(anyhow::anyhow!("unknown function '{other}'")),
            };

            // 3) Send the final result
            let frame = match res {
                Ok(okv) => resp_ok(&request_id, okv),
                Err(e) => resp_err(&request_id, e.to_string()),
            };
            let _ = tx2.send(frame); // ignore if the client went away
        });
    }

    // (Unreachable because we `return` on read error above, but if you
    // ever add a break: drop(tx); let _ = writer_task.await; Ok(())
}

// ---------- Operations ----------

#[derive(Deserialize)]
struct HashParams {
    /// Base64-encoded input bytes
    data_base64: String,
}
async fn op_hash_compute(params: serde_json::Value) -> Result<serde_json::Value> {
    let p: HashParams = serde_json::from_value(params)?;
    let data = B64.decode(p.data_base64.as_bytes())?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let digest = hasher.finalize();
    let hex = digest.encode_hex::<String>();
    Ok(serde_json::json!({ "hex": hex }))
}

#[derive(Deserialize)]
struct SortParams {
    values: Vec<i32>,
}
async fn op_sort_array(params: serde_json::Value) -> Result<serde_json::Value> {
    let mut p: SortParams = serde_json::from_value(params)?;
    p.values.sort_unstable();
    Ok(serde_json::json!({ "values": p.values }))
}

#[derive(Deserialize)]
struct MatMulParams {
    n: usize,
    a: Vec<f64>,
    b: Vec<f64>,
}
async fn op_matrix_multiply(params: serde_json::Value) -> Result<serde_json::Value> {
    let p: MatMulParams = serde_json::from_value(params)?;
    if p.n == 0 { return Err(anyhow!("n must be > 0")); }
    if p.a.len() != p.n * p.n || p.b.len() != p.n * p.n {
        return Err(anyhow!("a and b must be length n*n"));
    }
    // Offload heavy work to blocking thread
    let n = p.n;
    let a = p.a;
    let b = p.b;
    let c = tokio::task::spawn_blocking(move || {
        let mut c = vec![0.0f64; n * n];
        for i in 0..n {
            for k in 0..n {
                let aik = a[i * n + k];
                if aik == 0.0 { continue; }
                for j in 0..n {
                    c[i * n + j] += aik * b[k * n + j];
                }
            }
        }
        c
    }).await?;
    Ok(serde_json::json!({ "c": c }))
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Algo { Zlib, Lz4 }

#[derive(Deserialize)]
struct CompressParams {
    algo: Algo,
    data_base64: String,
}
async fn op_compress_data(params: serde_json::Value) -> Result<serde_json::Value> {
    let p: CompressParams = serde_json::from_value(params)?;
    let data = B64.decode(p.data_base64.as_bytes())?;
    let out = match p.algo {
        Algo::Zlib => {
            let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
            use std::io::Write;
            enc.write_all(&data)?;
            enc.finish()?
        },
        Algo::Lz4 => {
            lz4_flex::block::compress_prepend_size(&data)
        }
    };
    Ok(serde_json::json!({
        "compressed_base64": B64.encode(out)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD as B64, Engine as _};

    #[tokio::test]
    async fn test_hash_compute() {
        let data = B64.encode(b"abc");
        let out = op_hash_compute(serde_json::json!({ "data_base64": data })).await.unwrap();
        assert_eq!(out["hex"].as_str().unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    }

    #[tokio::test]
    async fn test_sort_array() {
        let out = op_sort_array(serde_json::json!({ "values": [3,1,-5,7,1] })).await.unwrap();
        assert_eq!(out["values"], serde_json::json!([-5,1,1,3,7]));
    }

    #[tokio::test]
    async fn test_matrix_multiply_2x2() {
        let out = op_matrix_multiply(serde_json::json!({
            "n": 2,
            "a": [1.0,2.0,3.0,4.0],
            "b": [5.0,6.0,7.0,8.0]
        })).await.unwrap();
        assert_eq!(out["c"], serde_json::json!([19.0,22.0,43.0,50.0]));
    }

    #[tokio::test]
    async fn test_compress_data_zlib() {
        let out = op_compress_data(serde_json::json!({
            "algo": "zlib",
            "data_base64": B64.encode(b"hello hello hello")
        })).await.unwrap();
        assert!(out["compressed_base64"].as_str().unwrap().len() > 0);
    }
}
