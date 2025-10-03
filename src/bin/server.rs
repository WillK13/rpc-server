//! RPC server exposing hash_compute, sort_array, matrix_multiply, compress_data.

use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use flate2::{write::ZlibEncoder, Compression};
use hex::ToHex;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt};
use tracing::{error, info, warn};
use simple_rpc_rust::{RpcRequest, resp_ok, resp_err, read_frame, write_frame};

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

async fn handle_client(mut sock: TcpStream) -> Result<()> {
    loop {
        let val = match read_frame(&mut sock).await {
            Ok(v) => v,
            Err(e) => {
                // EOF is also an error here; just break
                return Err(e.into());
            }
        };

        let req: RpcRequest = match serde_json::from_value(val) {
            Ok(r) => r,
            Err(e) => {
                // Cannot recover request_id; close socket after logging
                error!("Malformed request JSON: {e}");
                return Err(anyhow!("malformed request"));
            }
        };

        let request_id = req.request_id.clone();
        let func = req.func.clone();
        let params = req.params.clone();

        let res = match func.as_str() {
            "hash_compute" => op_hash_compute(params).await,
            "sort_array" => op_sort_array(params).await,
            "matrix_multiply" => op_matrix_multiply(params).await,
            "compress_data" => op_compress_data(params).await,
            other => Err(anyhow!("unknown function '{}'", other)),
        };

        let resp = match res {
            Ok(okv) => resp_ok(&request_id, okv),
            Err(e) => resp_err(&request_id, e.to_string()),
        };

        if let Err(e) = write_frame(&mut sock, &resp).await {
            return Err(anyhow!("write response: {e}"));
        }
        sock.flush().await?;
    }
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
