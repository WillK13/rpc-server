//! RPC client demonstrating use of the four operations.

use anyhow::{Result, anyhow};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt};
use tracing::{info, warn};
use uuid::Uuid;

use simple_rpc_rust::{RpcRequest, RpcResponse, read_frame, write_frame};

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
            request_id: Uuid::new_v4().to_string(),
            func: func.to_string(),
            params,
        };
        let v = serde_json::to_value(&req)?;
        write_frame(&mut self.sock, &v).await?;
        self.sock.flush().await?;

        // Read one response
        let resp_v = read_frame(&mut self.sock).await?;
        let resp: RpcResponse = serde_json::from_value(resp_v)?;
        match resp {
            RpcResponse::Completed { ok, result, error, .. } => {
                if ok {
                    Ok(result.unwrap_or(serde_json::json!(null)))
                } else {
                    Err(anyhow!(error.unwrap_or_else(|| "unknown server error".into())))
                }
            }
            RpcResponse::Error { error, .. } => Err(anyhow!(error)),
        }
    }

    pub async fn hash_compute(&mut self, data: &[u8]) -> Result<String> {
        let params = json!({
            "data_base64": B64.encode(data),
        });
        let v = self.call_raw("hash_compute", params).await?;
        Ok(v.get("hex").and_then(|x| x.as_str()).unwrap_or_default().to_string())
    }

    pub async fn sort_array(&mut self, values: Vec<i32>) -> Result<Vec<i32>> {
        let params = json!({
            "values": values,
        });
        let v = self.call_raw("sort_array", params).await?;
        let arr = v.get("values").ok_or_else(|| anyhow!("missing 'values' in response"))?;
        Ok(serde_json::from_value(arr.clone())?)
    }

    pub async fn matrix_multiply(&mut self, n: usize, a: Vec<f64>, b: Vec<f64>) -> Result<Vec<f64>> {
        let params = json!({
            "n": n,
            "a": a,
            "b": b,
        });
        let v = self.call_raw("matrix_multiply", params).await?;
        let arr = v.get("c").ok_or_else(|| anyhow!("missing 'c' in response"))?;
        Ok(serde_json::from_value(arr.clone())?)
    }

    pub async fn compress_data(&mut self, algo: &str, data: &[u8]) -> Result<Vec<u8>> {
        let params = json!({
            "algo": algo,
            "data_base64": B64.encode(data),
        });
        let v = self.call_raw("compress_data", params).await?;
        let s = v.get("compressed_base64").and_then(|x| x.as_str()).ok_or_else(|| anyhow!("missing 'compressed_base64'"))?;
        Ok(B64.decode(s.as_bytes())?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let addr = std::env::var("RPC_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    let mut client = RpcClient::connect(&addr).await?;
    info!("Connected to {addr}");

    // Demo calls
    let h = client.hash_compute(b"abc").await?;
    println!("hash('abc') = {h}");

    let sorted = client.sort_array(vec![3,1,-5,7,1]).await?;
    println!("sorted = {:?}", sorted);

    let a = vec![1.0, 2.0,
                 3.0, 4.0];
    let b = vec![5.0, 6.0,
                 7.0, 8.0];
    let c = client.matrix_multiply(2, a, b).await?;
    println!("matrix product = {:?}", c);

    let data = b"hello hello hello";
    let z = client.compress_data("zlib", data).await?;
    println!("zlib compressed len = {}", z.len());
    let l = client.compress_data("lz4", data).await?;
    println!("lz4  compressed len = {}", l.len());

    Ok(())
}
