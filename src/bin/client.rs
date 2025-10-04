use anyhow::{Result, anyhow};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde_json::json;
use tokio::net::TcpStream;
use tokio::{io::AsyncWriteExt, sync::{mpsc, Mutex}};
use std::{collections::HashMap, sync::Arc};
use tracing::{info, warn};
use uuid::Uuid;
use simple_rpc_rust::{RpcRequest, RpcResponse, read_frame, write_frame};

type PendingMap = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<RpcResponse>>>>;

pub struct RpcClient {
    writer: Arc<Mutex<TcpStream>>,
    pending: PendingMap,
}

impl RpcClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true)?;
        let writer = Arc::new(Mutex::new(sock));
        let reader = writer.clone();
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));

        let pending_clone = pending.clone();
        tokio::spawn(async move {
            let mut r = reader.lock().await;
            loop {
                let frame = match read_frame(&mut *r).await {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("reader loop ended: {e}");
                        let mut p = pending_clone.lock().await;
                        for (_, tx) in p.drain() {
                            let _ = tx.send(RpcResponse::Error {
                                request_id: "".into(), ok: false, error: "connection closed".into()
                            });
                        }
                        break;
                    }
                };
                let resp: RpcResponse = match serde_json::from_value(frame) {
                    Ok(x) => x,
                    Err(e) => { warn!("bad response json: {e}"); continue; }
                };

                let req_id = match &resp {
                    RpcResponse::Accepted { request_id, .. } => request_id.clone(),
                    RpcResponse::Completed { request_id, .. } => request_id.clone(),
                    RpcResponse::Error { request_id, .. } => request_id.clone(),
                };

                let mut p = pending_clone.lock().await;
                if let Some(tx) = p.get(&req_id) {
                    let _ = tx.send(resp);
                    // On Completed/Error, we’re done—remove the entry.
                    match tx.is_closed() {
                        _ => {
                            if matches!(resp, RpcResponse::Completed{..} | RpcResponse::Error{..}) {
                                p.remove(&req_id);
                            }
                        }
                    }
                }
            }
        });

        Ok(Self { writer, pending })
    }

    pub async fn call(&self, func: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let request_id = Uuid::new_v4().to_string();
        let req = RpcRequest { request_id: request_id.clone(), func: func.to_string(), params };
        let msg = serde_json::to_value(&req)?;

        // mpsc to receive both Accepted and Completed/Error
        let (tx, mut rx) = mpsc::unbounded_channel::<RpcResponse>();
        {
            let mut p = self.pending.lock().await;
            p.insert(request_id.clone(), tx);
        }

        {
            let mut w = self.writer.lock().await;
            write_frame(&mut *w, &msg).await?;
            w.flush().await?;
        }

        // Drain Accepted; wait for final
        loop {
            match rx.recv().await.ok_or_else(|| anyhow!("connection closed"))? {
                RpcResponse::Accepted { .. } => { /* ignore, keep waiting */ }
                RpcResponse::Completed { ok, result, error, .. } => {
                    if ok { return Ok(result.unwrap_or(serde_json::json!(null))); }
                    else { return Err(anyhow!(error.unwrap_or_else(|| "server error".into()))); }
                }
                RpcResponse::Error { error, .. } => return Err(anyhow!(error)),
            }
        }
    }

    // High-level wrappers
    pub async fn hash_compute(&self, data: &[u8]) -> Result<String> {
        let v = self.call("hash_compute", json!({ "data_base64": B64.encode(data) })).await?;
        Ok(v.get("hex").and_then(|x| x.as_str()).unwrap_or_default().to_string())
    }
    pub async fn sort_array(&self, values: Vec<i32>) -> Result<Vec<i32>> {
        let v = self.call("sort_array", json!({ "values": values })).await?;
        Ok(serde_json::from_value(v.get("values").cloned().ok_or_else(|| anyhow!("missing values"))?)?)
    }
    pub async fn matrix_multiply(&self, n: usize, a: Vec<f64>, b: Vec<f64>) -> Result<Vec<f64>> {
        let v = self.call("matrix_multiply", json!({ "n": n, "a": a, "b": b })).await?;
        Ok(serde_json::from_value(v.get("c").cloned().ok_or_else(|| anyhow!("missing c"))?)?)
    }
    pub async fn compress_data(&self, algo: &str, data: &[u8]) -> Result<Vec<u8>> {
        let v = self.call("compress_data", json!({ "algo": algo, "data_base64": B64.encode(data) })).await?;
        let s = v.get("compressed_base64").and_then(|x| x.as_str()).ok_or_else(|| anyhow!("missing compressed_base64"))?;
        Ok(B64.decode(s.as_bytes())?)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let addr = std::env::var("RPC_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".to_string());
    let cli = RpcClient::connect(&addr).await?;
    info!("Connected to {addr}");

    // sanity demo
    println!("hash('abc') = {}", cli.hash_compute(b"abc").await?);
    println!("sorted = {:?}", cli.sort_array(vec![3,1,-5,7,1]).await?);
    println!("matmul = {:?}", cli.matrix_multiply(2, vec![1.0,2.0,3.0,4.0], vec![5.0,6.0,7.0,8.0]).await?);
    println!("zlib len = {}", cli.compress_data("zlib", b"hello hello hello").await?.len());
    Ok(())
}
