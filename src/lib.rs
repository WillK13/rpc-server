//! Shared protocol types and helpers for the Simple RPC assignment.

use serde::{Deserialize, Serialize};
use bytes::{BytesMut, BufMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use thiserror::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcRequest {
    pub request_id: String,
    pub func: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum RpcResponse {
    Completed {
        request_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Error {
        request_id: String,
        ok: bool,
        error: String,
    },
}

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

/// Write a length-prefixed JSON message
pub async fn write_frame<W: AsyncWriteExt + Unpin>(mut w: W, v: &serde_json::Value) -> Result<(), ProtoError> {
    let bytes = serde_json::to_vec(v)?;
    let mut buf = BytesMut::with_capacity(4 + bytes.len());
    buf.put_u32(bytes.len() as u32);
    buf.extend_from_slice(&bytes);
    w.write_all(&buf).await?;
    Ok(())
}

/// Read a length-prefixed JSON message
pub async fn read_frame<R: AsyncReadExt + Unpin>(mut r: R) -> Result<serde_json::Value, ProtoError> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut data = vec![0u8; len];
    r.read_exact(&mut data).await?;
    let v = serde_json::from_slice(&data)?;
    Ok(v)
}

/// Convenience helpers for building responses
pub fn resp_ok(request_id: &str, result: serde_json::Value) -> serde_json::Value {
    serde_json::to_value(RpcResponse::Completed {
        request_id: request_id.to_string(),
        ok: true,
        result: Some(result),
        error: None,
    }).unwrap()
}

pub fn resp_err(request_id: &str, msg: impl AsRef<str>) -> serde_json::Value {
    serde_json::to_value(RpcResponse::Error {
        request_id: request_id.to_string(),
        ok: false,
        error: msg.as_ref().to_string(),
    }).unwrap()
}
