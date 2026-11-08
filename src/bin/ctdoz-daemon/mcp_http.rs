//! Remote MCP endpoint (`POST /mcp`, JSON-RPC 2.0, Streamable-HTTP style JSON responses).
//!
//! Only this path is exposed (via Traefik); the upstream REST API stays on 127.0.0.1.
//! - Auth: `Authorization: Bearer $CTDOZ_MCP_TOKEN` (constant-time compare). No token → disabled.
//! - Bind: `$CTDOZ_MCP_BIND` (e.g. the host's Tailscale IP `100.x.y.z:17892`; never 0.0.0.0).
//! - Peer allowlist: `$CTDOZ_MCP_ALLOW_IPS` (comma-separated). Checked against the TCP peer
//!   address, never against `X-Forwarded-For` (there is no proxy in front).
//! - Policy (`automation.json` → `"mcp"`): read tools always on; `zalo_send_message` /
//!   `zalo_schedule_message` need `allow_send` and, if `send_thread_ids` is non-empty, a listed
//!   target; `zalo_configure_autoreply` writes need `allow_autoreply_config`. Sends share the
//!   daemon's per-minute limiter.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::daemon::Daemon;

const MAX_HEADER: usize = 16 * 1024;
const MAX_BODY: usize = 1024 * 1024;
const SUPPORTED_VERSIONS: &[&str] = &["2025-06-18", "2025-03-26", "2024-11-05"];
const SEND_TOOLS: &[&str] = &["zalo_send_message", "zalo_schedule_message"];

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpPolicy {
    #[serde(default)]
    pub allow_send: bool,
    /// Allowed targets for send/schedule; empty = any thread (only when `allow_send`).
    #[serde(default)]
    pub send_thread_ids: Vec<String>,
    #[serde(default)]
    pub allow_autoreply_config: bool,
}

/// `Err(reason)` when the policy forbids this tool call.
pub fn check_policy(p: &McpPolicy, tool: &str, args: &Value) -> Result<(), String> {
    if SEND_TOOLS.contains(&tool) {
        if !p.allow_send {
            return Err("Gửi tin qua MCP đang tắt (automation.json → mcp.allow_send)".into());
        }
        let to = args.get("to_id").and_then(Value::as_str).unwrap_or("");
        if !p.send_thread_ids.is_empty() && !p.send_thread_ids.iter().any(|t| t == to) {
            return Err(format!("Hội thoại {to} không nằm trong mcp.send_thread_ids"));
        }
    }
    if tool == "zalo_configure_autoreply" {
        let action = args.get("action").and_then(Value::as_str).unwrap_or("list");
        if action != "list" && !p.allow_autoreply_config {
            return Err("Sửa auto-reply qua MCP đang tắt (mcp.allow_autoreply_config)".into());
        }
    }
    Ok(())
}

fn tool_allowed_in_list(p: &McpPolicy, tool: &str) -> bool {
    !(SEND_TOOLS.contains(&tool) && !p.allow_send)
}

fn digest(s: &[u8]) -> [u8; 32] {
    Sha256::digest(s).into()
}

/// Constant-time token check (compares SHA-256 digests).
pub fn token_ok(expected: &[u8; 32], header: Option<&str>) -> bool {
    let Some(h) = header else { return false };
    let Some(tok) = h.strip_prefix("Bearer ").or_else(|| h.strip_prefix("bearer ")) else {
        return false;
    };
    let got = digest(tok.trim().as_bytes());
    got.iter().zip(expected.iter()).fold(0u8, |acc, (a, b)| acc | (a ^ b)) == 0
}

struct Request {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<Option<Request>> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i;
        }
        if buf.len() > MAX_HEADER {
            anyhow::bail!("header quá lớn");
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or("").split_whitespace();
    let method = first.next().unwrap_or("").to_string();
    let path = first.next().unwrap_or("").split('?').next().unwrap_or("").to_string();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    let len: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    if len > MAX_BODY {
        anyhow::bail!("body quá lớn");
    }
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < len {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(len);
    Ok(Some(Request { method, path, headers, body }))
}

async fn respond(stream: &mut TcpStream, status: &str, body: Option<&Value>, extra: &str) -> Result<()> {
    let payload = body.map(|b| b.to_string()).unwrap_or_default();
    let ctype = if body.is_some() { "Content-Type: application/json\r\n" } else { "" };
    let head = format!(
        "HTTP/1.1 {status}\r\n{ctype}Content-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n{extra}\r\n",
        payload.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(payload.as_bytes()).await?;
    stream.flush().await?;
    Ok(())
}

fn rpc_error(id: Value, code: i64, msg: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": msg } })
}

async fn handle_one(d: &Arc<Daemon>, msg: &Value, client: &str) -> Option<Value> {
    let id = msg.get("id").cloned();
    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    let policy = crate::daemon::load_config().mcp;
    let session = d.session.clone();

    if method == "tools/call" {
        let params = msg.get("params").cloned().unwrap_or(Value::Null);
        let tool = params.get("name").and_then(Value::as_str).unwrap_or("");
        let args = params.get("arguments").cloned().unwrap_or_else(|| json!({}));
        let target = args.get("to_id").and_then(Value::as_str).unwrap_or("-");
        if let Err(reason) = check_policy(&policy, tool, &args) {
            println!("🔌 MCP [{client}] {tool} → {target}: TỪ CHỐI ({reason})");
            return Some(json!({
                "jsonrpc": "2.0", "id": id.unwrap_or(Value::Null),
                "result": { "isError": true, "content": [{ "type": "text", "text": reason }] }
            }));
        }
        if SEND_TOOLS.contains(&tool) && !d.limiter.lock().await.allow() {
            println!("🔌 MCP [{client}] {tool} → {target}: vượt giới hạn gửi/phút");
            return Some(json!({
                "jsonrpc": "2.0", "id": id.unwrap_or(Value::Null),
                "result": { "isError": true, "content": [{ "type": "text", "text": "Vượt giới hạn gửi tin mỗi phút" }] }
            }));
        }
        println!("🔌 MCP [{client}] {tool} → {target}");
        if tool == "zalo_send_message" {
            let text = args.get("text").and_then(Value::as_str).unwrap_or("");
            d.remember_sent(target, text).await;
            // Remember what the AI said so replies to it can wake OpenClaw again.
            if let Ok(st) = zalo_rust::store::MessageStore::open(crate::daemon::DB_PATH) {
                if let Err(e) = crate::daemon::record_ai_sent(&st.conn, target, text, zalo_rust::login::unix_now()) {
                    println!("🔌 MCP: không lưu được tin AI đã gửi ({e})");
                }
            }
        }
        if SEND_TOOLS.contains(&tool) {
            // Every send is reported (not throttled like error alerts).
            let text = args.get("text").and_then(Value::as_str).unwrap_or("");
            let n = d.notifier.clone();
            let msg = format!(
                "🔌 MCP ({client}) {tool} → {target}: {}",
                crate::notify::truncate(text, 200)
            );
            tokio::spawn(async move { n.info(&msg).await });
        }
    }

    let mut resp = zalo_rust::api_mcp::handle_mcp_request(&session, msg, crate::daemon::DB_PATH).await?;

    if method == "initialize" {
        let want = msg.pointer("/params/protocolVersion").and_then(Value::as_str).unwrap_or("");
        let v = if SUPPORTED_VERSIONS.contains(&want) { want } else { SUPPORTED_VERSIONS[0] };
        if let Some(r) = resp.get_mut("result") {
            r["protocolVersion"] = json!(v);
            r["serverInfo"]["name"] = json!("zalo-bot");
        }
    }
    if method == "tools/list" {
        if let Some(tools) = resp.pointer_mut("/result/tools").and_then(Value::as_array_mut) {
            tools.retain(|t| tool_allowed_in_list(&policy, t.get("name").and_then(Value::as_str).unwrap_or("")));
        }
    }
    Some(resp)
}

async fn serve_conn(d: Arc<Daemon>, mut stream: TcpStream, expected: [u8; 32], allow: Arc<Vec<std::net::IpAddr>>) -> Result<()> {
    let peer_ip = stream.peer_addr().map(|a| a.ip()).ok();
    if !allow.is_empty() && !peer_ip.is_some_and(|ip| allow.contains(&ip)) {
        let who = peer_ip.map(|i| i.to_string()).unwrap_or_else(|| "?".into());
        d.notifier.alert("mcp-ip", format!("🔌 MCP chặn kết nối từ IP không được phép: {who}"));
        return respond(&mut stream, "403 Forbidden", None, "").await;
    }
    let Some(req) = tokio::time::timeout(Duration::from_secs(15), read_request(&mut stream)).await?? else {
        return Ok(());
    };
    let client = peer_ip.map(|i| i.to_string()).unwrap_or_else(|| "?".into());
    if req.path != "/mcp" && req.path != "/mcp/" {
        return respond(&mut stream, "404 Not Found", None, "").await;
    }
    if !token_ok(&expected, req.header("Authorization")) {
        d.notifier.alert("mcp-auth", format!("🔌 MCP từ chối request sai/thiếu token từ {client}"));
        return respond(
            &mut stream,
            "401 Unauthorized",
            Some(&json!({ "error": "unauthorized" })),
            "WWW-Authenticate: Bearer\r\n",
        )
        .await;
    }
    if req.method != "POST" {
        return respond(&mut stream, "405 Method Not Allowed", None, "Allow: POST\r\n").await;
    }
    let msg: Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(e) => {
            let err = rpc_error(Value::Null, -32700, &format!("Parse error: {e}"));
            return respond(&mut stream, "400 Bad Request", Some(&err), "").await;
        }
    };
    let out = match &msg {
        Value::Array(batch) => {
            let mut res = Vec::new();
            for m in batch {
                if let Some(r) = handle_one(&d, m, &client).await {
                    res.push(r);
                }
            }
            (!res.is_empty()).then(|| Value::Array(res))
        }
        m => handle_one(&d, m, &client).await,
    };
    match out {
        Some(v) => respond(&mut stream, "200 OK", Some(&v), "").await,
        None => respond(&mut stream, "202 Accepted", None, "").await,
    }
}

/// Start the endpoint if `CTDOZ_MCP_TOKEN` is set. Runs until aborted.
pub async fn serve(d: Arc<Daemon>) {
    let token = std::env::var("CTDOZ_MCP_TOKEN").unwrap_or_default();
    if token.trim().len() < 32 {
        println!("ℹ️  MCP từ xa tắt (CTDOZ_MCP_TOKEN chưa đặt hoặc ngắn hơn 32 ký tự)");
        return;
    }
    let expected = digest(token.trim().as_bytes());
    drop(token);
    let Ok(bind) = std::env::var("CTDOZ_MCP_BIND") else {
        println!("ℹ️  MCP từ xa tắt (chưa đặt CTDOZ_MCP_BIND)");
        return;
    };
    let allow: Arc<Vec<std::net::IpAddr>> = Arc::new(
        std::env::var("CTDOZ_MCP_ALLOW_IPS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|x| x.trim().parse().ok())
            .collect(),
    );
    if allow.is_empty() {
        d.notifier.alert("mcp-ip", "⚠️ CTDOZ_MCP_ALLOW_IPS trống — MCP nhận kết nối từ mọi IP tới được cổng này");
    }
    // Tailscale may come up after the container: retry the bind with backoff.
    let mut tries = 0u32;
    let listener = loop {
        match TcpListener::bind(&bind).await {
            Ok(l) => break l,
            Err(e) => {
                tries += 1;
                if tries >= 3 {
                    d.notifier.alert("mcp-bind", format!("Chưa mở được MCP tại {bind}: {e} — thử lại"));
                }
                tokio::time::sleep(crate::daemon::backoff_delay(tries.min(6))).await;
            }
        }
    };
    println!("🔌 MCP: http://{bind}/mcp (Bearer token, IP cho phép: {:?})", allow);
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let d2 = d.clone();
        let a2 = allow.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_conn(d2, stream, expected, a2).await {
                eprintln!("! MCP conn: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_check() {
        let exp = digest(b"s3cret-token-0123456789abcdef0123456789");
        assert!(token_ok(&exp, Some("Bearer s3cret-token-0123456789abcdef0123456789")));
        assert!(!token_ok(&exp, Some("Bearer wrong")));
        assert!(!token_ok(&exp, Some("s3cret-token-0123456789abcdef0123456789")));
        assert!(!token_ok(&exp, None));
    }

    #[test]
    fn policy_defaults_to_read_only() {
        let p = McpPolicy::default();
        assert!(check_policy(&p, "zalo_list_threads", &json!({})).is_ok());
        assert!(check_policy(&p, "zalo_send_message", &json!({"to_id":"1"})).is_err());
        assert!(check_policy(&p, "zalo_configure_autoreply", &json!({"action":"list"})).is_ok());
        assert!(check_policy(&p, "zalo_configure_autoreply", &json!({"action":"clear"})).is_err());
        assert!(!tool_allowed_in_list(&p, "zalo_send_message"));
    }

    #[test]
    fn policy_send_allowlist() {
        let p = McpPolicy { allow_send: true, send_thread_ids: vec!["g1".into()], allow_autoreply_config: false };
        assert!(check_policy(&p, "zalo_send_message", &json!({"to_id":"g1"})).is_ok());
        assert!(check_policy(&p, "zalo_schedule_message", &json!({"to_id":"g2"})).is_err());
    }
}
