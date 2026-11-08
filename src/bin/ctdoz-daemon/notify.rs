//! Telegram alerts: errors, session expiry, QR re-login.
//!
//! Configured with env `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`; without them every call is a no-op.
//! The same alert key is sent at most once per `THROTTLE` window to avoid spamming.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Result};

const THROTTLE: Duration = Duration::from_secs(15 * 60);

#[derive(Clone)]
pub struct Notifier {
    inner: Option<Arc<Inner>>,
}

struct Inner {
    http: reqwest::Client,
    token: String,
    chat_id: String,
    host: String,
    last: Mutex<HashMap<String, Instant>>,
}

impl Notifier {
    pub fn from_env() -> Self {
        let token = std::env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default();
        let chat_id = std::env::var("TELEGRAM_CHAT_ID").unwrap_or_default();
        if token.trim().is_empty() || chat_id.trim().is_empty() {
            eprintln!("ℹ️  Chưa đặt TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID — tắt cảnh báo Telegram");
            return Self { inner: None };
        }
        let host = std::env::var("CTDOZ_HOST_LABEL")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "ctdoz-daemon".into());
        Self {
            inner: Some(Arc::new(Inner {
                http: reqwest::Client::builder()
                    .timeout(Duration::from_secs(20))
                    .build()
                    .expect("reqwest client"),
                token: token.trim().into(),
                chat_id: chat_id.trim().into(),
                host,
                last: Mutex::new(HashMap::new()),
            })),
        }
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    fn should_send(&self, key: &str) -> bool {
        let Some(inner) = &self.inner else { return false };
        let mut last = inner.last.lock().unwrap();
        let now = Instant::now();
        if last.get(key).is_some_and(|t| now.duration_since(*t) < THROTTLE) {
            return false;
        }
        last.insert(key.to_string(), now);
        true
    }

    /// Throttled alert (fire-and-forget). `key` groups repeated errors of the same kind.
    pub fn alert(&self, key: &str, text: impl Into<String>) {
        let text = text.into();
        eprintln!("⚠️  [{key}] {text}");
        if !self.should_send(key) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.send_text(&format!("⚠️ {text}")).await {
                eprintln!("! Telegram gửi lỗi: {e}");
            }
        });
    }

    /// Unthrottled informational message (awaited).
    pub async fn info(&self, text: &str) {
        println!("ℹ️  {text}");
        if let Err(e) = self.send_text(text).await {
            eprintln!("! Telegram gửi lỗi: {e}");
        }
    }

    pub async fn send_text(&self, text: &str) -> Result<()> {
        let Some(inner) = &self.inner else { return Ok(()) };
        let body = serde_json::json!({
            "chat_id": inner.chat_id,
            "text": format!("[{}] {}", inner.host, truncate(text, 3500)),
            "disable_web_page_preview": true,
        });
        let resp = inner
            .http
            .post(format!("https://api.telegram.org/bot{}/sendMessage", inner.token))
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e.without_url()))?;
        if !resp.status().is_success() {
            bail!("Telegram HTTP {}", resp.status());
        }
        Ok(())
    }

    /// Send a PNG (QR code) with a caption.
    pub async fn send_photo(&self, png: Vec<u8>, caption: &str) -> Result<()> {
        let Some(inner) = &self.inner else { return Ok(()) };
        let boundary = format!("ctdoz{:x}", rand_u64());
        let mut body = Vec::with_capacity(png.len() + 512);
        let field = |body: &mut Vec<u8>, name: &str, value: &str| {
            body.extend_from_slice(
                format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n")
                    .as_bytes(),
            );
        };
        field(&mut body, "chat_id", &inner.chat_id);
        field(&mut body, "caption", &format!("[{}] {}", inner.host, truncate(caption, 900)));
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"photo\"; filename=\"qr.png\"\r\nContent-Type: image/png\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(&png);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let resp = inner
            .http
            .post(format!("https://api.telegram.org/bot{}/sendPhoto", inner.token))
            .header("Content-Type", format!("multipart/form-data; boundary={boundary}"))
            .body(body)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e.without_url()))?;
        if !resp.status().is_success() {
            bail!("Telegram HTTP {}", resp.status());
        }
        Ok(())
    }
}

fn rand_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());
    h.finish()
}

pub fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        format!("{}…", s.chars().take(max).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_notifier_is_noop_and_throttle_off() {
        let n = Notifier { inner: None };
        assert!(!n.enabled());
        assert!(!n.should_send("x"));
    }

    #[test]
    fn throttle_blocks_repeat_keys() {
        let n = Notifier {
            inner: Some(Arc::new(Inner {
                http: reqwest::Client::new(),
                token: "t".into(),
                chat_id: "c".into(),
                host: "h".into(),
                last: Mutex::new(HashMap::new()),
            })),
        };
        assert!(n.should_send("a"));
        assert!(!n.should_send("a"));
        assert!(n.should_send("b"));
    }

    #[test]
    fn truncate_counts_chars() {
        assert_eq!(truncate("xin chào", 3), "xin…");
        assert_eq!(truncate("ok", 3), "ok");
    }
}
