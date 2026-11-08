//! `ctdoz-daemon` — headless Zalo bot built on the ctdozlite library.
//!
//! Lives entirely in `src/bin/ctdoz-daemon/` and only uses the library's public API, so
//! upstream updates merge without touching any upstream file (Cargo auto-discovers this bin).
//!
//! Usage: `ctdoz-daemon [--data-dir DIR] [--api-port PORT] [--proxy URL] [--qr] [--dev]`
//!        `ctdoz-daemon --vault FILE --work-dir TMPFS_DIR …`  (encrypted at rest, see vault.rs)

mod daemon;
mod mcp_http;
mod notify;
mod vault;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use zalo_rust::{data_dir, logger, login, qr, session::Session, session::SessionStatus};

use notify::Notifier;

fn arg_value(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
        .or_else(|| {
            let prefix = format!("{name}=");
            args.iter().find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        })
}

/// Render the QR payload to PNG bytes (for Telegram).
fn qr_png(q: &qr::QrLogin) -> Result<Vec<u8>> {
    use base64::{engine::general_purpose::STANDARD as B64, Engine};
    if let Some(b64) = q.base64_png.as_deref() {
        return Ok(B64.decode(b64.trim())?);
    }
    let payload = q.payload().context("QR không có payload")?;
    let code = qrcode::QrCode::new(payload.as_bytes())?;
    let w = code.width() as u32;
    let (scale, quiet) = (8u32, 4u32);
    let size = (w + quiet * 2) * scale;
    let colors = code.to_colors();
    let img = image::GrayImage::from_fn(size, size, |x, y| {
        let (mx, my) = (x / scale, y / scale);
        let dark = mx >= quiet
            && my >= quiet
            && mx < w + quiet
            && my < w + quiet
            && colors[((my - quiet) * w + (mx - quiet)) as usize] == qrcode::Color::Dark;
        image::Luma([if dark { 0 } else { 255 }])
    });
    let mut out = std::io::Cursor::new(Vec::new());
    img.write_to(&mut out, image::ImageFormat::Png)?;
    Ok(out.into_inner())
}

/// QR login; the code is printed to the terminal AND sent to Telegram. Retries until scanned.
async fn login_with_qr(s: &mut Session, notifier: &Notifier) -> Result<()> {
    // Same as upstream's terminal login: refresh server info / domains before asking for a QR.
    if let Ok(resp) = login::get_server_info(s).await {
        if let Some(d) = resp.get("data") {
            s.apply_server_info(d);
        }
    }
    for attempt in 1..=3u32 {
        let q = qr::request_qr(s, "vi").await.context("Không lấy được mã QR")?;
        if let Ok(art) = qr::render_terminal(&q) {
            println!("{art}");
        }
        println!("Quét mã QR bằng Zalo trên điện thoại (lần {attempt})…");
        if notifier.enabled() {
            match qr_png(&q) {
                Ok(png) => {
                    let caption = format!(
                        "🔑 Phiên Zalo cần đăng nhập lại. Quét QR này bằng app Zalo (hiệu lực ~{}s, lần {attempt}).",
                        q.qr_timeout
                    );
                    if let Err(e) = notifier.send_photo(png, &caption).await {
                        eprintln!("! Không gửi được QR qua Telegram: {e}");
                    }
                }
                Err(e) => eprintln!("! Không tạo được ảnh QR: {e}"),
            }
        }
        match qr::wait_for_scan(s, &q).await {
            Ok(_) => {}
            Err(e) if e.to_string().contains("hết thời gian") => continue,
            Err(e) => return Err(e),
        }
        let resp = qr::confirm(s, &q).await?;
        if let Some(sek) = qr::extract_zpw_sek(&resp) {
            s.set_zpw_sek(sek);
        }
        // Same order as the upstream GUI worker: apply the confirm payload first.
        if let Some(data) = resp.get("data") {
            login::apply_login_config(s, data);
        }
        let info = login::get_login_info(s).await?;
        // Do NOT run `extract_zpw_sek` on getLoginInfo: it falls back to `zpw_wpsk`, a different
        // token, and overwriting the real `zpw_sek` with it makes every later call fail (400/600).
        // Upstream's GUI only applies the config here, so do the same.
        let cfg = info.get("data").context("getLoginInfo không trả về data")?;
        login::apply_login_config(s, cfg);
        s.apply_server_info(cfg);
        // `apply_server_info` switches to per-account hosts (p*-msg-chat, tt-group-wpa…) but the
        // `zpw_sek` cookie was only installed for the old hosts → 400/600 everywhere. Re-install it.
        if let Some(sek) = s.zpw_sek.clone() {
            s.set_zpw_sek(sek);
        }
        let _ = s.sync_profile_me().await;
        s.save_to_file("session.json")?;
        flush_vault().await;
        let name = s.login.as_ref().map(|l| l.display_name.clone()).unwrap_or_default();
        notifier.info(&format!("✅ Đã đăng nhập Zalo: {name}")).await;
        return Ok(());
    }
    anyhow::bail!("Không ai quét QR sau 3 lần")
}

static VAULT: std::sync::OnceLock<Arc<vault::Vault>> = std::sync::OnceLock::new();

/// Persist the encrypted snapshot now (no-op without `--vault`).
async fn flush_vault() {
    if let Some(v) = VAULT.get().cloned() {
        match tokio::task::spawn_blocking(move || v.snapshot()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => eprintln!("! vault snapshot lỗi: {e:#}"),
            Err(e) => eprintln!("! vault snapshot panic: {e}"),
        }
    }
}

/// Wait for the owner's passphrase (written by `unlock.sh` into a RAM-backed file), open the
/// vault, and delete the passphrase file immediately. A wrong passphrase is reported and the
/// daemon keeps waiting; it never starts over an undecryptable vault.
async fn unlock_vault(notifier: &Notifier, vault_file: &str, work: &str) -> Result<(vault::Vault, usize)> {
    let pass_file = std::env::var("CTDOZ_PASSPHRASE_FILE")
        .ok()
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "/run/ctdoz/passphrase".into());
    let iterations = std::env::var("CTDOZ_KDF_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(vault::DEFAULT_ITERATIONS);
    let mut waited = false;
    loop {
        let pass = match std::fs::read_to_string(&pass_file) {
            Ok(p) => {
                let _ = std::fs::remove_file(&pass_file);
                p.trim_end_matches(['\r', '\n']).to_string()
            }
            Err(_) => {
                if !waited {
                    notifier.alert(
                        "vault-locked",
                        "🔒 Vault đang khoá — SSH vào server và chạy ./unlock.sh để nhập mật khẩu",
                    );
                    waited = true;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
                continue;
            }
        };
        if pass.chars().count() < vault::MIN_PASSPHRASE_CHARS {
            notifier.alert("vault-pass", format!("Mật khẩu vault phải dài ít nhất {} ký tự", vault::MIN_PASSPHRASE_CHARS));
            continue;
        }
        let (vf, wd) = (PathBuf::from(vault_file), PathBuf::from(work));
        let res = tokio::task::spawn_blocking(move || vault::Vault::open(&pass, vf, wd, iterations)).await?;
        match res {
            Ok(ok) => return Ok(ok),
            Err(e) if e.downcast_ref::<vault::WrongPassphrase>().is_some() => {
                // Not throttled per key: every wrong attempt should be visible.
                let _ = notifier.send_text("⛔ Sai mật khẩu vault — chạy lại ./unlock.sh").await;
                eprintln!("! {e}");
            }
            Err(e) => return Err(e),
        }
    }
}

/// Decrypt the vault into the tmpfs work dir and start periodic snapshots + stop-file watcher.
async fn open_vault(args: &[String], notifier: &Notifier) -> Result<Option<Vec<String>>> {
    let Some(vault_file) = arg_value(args, "--vault") else {
        return Ok(None);
    };
    let work = arg_value(args, "--work-dir").context("--vault cần --work-dir (thư mục tmpfs)")?;
    // Graceful stop without extra deps: the entrypoint touches this file on SIGTERM.
    let stop_file = std::env::var("CTDOZ_STOP_FILE").unwrap_or_else(|_| "/tmp/ctdoz.stop".into());
    let _ = std::fs::remove_file(&stop_file);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if std::path::Path::new(&stop_file).exists() {
                println!("⏹  Nhận yêu cầu dừng — đang ghi vault…");
                flush_vault().await;
                std::process::exit(0);
            }
        }
    });
    let (v, n) = unlock_vault(notifier, &vault_file, &work).await.context("Không mở được vault")?;
    let v = Arc::new(v);
    println!("🔐 Vault: đã giải mã {n} file vào {work}");
    // One-shot import (e.g. a session moved from another machine by import-session.sh). The
    // directory lives on host tmpfs (/run/ctdoz) so plaintext never touches disk.
    let import_dir = PathBuf::from(
        std::env::var("CTDOZ_IMPORT_DIR").unwrap_or_else(|_| "/run/ctdoz/import".into()),
    );
    if import_dir.is_dir() {
        match import_files(&import_dir, &PathBuf::from(&work)) {
            Ok(k) => {
                let _ = std::fs::remove_dir_all(&import_dir);
                let v2 = v.clone();
                tokio::task::spawn_blocking(move || v2.snapshot()).await??;
                notifier.info(&format!("📥 Đã nhập {k} file vào vault (đã mã hoá, xoá bản tạm)")).await;
            }
            Err(e) => notifier.alert("import", format!("Nhập dữ liệu thất bại: {e:#}")),
        }
    }
    let _ = VAULT.set(v);

    let interval = std::env::var("CTDOZ_VAULT_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30u64);
    let n2 = notifier.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval));
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Some(v) = VAULT.get().cloned() {
                if let Ok(Err(e)) = tokio::task::spawn_blocking(move || v.snapshot()).await {
                    n2.alert("vault", format!("Không ghi được vault.enc: {e:#}"));
                }
            }
        }
    });
    // Point the library at the work dir.
    let mut lib_args: Vec<String> = Vec::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == "--data-dir" {
            it.next();
        } else if !a.starts_with("--data-dir=") {
            lib_args.push(a.clone());
        }
    }
    lib_args.push("--data-dir".into());
    lib_args.push(work);
    Ok(Some(lib_args))
}

/// Copy every regular file under `src` into `dst` (overwrite), returning the count.
fn import_files(src: &std::path::Path, dst: &std::path::Path) -> Result<usize> {
    let mut n = 0;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let ft = e.file_type()?;
        let target = dst.join(e.file_name());
        if ft.is_dir() {
            std::fs::create_dir_all(&target)?;
            n += import_files(&e.path(), &target)?;
        } else if ft.is_file() {
            std::fs::copy(e.path(), &target)?;
            n += 1;
        }
    }
    Ok(n)
}

/// Load / validate the saved session; QR login when needed.
async fn ensure_session(args: &[String], notifier: &Notifier, force_qr: bool) -> Result<Session> {
    let dev = args.iter().any(|a| a == "--dev");
    let mut s = Session::new(dev, false, false)?;
    if let Some(p) = arg_value(args, "--proxy") {
        s.set_proxy(Some(&p))?;
    }
    let restored = s.load_from_file("session.json").is_ok() && s.login.is_some();
    let mut need_login = force_qr || !restored || s.is_expired_marked();
    if !need_login {
        let mut unreachable = 0u32;
        loop {
            match s.session_status().await {
                SessionStatus::Valid => break,
                SessionStatus::Expired => {
                    s.mark_expired();
                    let _ = s.save_to_file("session.json");
                    need_login = true;
                    break;
                }
                SessionStatus::Unreachable => {
                    unreachable += 1;
                    let wait = Duration::from_secs((60u64 << unreachable.min(4)).min(900));
                    notifier.alert(
                        "unreachable",
                        format!("Không kết nối được máy chủ Zalo — thử lại sau {}s", wait.as_secs()),
                    );
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }
    if need_login {
        notifier.alert("session-expired", "Phiên Zalo hết hạn hoặc chưa đăng nhập — đang tạo QR");
        login_with_qr(&mut s, notifier).await?;
    }
    Ok(s)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let notifier = Notifier::from_env();
    let lib_args = match open_vault(&args, &notifier).await {
        Ok(a) => a.unwrap_or_else(|| args.clone()),
        Err(e) => {
            // Fail closed: never start with an empty store over an undecryptable vault.
            let _ = notifier.send_text(&format!("⛔ Không mở được vault dữ liệu: {e:#}")).await;
            return Err(e);
        }
    };
    data_dir::initialize(&lib_args)?;
    logger::init(args.iter().any(|a| a == "--dev"));
    let api_port = arg_value(&args, "--api-port")
        .and_then(|v| v.parse().ok())
        .unwrap_or(zalo_rust::api_mcp::DEFAULT_API_PORT);

    {
        let n = notifier.clone();
        std::panic::set_hook(Box::new(move |info| {
            let msg = format!("💥 ctdoz-daemon panic: {info}");
            eprintln!("{msg}");
            // Best effort from a non-async context.
            let n = n.clone();
            let _ = std::thread::spawn(move || {
                if let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
                    let _ = rt.block_on(n.send_text(&msg));
                }
            })
            .join();
        }));
    }

    let mut force_qr = args.iter().any(|a| a == "--qr");
    let mut crashes = 0u32;
    loop {
        let session = match ensure_session(&args, &notifier, force_qr).await {
            Ok(s) => s,
            Err(e) => halt(&notifier, format!("Đăng nhập Zalo thất bại: {e:#}")).await,
        };
        force_qr = false;
        let started = Instant::now();
        match daemon::run(session, api_port, notifier.clone()).await {
            Ok(daemon::Exit::SessionExpired) if started.elapsed() >= MIN_HEALTHY_SESSION => {
                notifier.alert("session-expired", "Phiên Zalo đã hết hạn — đang gửi QR để đăng nhập lại");
                force_qr = true;
            }
            Ok(daemon::Exit::SessionExpired) => {
                halt(
                    &notifier,
                    format!(
                        "Zalo từ chối phiên chỉ sau {} phút chạy — nhiều khả năng là lỗi giao thức, không tự đăng nhập lại",
                        started.elapsed().as_secs() / 60
                    ),
                )
                .await
            }
            Ok(daemon::Exit::Halted(reason)) => halt(&notifier, reason).await,
            Err(e) => {
                crashes += 1;
                if crashes >= 3 {
                    halt(&notifier, format!("Daemon lỗi {crashes} lần liên tiếp: {e:#}")).await;
                }
                let wait = daemon::backoff_delay(crashes + 3);
                notifier.alert(
                    "daemon-crash",
                    format!("Daemon dừng do lỗi: {e:#} — khởi động lại sau {}s", wait.as_secs()),
                );
                tokio::time::sleep(wait).await;
            }
        }
    }
}

/// A session must survive this long before an expiry is treated as normal (→ auto re-login).
const MIN_HEALTHY_SESSION: Duration = Duration::from_secs(30 * 60);

/// Stop all traffic to Zalo, persist the vault and idle until a human restarts the container.
async fn halt(notifier: &Notifier, reason: String) -> ! {
    let msg = format!(
        "⛔ zalo-bot đã TỰ DỪNG để tránh spam request lên Zalo.\nLý do: {reason}\nKiểm tra log rồi chạy lại: docker compose restart && ./unlock.sh"
    );
    eprintln!("{msg}");
    let _ = notifier.send_text(&msg).await;
    flush_vault().await;
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}
