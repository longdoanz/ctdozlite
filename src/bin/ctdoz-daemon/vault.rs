//! Encrypted-at-rest data directory.
//!
//! The library reads/writes plaintext files (`session.json`, `zalo_history.db`, …) in its working
//! directory. With the vault enabled that working directory lives on tmpfs (RAM only), and the
//! only thing written to persistent disk is `vault.enc`:
//!
//! ```text
//! "CTDZV2" | salt[16] | iterations u32 LE | nonce[12] | AES-256-GCM( gzip( archive ), aad = header )
//! archive = repeated [u32 path_len][path utf-8][u64 data_len][data]
//! ```
//!
//! SQLite databases are captured with `VACUUM INTO` so snapshots are consistent while the
//! library keeps writing. Directories whose name contains `cache` and `-wal`/`-shm`/`-journal`
//! files are skipped (caches are re-downloadable; WAL content is already in the VACUUM copy).
//!
//! The key is derived from a passphrase typed by the owner (PBKDF2-HMAC-SHA256, 600k rounds,
//! random salt kept in the header). Neither passphrase nor key is ever written to disk here.

use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 6] = b"CTDZV2";
const SALT_LEN: usize = 16;
const HEADER_LEN: usize = MAGIC.len() + SALT_LEN + 4;
pub const DEFAULT_ITERATIONS: u32 = 600_000;
pub const MIN_PASSPHRASE_CHARS: usize = 8;
const MAX_FILE: u64 = 512 * 1024 * 1024;

pub struct Vault {
    key: [u8; 32],
    salt: [u8; SALT_LEN],
    iterations: u32,
    vault_file: PathBuf,
    work_dir: PathBuf,
    last_digest: std::sync::Mutex<Option<[u8; 32]>>,
}

fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.map(|b| b ^ 0x36));
    for p in parts {
        inner.update(p);
    }
    let mut outer = Sha256::new();
    outer.update(k.map(|b| b ^ 0x5c));
    outer.update(inner.finalize());
    outer.finalize().into()
}

/// PBKDF2-HMAC-SHA256 with a 32-byte output (RFC 8018, single block).
pub fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut u = hmac_sha256(password, &[salt, &1u32.to_be_bytes()]);
    let mut out = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &[&u]);
        for (o, x) in out.iter_mut().zip(u.iter()) {
            *o ^= x;
        }
    }
    out
}

fn rand_bytes<const N: usize>() -> [u8; N] {
    use aes_gcm::aead::rand_core::RngCore;
    let mut b = [0u8; N];
    aes_gcm::aead::OsRng.fill_bytes(&mut b);
    b
}

fn header(salt: &[u8; SALT_LEN], iterations: u32) -> Vec<u8> {
    let mut h = Vec::with_capacity(HEADER_LEN);
    h.extend_from_slice(MAGIC);
    h.extend_from_slice(salt);
    h.extend_from_slice(&iterations.to_le_bytes());
    h
}

/// Read `(salt, iterations)` from a vault blob.
pub fn parse_header(blob: &[u8]) -> Result<([u8; SALT_LEN], u32)> {
    if blob.len() < HEADER_LEN + 12 + 16 || &blob[..MAGIC.len()] != MAGIC {
        bail!("vault.enc không đúng định dạng");
    }
    let salt: [u8; SALT_LEN] = blob[MAGIC.len()..MAGIC.len() + SALT_LEN].try_into()?;
    let iterations = u32::from_le_bytes(blob[MAGIC.len() + SALT_LEN..HEADER_LEN].try_into()?);
    if !(1..=50_000_000).contains(&iterations) {
        bail!("vault.enc có số vòng KDF không hợp lệ");
    }
    Ok((salt, iterations))
}

pub fn encrypt(key: &[u8; 32], salt: &[u8; SALT_LEN], iterations: u32, plain: &[u8]) -> Result<Vec<u8>> {
    use aes_gcm::aead::Payload;
    let cipher = Aes256Gcm::new_from_slice(key)?;
    let nonce = rand_bytes::<12>();
    let mut out = header(salt, iterations);
    let ct = cipher
        .encrypt(Nonce::from_slice(&nonce), Payload { msg: plain, aad: &out })
        .map_err(|_| anyhow::anyhow!("mã hoá thất bại"))?;
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ct);
    Ok(out)
}

pub fn decrypt(key: &[u8; 32], blob: &[u8]) -> Result<Vec<u8>> {
    use aes_gcm::aead::Payload;
    parse_header(blob)?;
    let cipher = Aes256Gcm::new_from_slice(key)?;
    let nonce = &blob[HEADER_LEN..HEADER_LEN + 12];
    cipher
        .decrypt(Nonce::from_slice(nonce), Payload { msg: &blob[HEADER_LEN + 12..], aad: &blob[..HEADER_LEN] })
        .map_err(|_| WrongPassphrase.into())
}

/// Decryption failed: wrong passphrase (or a tampered file).
#[derive(Debug)]
pub struct WrongPassphrase;

impl std::fmt::Display for WrongPassphrase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("sai mật khẩu vault (hoặc vault.enc đã bị sửa)")
    }
}

impl std::error::Error for WrongPassphrase {}

fn skip_dir(name: &str) -> bool {
    name.to_lowercase().contains("cache")
}

fn skip_file(name: &str) -> bool {
    name.ends_with("-wal") || name.ends_with("-shm") || name.ends_with("-journal") || name.ends_with(".vacuum-tmp")
}

fn is_sqlite(path: &Path) -> bool {
    let mut head = [0u8; 16];
    std::fs::File::open(path)
        .and_then(|mut f| f.read_exact(&mut head))
        .map(|_| &head == b"SQLite format 3\0")
        .unwrap_or(false)
}

/// Consistent copy of a live SQLite database.
fn sqlite_snapshot(path: &Path) -> Result<Vec<u8>> {
    let tmp = std::env::temp_dir().join(format!(
        "ctdoz-{}-{}.vacuum-tmp",
        std::process::id(),
        B64.encode(rand_bytes::<6>()).replace(['/', '+', '='], "_")
    ));
    let res = (|| {
        let conn = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        conn.execute("VACUUM INTO ?1", [tmp.to_string_lossy().as_ref()])?;
        drop(conn);
        Ok(std::fs::read(&tmp)?)
    })();
    let _ = std::fs::remove_file(&tmp);
    res
}

fn collect(root: &Path, dir: &Path, out: &mut Vec<(String, Vec<u8>)>) -> Result<()> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let name = e.file_name().to_string_lossy().to_string();
        let ft = e.file_type()?;
        let path = e.path();
        if ft.is_dir() {
            if !skip_dir(&name) {
                collect(root, &path, out)?;
            }
        } else if ft.is_file() && !skip_file(&name) {
            if e.metadata()?.len() > MAX_FILE {
                eprintln!("! vault: bỏ qua file quá lớn {}", path.display());
                continue;
            }
            let data = if is_sqlite(&path) {
                sqlite_snapshot(&path).with_context(|| format!("snapshot {}", path.display()))?
            } else {
                std::fs::read(&path)?
            };
            let rel = path.strip_prefix(root)?.to_string_lossy().replace('\\', "/");
            out.push((rel, data));
        }
    }
    Ok(())
}

pub fn pack(root: &Path) -> Result<Vec<u8>> {
    let mut files = Vec::new();
    collect(root, root, &mut files)?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    for (rel, data) in files {
        gz.write_all(&(rel.len() as u32).to_le_bytes())?;
        gz.write_all(rel.as_bytes())?;
        gz.write_all(&(data.len() as u64).to_le_bytes())?;
        gz.write_all(&data)?;
    }
    Ok(gz.finish()?)
}

/// Reject absolute paths and `..` so a crafted archive cannot escape `root`.
fn safe_rel(rel: &str) -> Result<PathBuf> {
    let p = Path::new(rel);
    if rel.is_empty() || p.components().any(|c| !matches!(c, Component::Normal(_))) {
        bail!("đường dẫn không hợp lệ trong vault: {rel:?}");
    }
    Ok(p.to_path_buf())
}

pub fn unpack(archive_gz: &[u8], root: &Path) -> Result<usize> {
    let mut raw = Vec::new();
    flate2::read::GzDecoder::new(archive_gz).read_to_end(&mut raw)?;
    let mut i = 0usize;
    let mut n = 0usize;
    let take = |i: &mut usize, len: usize| -> Result<&[u8]> {
        let end = i.checked_add(len).filter(|e| *e <= raw.len()).context("vault bị cắt cụt")?;
        let s = &raw[*i..end];
        *i = end;
        Ok(s)
    };
    while i < raw.len() {
        let plen = u32::from_le_bytes(take(&mut i, 4)?.try_into()?) as usize;
        let rel = String::from_utf8(take(&mut i, plen)?.to_vec())?;
        let dlen = u64::from_le_bytes(take(&mut i, 8)?.try_into()?) as usize;
        let data = take(&mut i, dlen)?;
        let dest = root.join(safe_rel(&rel)?);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest, data)?;
        set_private(&dest);
        n += 1;
    }
    Ok(n)
}

fn set_private(p: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(p, std::fs::Permissions::from_mode(0o600));
    }
    #[cfg(not(unix))]
    let _ = p;
}

impl Vault {
    /// Derive the key from `passphrase` and decrypt `vault_file` into `work_dir` (fresh start when
    /// the vault does not exist yet). Returns the vault and the number of restored files.
    pub fn open(passphrase: &str, vault_file: PathBuf, work_dir: PathBuf, iterations: u32) -> Result<(Self, usize)> {
        std::fs::create_dir_all(&work_dir)?;
        let existing = match std::fs::read(&vault_file) {
            Ok(b) => Some(b),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let (salt, iterations) = match &existing {
            Some(blob) => parse_header(blob)?,
            None => (rand_bytes::<SALT_LEN>(), iterations),
        };
        let key = pbkdf2_sha256(passphrase.as_bytes(), &salt, iterations);
        let v = Self { key, salt, iterations, vault_file, work_dir, last_digest: std::sync::Mutex::new(None) };
        let mut n = 0;
        if let Some(blob) = existing {
            let archive = decrypt(&v.key, &blob)?;
            n = unpack(&archive, &v.work_dir)?;
            *v.last_digest.lock().unwrap() = Some(Sha256::digest(&archive).into());
        }
        Ok((v, n))
    }

    /// Encrypt the work dir into `vault.enc` (atomic replace). Returns `false` when unchanged.
    pub fn snapshot(&self) -> Result<bool> {
        let archive = pack(&self.work_dir)?;
        let digest: [u8; 32] = Sha256::digest(&archive).into();
        if self.last_digest.lock().unwrap().as_ref() == Some(&digest) {
            return Ok(false);
        }
        let blob = encrypt(&self.key, &self.salt, self.iterations, &archive)?;
        let tmp = self.vault_file.with_extension("enc.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&blob)?;
            f.sync_all()?;
        }
        set_private(&tmp);
        std::fs::rename(&tmp, &self.vault_file)?;
        if let Some(dir) = self.vault_file.parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        *self.last_digest.lock().unwrap() = Some(digest);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ctdoz-vault-test-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn pbkdf2_matches_known_vectors() {
        let hex = |b: [u8; 32]| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
        assert_eq!(
            hex(pbkdf2_sha256(b"password", b"salt", 1)),
            "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
        );
        assert_eq!(
            hex(pbkdf2_sha256(b"password", b"salt", 2)),
            "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43"
        );
    }

    #[test]
    fn roundtrip_with_sqlite_and_skips() {
        let src = tmpdir("src");
        std::fs::write(src.join("session.json"), b"{\"secret\":1}").unwrap();
        std::fs::create_dir_all(src.join("accounts")).unwrap();
        std::fs::write(src.join("accounts/a.json"), b"x").unwrap();
        std::fs::create_dir_all(src.join("stickers_cache")).unwrap();
        std::fs::write(src.join("stickers_cache/big.png"), b"skip").unwrap();
        let conn = rusqlite::Connection::open(src.join("zalo_history.db")).unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE m(t TEXT); INSERT INTO m VALUES('xin chào');")
            .unwrap();

        let vault_file = tmpdir("store").join("vault.enc");
        let pass = "mật khẩu thử nghiệm dài";
        let (v, n0) = Vault::open(pass, vault_file.clone(), src.clone(), 1000).unwrap();
        assert_eq!(n0, 0);
        assert!(v.snapshot().unwrap());
        assert!(!v.snapshot().unwrap(), "unchanged snapshot must be skipped");
        drop(conn);

        let blob = std::fs::read(&vault_file).unwrap();
        assert!(!blob.windows(6).any(|w| w == b"secret"), "plaintext leaked");
        assert_eq!(parse_header(&blob).unwrap().1, 1000);

        let dst = tmpdir("dst");
        let (v2, n) = Vault::open(pass, vault_file.clone(), dst.clone(), 999_999).unwrap();
        assert_eq!(n, 3);
        assert_eq!(v2.iterations, 1000, "existing vault keeps its own KDF params");
        assert_eq!(std::fs::read(dst.join("session.json")).unwrap(), b"{\"secret\":1}");
        assert!(!dst.join("stickers_cache").exists());
        let c2 = rusqlite::Connection::open(dst.join("zalo_history.db")).unwrap();
        let t: String = c2.query_row("SELECT t FROM m", [], |r| r.get(0)).unwrap();
        assert_eq!(t, "xin chào");

        let err = Vault::open("sai mật khẩu rồi nhé", vault_file.clone(), tmpdir("wrong"), 1000)
            .err()
            .unwrap();
        assert!(err.downcast_ref::<WrongPassphrase>().is_some());

        // Header is authenticated: bumping the iteration count must break decryption.
        let mut tampered = blob.clone();
        tampered[MAGIC.len() + SALT_LEN] ^= 1;
        assert!(decrypt(&v2.key, &tampered).is_err());
    }

    #[test]
    fn rejects_path_traversal() {
        assert!(safe_rel("../etc/passwd").is_err());
        assert!(safe_rel("/etc/passwd").is_err());
        assert!(safe_rel("accounts/a.json").is_ok());
    }
}
