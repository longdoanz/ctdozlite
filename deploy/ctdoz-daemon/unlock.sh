#!/usr/bin/env bash
# Nhập mật khẩu vault cho ctdoz-daemon. Mật khẩu chỉ đi qua tmpfs /run/ctdoz (RAM của host);
# daemon đọc xong sẽ xoá ngay. Chạy lại sau mỗi lần container/server khởi động lại.
set -euo pipefail
cd "$(dirname "$0")"
if [[ "$(uname -s)" != Linux ]] || ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qx ctdoz-daemon; then
  # Not on the server: run the same script on the server over SSH (interactive, hidden input).
  HOST="${CTDOZ_SSH_HOST:?Đặt CTDOZ_SSH_HOST=<ssh host của server chạy daemon>}"
  [[ -z "${CTDOZ_FORWARDED:-}" ]] || { echo "Không thấy container ctdoz-daemon trên $HOST."; exit 1; }
  echo "Máy này ($(hostname -s)) không chạy ctdoz-daemon — chuyển sang $HOST qua SSH…"
  exec ssh -t "$HOST" 'cd ~/zalo-bot/deploy/ctdoz-daemon && CTDOZ_FORWARDED=1 ./unlock.sh'
fi
DIR=/run/ctdoz
sudo install -d -m 0770 -o root -g 10001 "$DIR"
first=0
sudo test -s data/vault.enc || first=1
read -rsp "Mật khẩu vault: " P1; echo
if (( first )); then
  echo "Chưa có vault: mật khẩu này sẽ tạo vault mới. MẤT MẬT KHẨU = MẤT DỮ LIỆU."
  read -rsp "Nhập lại: " P2; echo
  [[ "$P1" == "$P2" ]] || { echo "Không khớp."; exit 1; }
  unset P2
fi
(( ${#P1} >= 8 )) || { echo "Cần ít nhất 8 ký tự."; exit 1; }
(( ${#P1} >= 12 )) || echo "Lưu ý: mật khẩu ngắn (<12 ký tự) — nên trộn chữ hoa, thường, số, ký tự đặc biệt và không dùng từ có nghĩa."
umask 077
printf '%s' "$P1" | sudo tee "$DIR/passphrase.tmp" >/dev/null
unset P1
sudo chown root:10001 "$DIR/passphrase.tmp"; sudo chmod 0440 "$DIR/passphrase.tmp"
sudo mv "$DIR/passphrase.tmp" "$DIR/passphrase"
echo "Đã gửi mật khẩu. Kiểm tra: docker compose logs --tail 20 ctdoz-daemon"
for _ in $(seq 1 30); do sudo test -e "$DIR/passphrase" || break; sleep 1; done
sudo rm -f "$DIR/passphrase"
docker compose logs --tail 5 ctdoz-daemon 2>/dev/null | grep -E 'Vault|Sai mật khẩu|vault' || true
