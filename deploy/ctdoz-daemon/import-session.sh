#!/usr/bin/env bash
# Chuyển phiên Zalo đang chạy trên máy này (ví dụ bản GUI trên Mac) vào vault trên server,
# để không phải quét QR lại. Dữ liệu đi qua SSH và chỉ nằm trên tmpfs /run/ctdoz/import (RAM)
# cho đến khi daemon mã hoá vào vault rồi xoá.
#   ./import-session.sh <thư mục dữ liệu ctdozlite trên máy này>
# Sau khi nhập, KHÔNG mở lại bản GUI dùng cùng phiên đó (2 nơi cùng một phiên dễ bị Zalo chặn).
set -euo pipefail
SRC="${1:?Dùng: $0 <data-dir>}"
HOST="${CTDOZ_SSH_HOST:?Đặt CTDOZ_SSH_HOST=<ssh host của server chạy daemon>}"
REMOTE_DIR='~/zalo-bot/deploy/ctdoz-daemon'
[[ -f "$SRC/session.json" ]] || { echo "Không thấy $SRC/session.json"; exit 1; }
if pgrep -f "ctdozlite.*--data-dir[ =]$SRC" >/dev/null 2>&1; then
  echo "Bản ctdozlite đang chạy với $SRC — tắt nó trước."; exit 1
fi
files=(session.json)
for f in threads_cache.json zalo_history.db accounts; do [[ -e "$SRC/$f" ]] && files+=("$f"); done
echo "Dừng daemon trên $HOST…"
ssh "$HOST" "cd $REMOTE_DIR && docker compose stop >/dev/null"
echo "Gửi ${files[*]} → $HOST:/run/ctdoz/import (tmpfs)…"
COPYFILE_DISABLE=1 tar -C "$SRC" -cf - "${files[@]}" | ssh "$HOST" \
  'sudo install -d -m 0770 -o root -g 10001 /run/ctdoz && sudo rm -rf /run/ctdoz/import &&
   sudo install -d -m 0700 -o 10001 -g 10001 /run/ctdoz/import &&
   sudo tar -C /run/ctdoz/import --no-same-owner -xf - && sudo chown -R 10001:10001 /run/ctdoz/import &&
   sudo chmod -R go-rwx /run/ctdoz/import'
ssh "$HOST" "cd $REMOTE_DIR && docker compose up -d >/dev/null"
echo "Xong. Giờ mở khoá vault:"
exec ssh -t "$HOST" "cd $REMOTE_DIR && CTDOZ_FORWARDED=1 ./unlock.sh"
