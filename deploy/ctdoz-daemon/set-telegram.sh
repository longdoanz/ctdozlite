#!/usr/bin/env bash
# Đặt token bot Telegram cho ctdoz-daemon mà không để token hiện ra màn hình hay lịch sử shell.
#   ./set-telegram.sh            nhập token mới (ẩn), kiểm tra bằng getMe, ghi vào .env
#   ./set-telegram.sh --groups   liệt kê các group/chat bot đã thấy (để lấy TELEGRAM_CHAT_ID)
#   ./set-telegram.sh --chat ID  đặt TELEGRAM_CHAT_ID và gửi tin thử
set -euo pipefail
cd "$(dirname "$0")"
ENV=.env
[[ -f $ENV ]] || { echo "Chưa có $ENV (cp .env.example .env)"; exit 1; }

set_var() { # set_var KEY VALUE: replace/append in .env, keep mode 600
  python3 -I - "$ENV" "$1" "$2" <<'PY'
import os, sys
path, key, val = sys.argv[1:]
lines = open(path).read().splitlines()
out, done = [], False
for l in lines:
    if l.split("=", 1)[0] == key:
        out.append(f"{key}={val}"); done = True
    else:
        out.append(l)
if not done:
    out.append(f"{key}={val}")
os.umask(0o077)
tmp = path + ".tmp"
open(tmp, "w").write("\n".join(out) + "\n")
os.chmod(tmp, 0o600)
os.replace(tmp, path)
PY
}

get_var() { grep -E "^$1=" "$ENV" | head -1 | cut -d= -f2-; }

api() { # api METHOD [curl args...]: token passed via config on stdin, never on the command line
  local tok; tok="$(get_var TELEGRAM_BOT_TOKEN)"
  [[ -n $tok ]] || { echo "Chưa có TELEGRAM_BOT_TOKEN"; exit 1; }
  local m=$1; shift
  printf 'url = "https://api.telegram.org/bot%s/%s"\n' "$tok" "$m" | curl -s -m 20 -K - "$@"
}

case "${1:-}" in
  --groups)
    api getUpdates -G --data-urlencode 'allowed_updates=["message","my_chat_member","channel_post"]' | python3 -I -c '
import json, sys
r = json.load(sys.stdin)
if not r.get("ok"):
    sys.exit("Lỗi: %s" % r.get("description"))
seen = {}
for u in r["result"]:
    for k in ("message", "my_chat_member", "channel_post", "edited_message"):
        c = (u.get(k) or {}).get("chat")
        if c:
            seen[c["id"]] = (c.get("type"), c.get("title") or c.get("username") or c.get("first_name"))
if not seen:
    print("Chưa thấy chat nào. Thêm bot vào group rồi gửi trong group: /start@<tên_bot>")
for cid, (t, name) in seen.items():
    print(f"{cid}\t{t}\t{name}")
'
    ;;
  --chat)
    [[ -n ${2:-} ]] || { echo "Dùng: $0 --chat <ID>"; exit 1; }
    set_var TELEGRAM_CHAT_ID "$2"
    api sendMessage --data-urlencode "chat_id=$2" --data-urlencode "text=✅ zalo-bot sẽ gửi cảnh báo vào đây" \
      | python3 -I -c 'import json,sys; r=json.load(sys.stdin); print("Gửi tin thử:", "OK" if r.get("ok") else r.get("description"))'
    echo "Áp dụng: docker compose up -d --force-recreate"
    ;;
  "")
    read -rsp "Token bot Telegram mới (ẩn): " TOK; echo
    [[ $TOK =~ ^[0-9]+:[A-Za-z0-9_-]{30,}$ ]] || { echo "Token không đúng định dạng."; exit 1; }
    name="$(printf 'url = "https://api.telegram.org/bot%s/getMe"\n' "$TOK" | curl -s -m 20 -K - \
      | python3 -I -c 'import json,sys; r=json.load(sys.stdin); print(r["result"]["username"] if r.get("ok") else "")')"
    [[ -n $name ]] || { unset TOK; echo "Telegram từ chối token này."; exit 1; }
    set_var TELEGRAM_BOT_TOKEN "$TOK"; unset TOK
    echo "Đã lưu token cho bot @$name vào $ENV."
    echo "Tiếp: thêm @$name vào group, gửi trong group: /start@$name — rồi chạy: $0 --groups"
    ;;
  *) echo "Dùng: $0 [--groups | --chat ID]"; exit 1 ;;
esac
