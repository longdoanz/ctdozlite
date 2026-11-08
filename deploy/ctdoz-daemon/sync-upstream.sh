#!/usr/bin/env bash
# Kéo bản mới từ upstream (ctdoteam1110/ctdozlite) rồi kiểm tra daemon vẫn build + test được.
# Phần custom chỉ nằm trong src/bin/ctdoz-daemon/ và deploy/ctdoz-daemon/ nên merge không đụng file upstream.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
git remote get-url upstream >/dev/null 2>&1 || git remote add upstream https://github.com/ctdoteam1110/ctdozlite
git fetch upstream
git merge --no-edit upstream/main
cargo test --locked --bin ctdoz-daemon
echo "OK: đã đồng bộ upstream và daemon vẫn build/test được."
