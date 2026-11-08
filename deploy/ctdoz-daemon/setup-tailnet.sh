#!/usr/bin/env bash
# Ghi IP Tailscale thật vào .env và automation.json (không hardcode IP trong repo).
#   ./setup-tailnet.sh <tên-máy-openclaw-trên-tailnet>
set -euo pipefail
cd "$(dirname "$0")"
PEER="${1:?Dùng: $0 <tên-máy-openclaw-trên-tailnet>}"
SELF_IP="$(tailscale ip -4 | head -1)"
PEER_IP="$(tailscale ip -4 "$PEER" | head -1)"
[[ $SELF_IP == 100.* && $PEER_IP == 100.* ]] || { echo "Không lấy được IP Tailscale (self=$SELF_IP peer=$PEER_IP)"; exit 1; }
python3 -I - "$SELF_IP" "$PEER_IP" <<'PY'
import json, os, sys
self_ip, peer_ip = sys.argv[1:]
p = ".env"; keep = [l for l in open(p).read().splitlines() if not l.startswith(("CTDOZ_MCP_BIND=", "CTDOZ_MCP_ALLOW_IPS="))]
keep += [f"CTDOZ_MCP_BIND={self_ip}:17892", f"CTDOZ_MCP_ALLOW_IPS={peer_ip}"]
os.umask(0o077); open(p + ".tmp", "w").write("\n".join(keep) + "\n"); os.chmod(p + ".tmp", 0o600); os.replace(p + ".tmp", p)
c = "config/automation.json"; d = json.load(open(c))
oc = d.setdefault("openclaw", {}); oc["url"] = f"http://{peer_ip}:18789/hooks/agent"; oc.setdefault("agent_id", "main"); oc.setdefault("max_per_thread_per_minute", 3)
json.dump(d, open(c, "w"), ensure_ascii=False, indent=2)
print(f"OK: MCP nghe {self_ip}:17892, chỉ nhận {peer_ip}; OpenClaw hook = http://{peer_ip}:18789/hooks/agent")
PY
