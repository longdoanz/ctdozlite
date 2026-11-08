"""Minimal webhook receiver for ctdoz-daemon (stdlib only). Run: python3 -I webhook_example.py

ctdoz-daemon POSTs JSON: {event, thread_id, thread_name, is_group, msg_id, sender_id,
sender_name, text, ts, mentioned_me, reply_to_text, trigger}
Answer {"reply": "..."} to reply in the same thread,
{"messages": [{"thread_id": "...", "is_group": true, "text": "..."}]} to send elsewhere,
or {} to stay silent. Replace handle() with a call to your AI.
"""
import hmac
import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

SECRET = os.environ.get("CTDOZLITE_WEBHOOK_SECRET", "")


def handle(ev: dict) -> dict:
    if ev.get("trigger") == "ai-command":
        prompt = ev["text"].strip()[len("/ai"):].strip()
        return {"reply": f"(AI sẽ trả lời: {prompt!r})"}
    return {}


class H(BaseHTTPRequestHandler):
    def do_POST(self):
        got = self.headers.get("X-Ctdozlite-Secret", "")
        if SECRET and not hmac.compare_digest(got, SECRET):
            self.send_response(401)
            self.end_headers()
            return
        ev = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        print(json.dumps(ev, ensure_ascii=False), flush=True)
        body = json.dumps(handle(ev), ensure_ascii=False).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", 18080), H).serve_forever()
