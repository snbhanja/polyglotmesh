# Slower mock upstream that takes ~150ms to respond, to exercise the queue.
from http.server import BaseHTTPRequestHandler, HTTPServer
import json, sys, time

class H(BaseHTTPRequestHandler):
    def log_message(self, *a, **k): pass
    def _read_body(self):
        ln = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(ln) if ln else b""
    def do_POST(self):
        body = self._read_body()
        time.sleep(0.05)  # simulate work
        try:
            data = json.loads(body or b"{}")
        except Exception:
            data = {}
        model = data.get("model", "m")
        out = {
            "id": f"cmpl-{self.path}",
            "object": "chat.completion",
            "model": model,
            "upstream_id": sys.argv[2],
            "choices": [{"index": 0, "message": {"role": "assistant", "content": f"hello from {sys.argv[2]}"}, "finish_reason": "stop"}],
        }
        body = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b"ok")

if __name__ == "__main__":
    port = int(sys.argv[1])
    print(f"mock {sys.argv[2]} on {port}", flush=True)
    HTTPServer(("127.0.0.1", port), H).serve_forever()
