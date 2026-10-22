"""Local webhook receiver: python scripts/webhook_receiver.py."""

import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        self.send_response(int(os.environ.get("WEBHOOK_STATUS", "200")))
        self.end_headers()
        self.wfile.write(b"ok")


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
