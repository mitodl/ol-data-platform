import threading
import urllib.error
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from notebook_publisher.cluster import _NO_REDIRECTS


class _Redirect(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(302)
        self.send_header("Location", "http://169.254.169.254/latest/meta-data/")
        self.end_headers()

    def log_message(self, *_: object) -> None:
        pass


@pytest.fixture
def redirecting_server() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), _Redirect)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/demo/health"
    server.shutdown()


def test_health_probe_does_not_follow_redirects(redirecting_server: str) -> None:
    with pytest.raises(urllib.error.HTTPError) as raised:
        _NO_REDIRECTS.open(redirecting_server, timeout=5)
    assert raised.value.code == 302
