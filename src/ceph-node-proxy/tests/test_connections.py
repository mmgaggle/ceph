import http.client
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator, Optional
from unittest.mock import patch
from urllib.error import HTTPError, URLError

import pytest

from ceph_node_proxy import connections
from ceph_node_proxy.connections import ConnectionPool


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: "Server"

    def setup(self) -> None:
        super().setup()
        with self.server.lock:
            self.server.connections += 1

    def log_message(self, *args: Any) -> None:
        pass

    def reply(self, status: int, body: bytes = b"{}", **headers: str) -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        for name, value in headers.items():
            self.send_header(name.replace("_", "-"), value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/missing":
            self.reply(404, b"nope")
        elif self.path == "/bye":
            # close without saying so: the client keeps a dead connection
            self.reply(200)
            self.close_connection = True
        elif self.path == "/close":
            self.reply(200, Connection="close")
        elif self.path == "/moved":
            self.reply(301, Location="/ok")
        else:
            self.reply(200, b'{"path": "%s"}' % self.path.encode())

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        self.server.posts.append(body)
        self.reply(201, body, X_Auth_Token="tok")


class Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.lock = threading.Lock()
        self.connections = 0
        self.posts: list = []
        self.closed = threading.Event()

    def shutdown_request(self, request: Any) -> None:
        super().shutdown_request(request)
        self.closed.set()


@pytest.fixture
def server() -> Iterator[Server]:
    srv = Server()
    thread = threading.Thread(target=srv.serve_forever, args=(0.05,), daemon=True)
    thread.start()
    yield srv
    srv.shutdown()
    srv.server_close()


def plain_pool(port: int) -> ConnectionPool:
    pool = ConnectionPool("127.0.0.1", str(port))

    def connect(timeout: Optional[int]) -> http.client.HTTPConnection:
        return http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)

    pool._connect = connect  # type: ignore[method-assign]
    return pool


def port_of(srv: Server) -> int:
    return srv.server_address[1]


def test_requests_share_one_connection(server: Server) -> None:
    pool = plain_pool(port_of(server))
    for path in ("/a", "/b", "/c"):
        headers, body, status = pool.request(endpoint=path, timeout=5)
        assert (status, body) == (200, '{"path": "%s"}' % path)
    assert server.connections == 1


def test_raw_body_is_bytes(server: Server) -> None:
    _, body, _ = plain_pool(port_of(server)).request(endpoint="/a", raw=True)
    assert body == b'{"path": "/a"}'


def test_post_data_and_headers(server: Server) -> None:
    pool = plain_pool(port_of(server))
    headers, body, status = pool.request(
        endpoint="/login",
        data='{"UserName": "u"}',
        headers={"Content-Type": "application/json"},
    )
    assert status == 201
    assert headers["X-Auth-Token"] == "tok"
    assert server.posts == [b'{"UserName": "u"}']


def test_error_status_raises_http_error_and_keeps_connection(server: Server) -> None:
    pool = plain_pool(port_of(server))
    with pytest.raises(HTTPError) as e:
        pool.request(endpoint="/missing")
    assert e.value.code == 404
    assert e.value.read() == b"nope"
    pool.request(endpoint="/a")
    assert server.connections == 1


def test_connection_closed_by_service_is_replaced(server: Server) -> None:
    pool = plain_pool(port_of(server))
    pool.request(endpoint="/bye")
    assert server.closed.wait(5)
    assert pool.request(endpoint="/a")[2] == 200
    assert server.connections == 2


def test_get_sent_again_when_the_close_was_not_seen(server: Server) -> None:
    pool = plain_pool(port_of(server))
    pool.request(endpoint="/bye")
    assert server.closed.wait(5)
    with patch.object(connections, "_closed_by_peer", return_value=False):
        assert pool.request(endpoint="/a")[2] == 200
    assert server.connections == 2


def test_post_not_sent_again(server: Server) -> None:
    pool = plain_pool(port_of(server))
    pool.request(endpoint="/bye")
    assert server.closed.wait(5)
    with patch.object(connections, "_closed_by_peer", return_value=False):
        with pytest.raises(URLError):
            pool.request(endpoint="/reboot", data="{}")
    assert server.posts == []


def test_connection_close_header_not_kept(server: Server) -> None:
    pool = plain_pool(port_of(server))
    pool.request(endpoint="/close")
    assert pool._idle == []
    pool.request(endpoint="/a")
    assert server.connections == 2


def test_redirect_followed_by_urllib(server: Server) -> None:
    pool = plain_pool(port_of(server))
    with patch.object(connections, "http_req", return_value=("h", "b", 200)) as req:
        assert pool.request(endpoint="/moved", timeout=5) == ("h", "b", 200)
    assert req.call_args.kwargs["endpoint"] == "/moved"


def test_unreachable_service_raises_url_error(server: Server) -> None:
    port = port_of(server)
    server.shutdown()
    server.server_close()
    with pytest.raises(URLError):
        plain_pool(port).request(endpoint="/a", timeout=2)


def test_concurrent_requests(server: Server) -> None:
    pool = plain_pool(port_of(server))
    results: list = []

    def get(n: int) -> None:
        for i in range(5):
            results.append(pool.request(endpoint=f"/{n}/{i}")[2])

    threads = [threading.Thread(target=get, args=(n,)) for n in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == [200] * 40
    assert server.connections <= 8
    assert len(pool._idle) <= connections.MAX_IDLE


def test_close_drops_idle_connections(server: Server) -> None:
    pool = plain_pool(port_of(server))
    pool.request(endpoint="/a")
    assert len(pool._idle) == 1
    pool.close()
    assert pool._idle == []
    pool.request(endpoint="/b")
    assert server.connections == 2
