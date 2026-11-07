"""Keep node-proxy's HTTPS connections to a Redfish service open.

urllib opens a connection and does a TLS handshake for every request. An
update cycle makes over a hundred requests to the same service, and the
handshakes cost both ends more CPU than the requests: against HAL, one
connection per request took twice the service's CPU and five times
node-proxy's. ConnectionPool keeps the connections the service leaves open
and reuses them.
"""

import http.client
import io
import selectors
import ssl
import threading
from http.client import HTTPMessage
from typing import Any, List, Mapping, Optional, Tuple
from urllib.error import HTTPError, URLError

from ceph_node_proxy.util import get_logger, http_req

#: the methods sent again, on a new connection, when the service had
#: closed the kept one: they change nothing more when repeated
IDEMPOTENT = frozenset(("GET", "HEAD", "PUT", "DELETE", "OPTIONS"))
#: what sending on a connection the service has closed raises
CLOSED = (
    ConnectionError,
    http.client.BadStatusLine,
    ssl.SSLEOFError,
    ssl.SSLZeroReturnError,
)
#: idle connections kept: as many as the update cycle has requests in flight
MAX_IDLE = 8


def _closed_by_peer(conn: http.client.HTTPConnection) -> bool:
    """Whether the service closed (or wrote on) an idle connection: an idle
    connection has nothing to read unless the service is done with it."""
    if conn.sock is None:
        return True
    with selectors.DefaultSelector() as selector:
        selector.register(conn.sock, selectors.EVENT_READ)
        return bool(selector.select(0))


class ConnectionPool:
    """Keep-alive HTTPS connections to one service, shared by the threads
    of node-proxy; request() answers as util.http_req() does."""

    def __init__(self, host: str, port: str) -> None:
        self.host = host
        self.port = port
        self.log = get_logger(__name__)
        self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._context.check_hostname = False
        self._context.verify_mode = ssl.CERT_NONE
        self._idle: List[http.client.HTTPConnection] = []
        self._lock = threading.Lock()

    def _connect(self, timeout: Optional[int]) -> http.client.HTTPConnection:
        return http.client.HTTPSConnection(
            self.host, int(self.port), timeout=timeout, context=self._context
        )

    def _take(self, timeout: Optional[int]) -> Tuple[http.client.HTTPConnection, bool]:
        """An idle connection the service still has open, else a new one;
        and whether it is a kept one."""
        while True:
            with self._lock:
                conn = self._idle.pop() if self._idle else None
            if conn is None:
                return self._connect(timeout), False
            if _closed_by_peer(conn):
                conn.close()
                continue
            conn.timeout = timeout
            if conn.sock is not None:
                conn.sock.settimeout(timeout)
            return conn, True

    def _give_back(self, conn: http.client.HTTPConnection) -> None:
        with self._lock:
            if len(self._idle) < MAX_IDLE:
                self._idle.append(conn)
                return
        conn.close()

    def close(self) -> None:
        with self._lock:
            idle, self._idle = self._idle, []
        for conn in idle:
            conn.close()

    def _send(
        self,
        conn: http.client.HTTPConnection,
        method: str,
        endpoint: str,
        headers: Mapping[str, str],
        body: Optional[bytes],
    ) -> Tuple[http.client.HTTPResponse, bytes]:
        conn.request(method, endpoint, body=body, headers=dict(headers))
        response = conn.getresponse()
        return response, response.read()

    def request(
        self,
        method: Optional[str] = None,
        endpoint: str = "/",
        headers: Mapping[str, str] = {},
        data: Optional[str] = None,
        timeout: Optional[int] = None,
        raw: bool = False,
    ) -> Tuple[HTTPMessage, Any, int]:
        """(headers, body, status) of the request; the body is bytes when
        raw is set, else text. A status other than 2xx raises HTTPError, a
        connection that fails URLError, as urllib does."""
        method = method or ("POST" if data else "GET")
        body = data.encode() if data else None
        conn, kept = self._take(timeout)
        try:
            response, payload = self._send(conn, method, endpoint, headers, body)
        except CLOSED as e:
            conn.close()
            if not (kept and method in IDEMPOTENT):
                raise URLError(e)
            # the service closed the kept connection as the request left
            self.log.debug(f"{endpoint}: kept connection closed ({e}), reconnecting")
            conn = self._connect(timeout)
            try:
                response, payload = self._send(conn, method, endpoint, headers, body)
            except (http.client.HTTPException, OSError) as e:
                conn.close()
                raise URLError(e)
        except (http.client.HTTPException, OSError) as e:
            conn.close()
            raise URLError(e)
        if response.will_close:
            conn.close()
        else:
            self._give_back(conn)
        status = response.status
        if 300 <= status < 400:
            # urllib follows redirects
            return http_req(
                hostname=self.host,
                port=self.port,
                method=method,
                headers=dict(headers),
                data=data,
                endpoint=endpoint,
                timeout=timeout,
                raw=raw,
            )
        if not 200 <= status < 300:
            raise HTTPError(
                f"https://{self.host}:{self.port}{endpoint}",
                status,
                response.reason,
                response.headers,
                io.BytesIO(payload),
            )
        return response.headers, payload if raw else payload.decode(), status
