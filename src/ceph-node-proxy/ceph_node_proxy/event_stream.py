"""Wake node-proxy when the Redfish service reports an event.

node-proxy polls the Redfish service every refresh_interval seconds. When
the service has an EventService with a ServerSentEventUri, this thread
holds that stream open and sets a wake event on each event, so the next
poll (and the report to the mgr) happens at once rather than up to
refresh_interval later. Polling stays the source of the data: the events
only say when to look. A service without server-sent events, or a stream
that keeps failing, just leaves polling as it was.
"""

import json
import ssl
import threading
import time
from typing import Callable, Iterator, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from ceph_node_proxy.util import BaseThread, get_logger

RETRY_MIN_SEC = 5
RETRY_MAX_SEC = 300
#: a service without (usable) server-sent events is asked again after this
NO_STREAM_SEC = 3600
#: no byte for this long: the stream is dead (HAL sends a comment every 15 s)
READ_TIMEOUT_SEC = 120


def sse_events(lines: Iterator[bytes]) -> Iterator[str]:
    """The data of each event of a server-sent event stream."""
    data: List[str] = []
    for raw in lines:
        line = raw.decode("utf-8", "replace").rstrip("\r\n")
        if not line:
            if data:
                yield "\n".join(data)
                data = []
            continue
        if line.startswith("data:"):
            data.append(line[5:].lstrip())


class EventStreamWatcher(BaseThread):
    def __init__(
        self,
        client: object,
        wake: threading.Event,
        open_url: Optional[Callable[..., object]] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        super().__init__()
        self.client = client
        self.wake = wake
        self._open = open_url or self._urlopen
        self._sleep = sleep
        self.log = get_logger(__name__)
        self.events = 0

    @staticmethod
    def _urlopen(request: Request, timeout: int) -> object:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return urlopen(request, context=context, timeout=timeout)

    def sse_uri(self) -> Optional[str]:
        """The service's ServerSentEventUri, None when it has none."""
        _, body, _ = self.client.query(  # type: ignore[attr-defined]
            method="GET", endpoint="/redfish/v1/EventService"
        )
        service = json.loads(body) if body else {}
        if service.get("ServiceEnabled") is False:
            return None
        uri = service.get("ServerSentEventUri")
        return uri if isinstance(uri, str) and uri.startswith("/") else None

    def stream(self, uri: str) -> Iterator[str]:
        url = f"{self.client.url}{uri}"  # type: ignore[attr-defined]
        headers = {"Accept": "text/event-stream"}
        if getattr(self.client, "token", ""):
            headers["X-Auth-Token"] = self.client.token  # type: ignore[attr-defined]
        response = self._open(Request(url, headers=headers), READ_TIMEOUT_SEC)
        try:
            yield from sse_events(iter(response.readline, b""))  # type: ignore[attr-defined]
        finally:
            response.close()  # type: ignore[attr-defined]

    def run_once(self) -> None:
        uri = self.sse_uri()
        if uri is None:
            raise LookupError("the Redfish service has no server-sent event stream")
        try:
            for _ in self.stream(uri):
                if self.stop:
                    return
                self.events += 1
                self.wake.set()
        except HTTPError as e:
            if e.code != 401:
                raise
            # the session expired, or this stream wants other credentials
            self.client.login()  # type: ignore[attr-defined]
            for _ in self.stream(uri):
                if self.stop:
                    return
                self.events += 1
                self.wake.set()

    def main(self) -> None:
        delay: float = RETRY_MIN_SEC
        while not self.stop:
            try:
                self.run_once()
                delay = RETRY_MIN_SEC
            except LookupError as e:
                self.log.info(f"{e}: polling only")
                delay = NO_STREAM_SEC
            except HTTPError as e:
                self.log.info(f"event stream refused ({e.code}): polling only for a while")
                delay = NO_STREAM_SEC if e.code in (401, 403, 404, 405, 501) else delay
            except (URLError, OSError, ValueError) as e:
                self.log.debug(f"event stream: {e}, reconnecting in {delay} s")
                delay = min(delay * 2, RETRY_MAX_SEC)
            self._sleep(delay)
