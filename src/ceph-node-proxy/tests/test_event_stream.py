import io
import json
import threading
from unittest.mock import MagicMock
from urllib.error import HTTPError

from ceph_node_proxy.event_stream import EventStreamWatcher, sse_events
from ceph_node_proxy.reporter import Reporter


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.buffer = io.BytesIO(text.encode())
        self.closed = False

    def readline(self) -> bytes:
        return self.buffer.readline()

    def close(self) -> None:
        self.closed = True


def client(service: dict) -> MagicMock:
    c = MagicMock()
    c.url = "https://127.0.0.1:40443"
    c.token = "tok"
    c.query.return_value = (None, json.dumps(service), 200)
    return c


STREAM = (": HAL event stream\n\n"
          "id: 1\ndata: {\"Events\": [{\"MessageId\": \"IBMHal.1.0.FanRotorFailed\"}]}\n\n"
          ": keepalive\n\n"
          "id: 2\ndata: {\"Events\": []}\n\n")


def test_sse_events() -> None:
    events = list(sse_events(iter(FakeResponse(STREAM).readline, b"")))
    assert len(events) == 2
    assert json.loads(events[0])["Events"][0]["MessageId"] == "IBMHal.1.0.FanRotorFailed"


def test_events_wake_the_poll() -> None:
    wake = threading.Event()
    opened = []

    def open_url(request, timeout):
        opened.append(request)
        return FakeResponse(STREAM)

    watcher = EventStreamWatcher(
        client({"ServerSentEventUri": "/redfish/v1/EventService/SSE"}), wake, open_url
    )
    watcher.run_once()
    assert wake.is_set()
    assert watcher.events == 2
    assert opened[0].full_url == "https://127.0.0.1:40443/redfish/v1/EventService/SSE"
    assert opened[0].get_header("X-auth-token") == "tok"


def test_no_stream_means_polling_only() -> None:
    watcher = EventStreamWatcher(client({"ServiceEnabled": True}), threading.Event())
    try:
        watcher.run_once()
    except LookupError:
        pass
    else:
        raise AssertionError("expected LookupError")


def test_login_again_once_on_401() -> None:
    calls = []

    def open_url(request, timeout):
        calls.append(request)
        if len(calls) == 1:
            raise HTTPError(request.full_url, 401, "Unauthorized", {}, None)
        return FakeResponse(STREAM)

    c = client({"ServerSentEventUri": "/sse"})
    watcher = EventStreamWatcher(c, threading.Event(), open_url)
    watcher.run_once()
    c.login.assert_called_once()
    assert watcher.events == 2


class FakeSystem:
    def __init__(self) -> None:
        self.data_ready = True
        self.previous_data = {}
        self.current = {}
        self.lock = threading.Lock()
        self.pending_shutdown = False

    def get_system(self) -> dict:
        return self.current


def report(reading: float, health: str = "OK") -> dict:
    return {"status": {"temperatures": {"1": {"inlet": {"reading": reading,
                                                        "status": {"health": health}}}}}}


def test_readings_alone_are_sent_at_most_every_interval() -> None:
    now = [1000.0]
    system = FakeSystem()
    reporter = Reporter(system, {"cephx": {"name": "node-proxy.h", "secret": "s"}},
                        reporter_hostname="mgr", readings_interval=300,
                        clock=lambda: now[0])
    sent = []
    reporter._send_with_retries = lambda: sent.append(reporter.data["patch"]) or True
    system.current = report(20.0)
    reporter._try_send_update()               # first report: sent
    system.current = report(21.0)
    now[0] += 20
    reporter._try_send_update()               # readings only, 20 s later: not yet
    system.current = report(21.0, "Critical")
    now[0] += 20
    reporter._try_send_update()               # state changed: at once
    system.current = report(22.0, "Critical")
    now[0] += 301
    reporter._try_send_update()               # readings only, after the interval
    assert [r["status"]["temperatures"]["1"]["inlet"]["reading"] for r in sent] == \
        [20.0, 21.0, 22.0]
