from unittest.mock import MagicMock
from urllib.error import HTTPError

import cherrypy  # type: ignore
import pytest

from ceph_node_proxy.api import API

# drives are reported per system: {system_id: {drive_id: ...}}
STORAGE = {
    "CAN01": {"driveslot-2": {"redfish_endpoint": "/redfish/v1/Chassis/C/Drives/2"}},
    "CAN02": {"slot-17": {"redfish_endpoint": "/redfish/v1/Chassis/F/Drives/17"}},
}


@pytest.fixture
def api():
    # bypass Server.__init__ (no engine subscription in unit tests)
    _api = API.__new__(API)
    _api.log = MagicMock()
    _api.backend = MagicMock()
    _api.backend.get_storage.return_value = STORAGE
    _api.hostname = "host01"
    return _api


@pytest.fixture
def request_as():
    req = cherrypy.serving.request

    def _set(method, json=None):
        req.method = method
        req.json = json

    yield _set
    req.method = "GET"
    req.json = None


class TestLed:
    @pytest.mark.parametrize("drive", ["driveslot-2", "slot-17"])
    def test_drive_led_on(self, api, request_as, drive):
        request_as("PATCH", {"state": "on"})
        api.backend.device_led_on.return_value = 204
        assert api._led(type="drive", id=drive) == 204
        api.backend.device_led_on.assert_called_once_with(drive)

    def test_get_drive_led(self, api, request_as):
        request_as("GET")
        api.backend.get_device_led.return_value = {"LocationIndicatorActive": True}
        assert api._led(type="drive", id="driveslot-2") == {
            "LocationIndicatorActive": True
        }

    def test_unknown_drive(self, api, request_as):
        request_as("PATCH", {"state": "on"})
        with pytest.raises(cherrypy.HTTPError) as e:
            api._led(type="drive", id="CAN01")
        assert e.value.status == 400
        api.backend.device_led_on.assert_not_called()

    def test_chassis_led_off(self, api, request_as):
        request_as("PATCH", {"state": "off"})
        api.backend.chassis_led_off.return_value = 204
        assert api._led(type="chassis") == 204

    def test_redfish_error_is_forwarded(self, api, request_as):
        request_as("PATCH", {"state": "on"})
        api.backend.chassis_led_on.side_effect = HTTPError(
            "/redfish/v1/Chassis/C", 405, "Method Not Allowed", None, None
        )
        with pytest.raises(cherrypy.HTTPError) as e:
            api._led(type="chassis")
        assert e.value.status == 405

    def test_provider_without_led_support(self, api, request_as):
        request_as("GET")
        api.backend.get_chassis_led.side_effect = NotImplementedError()
        with pytest.raises(cherrypy.HTTPError) as e:
            api._led(type="chassis")
        assert e.value.status == 501


def test_check_auth_takes_the_cephx_identity() -> None:
    from unittest.mock import MagicMock
    from ceph_node_proxy.api import NodeProxyApi

    mgr = MagicMock()
    mgr.username, mgr.password = "oob-user", "oob-pass"
    mgr.cephx_name, mgr.cephx_secret = "node-proxy.host01", "AQBsecret=="
    api = NodeProxyApi.__new__(NodeProxyApi)
    api.username, api.password = mgr.username, mgr.password
    api.cephx_name, api.cephx_secret = mgr.cephx_name, mgr.cephx_secret
    assert api.check_auth("localhost", "node-proxy.host01", "AQBsecret==")
    assert api.check_auth("localhost", "oob-user", "oob-pass")     # an older mgr
    assert not api.check_auth("localhost", "node-proxy.host01", "wrong")
    assert not api.check_auth("localhost", "node-proxy.host02", "AQBsecret==")
    api.cephx_name = api.cephx_secret = ""
    assert not api.check_auth("localhost", "", "")


REPORT = {
    "status": {
        "storage": {"CAN01": {"driveslot-2": {
            "capacity_bytes": 3840755982336, "model": "FCM5", "protocol": "NVMe",
            "firmware_version": "000A5305", "slot": 2, "serial_number": "S1",
            "status": {"health": "OK", "state": "Enabled"}}}},
        "fans": {"CAN01": {"0": {"name": "FAN1", "reading": 8000,
                                 "status": {"health": "Warning", "state": "Enabled"}}}},
    },
    "firmware": {"bmc": {"name": "BMC", "version": "1.2"}},
}


class TestMetrics:
    def test_metrics(self, api):
        api.backend.snapshot.return_value = (REPORT, 1793862223.5)
        text = api.metrics().decode()
        assert cherrypy.serving.response.headers["Content-Type"].startswith(
            "text/plain; version=0.0.4"
        )
        assert 'ceph_hardware_fan_rpm{hostname="host01",fan_name="FAN1"} 8000.0\n' in text
        assert (
            'ceph_hardware_status{hostname="host01",component="FAN1",'
            'category="fans",state="Enabled"} 1.0\n'
        ) in text
        assert (
            'ceph_hardware_firmware_info{hostname="host01",component="BMC",version="1.2"} 1.0'
        ) in text
        assert 'ceph_hardware_report_timestamp_seconds{hostname="host01"} 1793862223.5\n' in text
        api.backend.get_storage.assert_not_called()     # never waits on the system

    def test_before_the_first_update(self, api):
        api.backend.snapshot.return_value = ({}, None)
        text = api.metrics().decode()
        assert "# TYPE ceph_hardware_status gauge" in text
        assert "{" not in text


def test_metrics_credentials_open_metrics_only() -> None:
    from ceph_node_proxy.api import NodeProxyApi

    api = NodeProxyApi.__new__(NodeProxyApi)
    api.username = api.password = ""
    api.cephx_name, api.cephx_secret = "node-proxy.host01", "AQBsecret=="
    api.metrics_username, api.metrics_password = "prometheus", "scrape"
    request = cherrypy.serving.request
    try:
        request.path_info = "/metrics"
        assert api.check_auth("localhost", "prometheus", "scrape")
        assert not api.check_auth("localhost", "prometheus", "wrong")
        assert api.check_auth("localhost", "node-proxy.host01", "AQBsecret==")
        for path in ("/led/chassis", "/diagnostics", "/storage", "/metrics/x"):
            request.path_info = path
            assert not api.check_auth("localhost", "prometheus", "scrape"), path
        api.metrics_username = api.metrics_password = ""
        request.path_info = "/metrics"
        assert not api.check_auth("localhost", "", "")
    finally:
        request.path_info = "/"
