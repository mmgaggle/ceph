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
