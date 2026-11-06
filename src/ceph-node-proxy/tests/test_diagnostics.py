import json
from email.message import Message
from unittest.mock import MagicMock, patch

import cherrypy  # type: ignore
import pytest

from ceph_node_proxy.api import API
from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem

ROOT = "/redfish/v1"
CHASSIS = f"{ROOT}/Chassis/ComputeModule-0"
SERVICE = f"{CHASSIS}/LogServices/Diagnostics"
DRIVE = f"{CHASSIS}/Drives/DriveSlot-5"
ENTRY = f"{SERVICE}/Entries/03NK797_20261106035606"

ENTRY_DATA = {
    "@odata.id": ENTRY,
    "Id": "03NK797_20261106035606",
    "Name": "Diagnostic data of Enclosure0:Drive5",
    "Created": "2026-11-06T03:56:06+00:00",
    "Message": "Dump of Enclosure0:Drive5",
    "DiagnosticDataType": "Device",
    "AdditionalDataURI": f"{ENTRY}/attachment",
    "AdditionalDataSizeBytes": 1248895,
    "Links": {"OriginOfCondition": {"@odata.id": DRIVE}},
}

TREE = {
    CHASSIS: {"LogServices": {"@odata.id": f"{CHASSIS}/LogServices"}},
    f"{ROOT}/Chassis/Canister-1": {},
    f"{CHASSIS}/LogServices": {"Members": [{"@odata.id": SERVICE}]},
    SERVICE: {
        "@odata.id": SERVICE,
        "Entries": {"@odata.id": f"{SERVICE}/Entries"},
        "Actions": {
            "#LogService.CollectDiagnosticData": {
                "target": f"{SERVICE}/Actions/LogService.CollectDiagnosticData",
                "DiagnosticDataType@Redfish.AllowableValues": ["Device"],
            },
        },
    },
    f"{SERVICE}/Entries": {"Members": [ENTRY_DATA]},
    DRIVE: {"Links": {"Chassis": {"@odata.id": CHASSIS}}},
}

TASK = {
    "Id": "7",
    "Name": "Diagnostic data of Enclosure0:Drive5",
    "TaskState": "Completed",
    "TaskStatus": "OK",
    "PercentComplete": 100,
    "Messages": [{"Message": "collected"}],
    "Payload": {"HttpHeaders": [f"Location: {ENTRY}"]},
}


def get_path(path):
    if path not in TREE:
        raise RuntimeError(path)
    return TREE[path]


def headers(**values):
    message = Message()
    for key, value in values.items():
        message[key.replace("_", "-")] = value
    return message


@pytest.fixture
def system():
    client, endpoints = MagicMock(), MagicMock()
    client.get_path.side_effect = get_path
    endpoints.__getitem__.return_value.get_members_endpoints.return_value = {
        "ComputeModule-0": CHASSIS,
        "Canister-1": f"{ROOT}/Chassis/Canister-1",
    }
    with (
        patch("ceph_node_proxy.baseredfishsystem.RedFishClient", return_value=client),
        patch("ceph_node_proxy.baseredfishsystem.EndpointMgr", return_value=endpoints),
    ):
        _system = BaseRedfishSystem(
            host="h", port="443", username="u", password="p", config={}
        )
    _system._sys["storage"] = {
        "CAN01": {
            "driveslot-5": {"redfish_endpoint": DRIVE},
            "driveslot-6": {"redfish_endpoint": f"{ROOT}/Chassis/Other/Drives/6"},
        }
    }
    return _system


class TestBackend:
    def test_list(self, system):
        [entry] = system.get_diagnostics()
        assert entry["id"] == "03NK797_20261106035606"
        assert entry["drive"] == "driveslot-5"
        assert entry["size"] == 1248895
        assert entry["download_endpoint"] == f"{ENTRY}/attachment"
        assert entry["service"] == SERVICE

    def test_collect(self, system):
        system.client.query.return_value = (
            headers(Location=f"{ROOT}/TaskService/Tasks/7"),
            json.dumps(dict(TASK, TaskState="Running", Payload={})),
            202,
        )
        task = system.collect_diagnostics("driveslot-5")
        assert (task["id"], task["state"], task["entries"]) == ("7", "Running", [])
        kwargs = system.client.query.call_args.kwargs
        assert kwargs["method"] == "POST"
        assert (
            kwargs["endpoint"] == f"{SERVICE}/Actions/LogService.CollectDiagnosticData"
        )
        assert json.loads(kwargs["data"]) == {
            "DiagnosticDataType": "Device",
            "TargetDevice": {"@odata.id": DRIVE},
        }

    def test_collect_not_supported(self, system):
        TREE[f"{ROOT}/Chassis/Other"] = {}
        try:
            with pytest.raises(NotImplementedError):
                system.collect_diagnostics("driveslot-6")
        finally:
            del TREE[f"{ROOT}/Chassis/Other"]
        with pytest.raises(KeyError):
            system.collect_diagnostics("nope")

    def test_task(self, system):
        system.client.query.return_value = ({}, json.dumps(TASK), 200)
        task = system.get_diagnostics_task("7")
        assert task["entries"] == ["03NK797_20261106035606"]
        assert task["percent_complete"] == 100
        assert task["messages"] == ["collected"]
        assert system.client.query.call_args.kwargs["endpoint"] == (
            f"{ROOT}/TaskService/Tasks/7"
        )

    def test_download(self, system):
        system.client.query.return_value = (
            headers(Content_Disposition='attachment; filename="03NK797.drive_dump"'),
            b"\x00dump",
            200,
        )
        data = system.download_diagnostics("03NK797_20261106035606")
        assert data == {"filename": "03NK797.drive_dump", "content": b"\x00dump"}
        kwargs = system.client.query.call_args.kwargs
        assert kwargs["raw"] is True
        assert kwargs["endpoint"] == f"{ENTRY}/attachment"

    def test_delete(self, system):
        system.client.query.return_value = ({}, "", 204)
        assert system.delete_diagnostics("03NK797_20261106035606") == 204
        assert system.client.query.call_args.kwargs == {
            "method": "DELETE",
            "endpoint": ENTRY,
        }
        with pytest.raises(KeyError):
            system.delete_diagnostics("unknown")


@pytest.fixture
def api():
    _api = API.__new__(API)
    _api.log = MagicMock()
    _api.backend = MagicMock()
    _api.backend.get_storage.return_value = {
        "CAN01": {"driveslot-5": {"redfish_endpoint": DRIVE}}
    }
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
    cherrypy.serving.response.status = 200


class TestApi:
    @pytest.mark.parametrize(
        "path, args",
        [
            ("/diagnostics", ()),
            ("/diagnostics/tasks/7", ("tasks", "7")),
            ("/diagnostics/E1", ("E1",)),
        ],
    )
    def test_cherrypy_dispatch(self, api, path, args):
        # through CherryPy's own dispatcher, as node-proxy serves it
        request = cherrypy.serving.request
        request.app = cherrypy.Application(api, "")
        request.config = {}
        try:
            cherrypy.dispatch.Dispatcher()(path)
            assert request.handler.callable.__name__ == "diagnostics"
            assert tuple(request.handler.args) == args
        finally:
            request.app = None

    def test_list(self, api, request_as):
        request_as("GET")
        api.backend.get_diagnostics.return_value = [{"id": "E1"}]
        assert json.loads(api.diagnostics()) == [{"id": "E1"}]
        assert cherrypy.serving.response.headers["Content-Type"] == "application/json"

    def test_collect(self, api, request_as):
        request_as("POST", {"drive": "driveslot-5"})
        api.backend.collect_diagnostics.return_value = {"id": "7"}
        assert json.loads(api.diagnostics()) == {"id": "7"}
        assert cherrypy.serving.response.status == 202
        api.backend.collect_diagnostics.assert_called_once_with("driveslot-5")

    @pytest.mark.parametrize("body", [{}, {"drive": "CAN01"}, {"drive": "slot-99"}])
    def test_collect_unknown_drive(self, api, request_as, body):
        request_as("POST", body)
        with pytest.raises(cherrypy.HTTPError) as e:
            api.diagnostics()
        assert e.value.status == 400
        api.backend.collect_diagnostics.assert_not_called()

    def test_not_supported(self, api, request_as):
        request_as("POST", {"drive": "driveslot-5"})
        api.backend.collect_diagnostics.side_effect = NotImplementedError()
        with pytest.raises(cherrypy.HTTPError) as e:
            api.diagnostics()
        assert e.value.status == 501

    def test_task(self, api, request_as):
        request_as("GET")
        api.backend.get_diagnostics_task.return_value = {"id": "7", "state": "Running"}
        assert json.loads(api.diagnostics("tasks", "7"))["state"] == "Running"
        api.backend.get_diagnostics_task.assert_called_once_with("7")

    def test_download(self, api, request_as):
        request_as("GET")
        api.backend.download_diagnostics.return_value = {
            "filename": "a.drive_dump",
            "content": b"dump",
        }
        assert api.diagnostics("E1") == b"dump"
        response_headers = cherrypy.serving.response.headers
        assert response_headers["Content-Type"] == "application/octet-stream"
        assert 'filename="a.drive_dump"' in response_headers["Content-Disposition"]

    def test_delete(self, api, request_as):
        request_as("DELETE")
        assert api.diagnostics("E1") == b""
        assert cherrypy.serving.response.status == 204
        api.backend.delete_diagnostics.assert_called_once_with("E1")

    def test_delete_unknown(self, api, request_as):
        request_as("DELETE")
        api.backend.delete_diagnostics.side_effect = KeyError("E9")
        with pytest.raises(cherrypy.HTTPError) as e:
            api.diagnostics("E9")
        assert e.value.status == 404

    @pytest.mark.parametrize(
        "method, path",
        [("POST", ("E1",)), ("GET", ("a", "b", "c"))],
    )
    def test_unknown_paths(self, api, request_as, method, path):
        request_as(method)
        with pytest.raises(cherrypy.HTTPError) as e:
            api.diagnostics(*path)
        assert e.value.status == 404
