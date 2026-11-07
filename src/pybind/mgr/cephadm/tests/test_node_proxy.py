import cherrypy
import json
import pytest
import socket
from _pytest.monkeypatch import MonkeyPatch
from urllib.error import HTTPError, URLError
from cherrypy.test import helper
from cephadm.agent import NodeProxyEndpoint
from unittest.mock import MagicMock, call, patch
from cephadm.inventory import AgentCache, NodeProxyCache, Inventory
from cephadm.ssl_certs import SSLCerts
from orchestrator import HardwareNoReport
from . import node_proxy_data


def _free_port() -> int:
    # Pick an ephemeral port so the test server does not clash with other
    # pytest-xdist workers running this module in parallel.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


PORT = _free_port()
fake_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""


class FakeCertMgr:
    def get_root_ca(self):
        return fake_cert

    def generate_cert(self, host_fqdn, node_ip):
        return fake_cert


class FakeMgr:
    def __init__(self) -> None:
        self.log = MagicMock()
        self.get_store = MagicMock(return_value=json.dumps(node_proxy_data.mgr_inventory_cache))
        self.set_store = MagicMock()
        self.set_health_warning = MagicMock()
        self.remove_health_warning = MagicMock()
        self.inventory = Inventory(self)
        self.agent_cache = AgentCache(self)
        self.agent_cache.agent_ports = {"host01": 1234}
        self.node_proxy_cache = NodeProxyCache(self)
        self.node_proxy_cache.save = MagicMock()
        self.node_proxy = MagicMock()
        self.http_server = MagicMock()
        self.http_server.agent = MagicMock()
        self.http_server.agent.ssl_certs = SSLCerts("59d1b32e-xxxx-11ef-xxxx-52540060267a")
        self.http_server.agent.ssl_certs.generate_root_cert(addr=self.get_mgr_ip())
        self.cert_mgr = FakeCertMgr()

    def get_mgr_ip(self) -> str:
        return '0.0.0.0'


class TestNodeProxyEndpoint(helper.CPWebCase):
    mgr = FakeMgr()
    app = NodeProxyEndpoint(mgr)
    mgr.node_proxy_cache.keyrings = {"host01": "fake-secret01",
                                     "host02": "fake-secret02"}
    mgr.node_proxy_cache.oob = {"host01": {"username": "oob-user01",
                                           "password": "oob-pass01"},
                                "host02": {"username": "oob-user02",
                                           "password": "oob-pass02"}}
    mgr.node_proxy_cache.data = node_proxy_data.full_set

    @classmethod
    def setup_server(cls):
        # cherrypy.tree.mount(NodeProxyEndpoint(TestNodeProxyEndpoint.mgr))
        cherrypy.tree.mount(TestNodeProxyEndpoint.app)
        cherrypy.config.update({'global': {
            'server.socket_host': '127.0.0.1',
            'server.socket_port': PORT}})

    def setUp(self):
        self.PORT = PORT
        self.monkeypatch = MonkeyPatch()
        # each test starts like a mgr that just started
        TestNodeProxyEndpoint.mgr.node_proxy_cache.nok = {}
        TestNodeProxyEndpoint.mgr.set_health_warning.reset_mock()

    def keyring_headers(self, hostname: str = 'host01', secret: str = 'fake-secret01') -> list:
        return [
            ('Cephx-Name', f'node-proxy.{hostname}'),
            ('Cephx-Secret', secret),
        ]

    def test_oob_data_misses_cephx_field(self):
        data = '{}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_oob_data_misses_name_field(self):
        data = '{"cephx": {"secret": "fake-secret"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_oob_data_misses_secret_field(self):
        data = '{"cephx": {"name": "node-proxy.host01"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_oob_agent_not_running(self):
        data = '{"cephx": {"name": "node-proxy.host03", "secret": "fake-secret03"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('502 Bad Gateway')

    def test_oob_wrong_keyring(self):
        data = '{"cephx": {"name": "node-proxy.host01", "secret": "wrong-keyring"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('403 Forbidden')

    def test_oob_ok(self):
        data = '{"cephx": {"name": "node-proxy.host01", "secret": "fake-secret01"}}'
        self.getPage("/oob", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                ('Content-Length', str(len(data)))])
        self.assertStatus('200 OK')

    def test_data_missing_patch(self):
        data = '{"cephx": {"name": "node-proxy.host01", "secret": "fake-secret01"}}'
        self.getPage("/data", method="POST", body=data, headers=[('Content-Type', 'application/json'),
                                                                 ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_data_raises_alert(self):
        patch = node_proxy_data.full_set_with_critical
        data = {"cephx": {"name": "node-proxy.host01", "secret": "fake-secret01"}, "patch": patch}
        data_str = json.dumps(data)
        self.getPage("/data", method="POST", body=data_str, headers=[('Content-Type', 'application/json'),
                                                                     ('Content-Length', str(len(data_str)))])
        self.assertStatus('200 OK')

        calls = [call('HARDWARE_STORAGE',
                      count=2,
                      detail=['[1]: disk.bay.0:enclosure.internal.0-1:raid.integrated.1-1 is critical: Enabled',
                              '[1]: disk.bay.9:enclosure.internal.0-1 is critical: Enabled'],
                      summary='2 storage members are not ok'),
                 call('HARDWARE_MEMORY',
                      count=1,
                      detail=['[1]: dimm.socket.a1 is critical: Enabled'],
                      summary='1 memory member is not ok')]

        assert TestNodeProxyEndpoint.mgr.set_health_warning.mock_calls == calls

    def test_data_same_report_leaves_health_alone(self):
        mgr = TestNodeProxyEndpoint.mgr
        self.post_data('host01', node_proxy_data.full_set_with_critical)
        mgr.set_health_warning.reset_mock()
        mgr.remove_health_warning.reset_mock()
        self.post_data('host01', node_proxy_data.full_set_with_critical)
        assert mgr.set_health_warning.mock_calls == []
        assert mgr.remove_health_warning.mock_calls == []

    def post_data(self, host, patch):
        data_str = json.dumps({"cephx": {"name": f"node-proxy.{host}",
                                         "secret": f"fake-secret{host[-2:]}"},
                               "patch": patch})
        self.getPage("/data", method="POST", body=data_str,
                     headers=[('Content-Type', 'application/json'),
                              ('Content-Length', str(len(data_str)))])
        self.assertStatus('200 OK')

    def test_data_alert_lists_all_hosts(self):
        # host02 reported a drive in Warning earlier: host01's report must
        # not drop it from the health check
        mgr = TestNodeProxyEndpoint.mgr
        host02 = json.loads(json.dumps(node_proxy_data.full_set['host02']))
        host02['status']['storage']['1']['disk.bay.8:enclosure.internal.0-1:nonraid.slot.2-1'][
            'status']['health'] = 'Warning'
        saved = mgr.node_proxy_cache.data
        mgr.node_proxy_cache.data = {'host01': node_proxy_data.full_set['host01'],
                                     'host02': host02}
        mgr.set_health_warning.reset_mock()
        try:
            self.post_data('host01', node_proxy_data.full_set_with_critical)
            calls = mgr.set_health_warning.mock_calls
        finally:
            mgr.node_proxy_cache.data = saved
            mgr.set_health_warning.reset_mock()
        assert calls[0] == call(
            'HARDWARE_STORAGE',
            count=3,
            detail=['[1]: disk.bay.0:enclosure.internal.0-1:raid.integrated.1-1 is critical: Enabled',
                    '[1]: disk.bay.9:enclosure.internal.0-1 is critical: Enabled',
                    '[1]: disk.bay.8:enclosure.internal.0-1:nonraid.slot.2-1 is warning: Enabled'],
            summary='3 storage members are not ok')

    def test_data_alert_shared_member_once(self):
        # hosts sharing an enclosure report its members alike
        mgr = TestNodeProxyEndpoint.mgr
        saved = mgr.node_proxy_cache.data
        mgr.node_proxy_cache.data = {'host01': node_proxy_data.full_set['host01'],
                                     'host02': node_proxy_data.full_set_with_critical}
        mgr.set_health_warning.reset_mock()
        try:
            self.post_data('host01', node_proxy_data.full_set_with_critical)
            calls = mgr.set_health_warning.mock_calls
        finally:
            mgr.node_proxy_cache.data = saved
            mgr.set_health_warning.reset_mock()
        assert [c.kwargs['count'] for c in calls] == [2, 1]

    def test_data_alert_says_what_is_wrong(self):
        # the BMC's Redfish Status.Conditions of a member, in the health detail
        mgr = TestNodeProxyEndpoint.mgr
        patch = json.loads(json.dumps(node_proxy_data.full_set_with_critical))
        patch['status']['network'] = {'CS1CAN01': {'ethernetinterfaces_bond0': {
            'name': 'bond0', 'status': {'health': 'Warning', 'state': 'Enabled'},
            'conditions': [{'message_id': 'IBMNetwork.1.0.TeamDegraded',
                            'message': "Only 1 of the 2 member interfaces of team 'bond0' "
                                       "are active. Inactive: ens51f1np1 (no link).",
                            'severity': 'Warning'}]}}}
        saved = mgr.node_proxy_cache.data
        mgr.node_proxy_cache.data = {}
        mgr.set_health_warning.reset_mock()
        try:
            self.post_data('host01', patch)
            calls = {c.args[0]: c.kwargs for c in mgr.set_health_warning.mock_calls}
        finally:
            mgr.node_proxy_cache.data = saved
            mgr.set_health_warning.reset_mock()
        assert calls['HARDWARE_NETWORK']['detail'] == [
            "[CS1CAN01]: ethernetinterfaces_bond0 is warning: Enabled: Only 1 of the 2 member "
            "interfaces of team 'bond0' are active. Inactive: ens51f1np1 (no link)."]
        # members without conditions are reported as before
        assert calls['HARDWARE_MEMORY']['detail'] == ['[1]: dimm.socket.a1 is critical: Enabled']

    def test_data_alert_says_why_a_drive_is_locked(self):
        # a self-encrypting drive the hardware cannot unlock: the drive's
        # conditions explain the HARDWARE_STORAGE warning
        mgr = TestNodeProxyEndpoint.mgr
        patch = json.loads(json.dumps(node_proxy_data.full_set_with_critical))
        patch['status']['storage'] = {'CS1CAN01': {'driveslot-5': {
            'description': 'Enclosure0:Drive5', 'encryption_status': 'Locked',
            'status': {'health': 'Warning', 'state': 'Enabled'},
            'conditions': [{'message_id': 'IBMEncryption.1.0.KeyManagerUnreachable',
                            'message': 'The drive is locked and HAL cannot read its '
                                       'credential from Vault.',
                            'severity': 'Warning'}]}}}
        saved = mgr.node_proxy_cache.data
        mgr.node_proxy_cache.data = {}
        mgr.set_health_warning.reset_mock()
        try:
            self.post_data('host01', patch)
            calls = {c.args[0]: c.kwargs for c in mgr.set_health_warning.mock_calls}
        finally:
            mgr.node_proxy_cache.data = saved
            mgr.set_health_warning.reset_mock()
        assert calls['HARDWARE_STORAGE']['detail'] == [
            '[CS1CAN01]: driveslot-5 is warning: Enabled: The drive is locked and HAL cannot '
            'read its credential from Vault.']

    def test_led_GET_no_hostname(self):
        self.getPage("/led", method="GET", headers=self.keyring_headers())
        self.assertStatus('501 Not Implemented')

    def test_led_PATCH_no_hostname(self):
        data = "{}"
        self.getPage("/led", method="PATCH", body=data, headers=[('Content-Type', 'application/json'),
                                                                 ('Content-Length', str(len(data)))])
        self.assertStatus('501 Not Implemented')

    def test_set_led_no_type(self):
        data = '{"state": "on", "keyring": "fake-secret01"}'
        self.getPage("/host01/led", method="PATCH", body=data, headers=[('Content-Type', 'application/json'),
                                                                        ('Content-Length', str(len(data)))])
        self.assertStatus('400 Bad Request')

    def test_set_chassis_led(self):
        data = '{"state": "on", "keyring": "fake-secret01"}'
        with patch('cephadm.agent.http_req') as p:
            p.return_value = [], '{}', 200
            self.getPage("/host01/led/chassis", method="PATCH", body=data, headers=[('Content-Type', 'application/json'),
                                                                                    ('Content-Length', str(len(data)))])
            self.assertStatus('200 OK')

    def test_get_led_missing_type(self):
        self.getPage("/host01/led", method="GET", headers=self.keyring_headers())
        self.assertStatus('400 Bad Request')

    def test_get_led_no_hostname(self):
        self.getPage("/led", method="GET", headers=self.keyring_headers())
        self.assertStatus('501 Not Implemented')

    def test_get_led_type_chassis_no_hostname(self):
        self.getPage("/led/chassis", method="GET", headers=self.keyring_headers())
        self.assertStatus('404 Not Found')

    def test_get_led_type_drive_no_hostname(self):
        self.getPage("/led/chassis", method="GET", headers=self.keyring_headers())
        self.assertStatus('404 Not Found')

    def test_get_led_type_drive_missing_id(self):
        self.getPage("/host01/led/drive", method="GET", headers=self.keyring_headers())
        self.assertStatus('400 Bad Request')

    def test_get_led_url_error(self):
        with patch('cephadm.agent.http_req') as p:
            p.side_effect = URLError('fake error')
            self.getPage("/host02/led/chassis", method="GET",
                         headers=self.keyring_headers('host02', 'fake-secret02'))
            self.assertStatus('502 Bad Gateway')

    def test_get_chassis_led_ok(self):
        with patch('cephadm.agent.http_req', return_value=MagicMock()) as p:
            p.return_value = [], '{}', 200
            self.getPage("/host01/led/chassis", method="GET", headers=self.keyring_headers())
            self.assertStatus('200 OK')

    def test_get_drive_led_without_id(self):
        self.getPage("/host01/led/drive", method="GET", headers=self.keyring_headers())
        self.assertStatus('400 Bad Request')

    def test_get_drive_led_with_id(self):
        with patch('cephadm.agent.http_req', return_value=MagicMock()) as p:
            p.return_value = [], '{}', 200
            self.getPage("/host01/led/drive/123", method="GET", headers=self.keyring_headers())
            self.assertStatus('200 OK')

    def test_set_drive_led(self):
        # drives are reported per system: {'1': {'disk.bay.8:...': {...}}}
        data = '{"state": "on", "keyring": "fake-secret01"}'
        drive = 'disk.bay.8:enclosure.internal.0-1:nonraid.slot.2-1'
        with patch('cephadm.agent.http_req') as p:
            p.return_value = [], '204', 200
            self.getPage(f"/host01/led/drive/{drive}", method="PATCH", body=data,
                         headers=[('Content-Type', 'application/json'),
                                  ('Content-Length', str(len(data)))])
            self.assertStatus('200 OK')
            self.assertEqual(p.call_args.kwargs['endpoint'], f'/led/drive/{drive}')

    def test_set_drive_led_unknown_drive(self):
        data = '{"state": "on", "keyring": "fake-secret01"}'
        with patch('cephadm.agent.http_req') as p:
            self.getPage("/host01/led/drive/nope", method="PATCH", body=data,
                         headers=[('Content-Type', 'application/json'),
                                  ('Content-Length', str(len(data)))])
            self.assertStatus('400 Bad Request')
            p.assert_not_called()

    def test_set_chassis_led_node_proxy_error(self):
        data = '{"state": "on", "keyring": "fake-secret01"}'
        with patch('cephadm.agent.http_req') as p:
            p.side_effect = HTTPError('https://host01:9456/led/chassis', 405,
                                      'Method Not Allowed', None, None)
            self.getPage("/host01/led/chassis", method="PATCH", body=data,
                         headers=[('Content-Type', 'application/json'),
                                  ('Content-Length', str(len(data)))])
            self.assertStatus('405 Method Not Allowed')

    def test_get_missing_keyring(self):
        self.getPage("/host02/fullreport", method="GET")
        self.assertStatus('400 Bad Request')

    def test_get_missing_keyring_no_hostname(self):
        self.getPage("/fullreport", method="GET")
        self.assertStatus('400 Bad Request')

    def test_get_wrong_keyring(self):
        self.getPage("/host02/fullreport", method="GET",
                     headers=self.keyring_headers('host02', 'wrong-keyring'))
        self.assertStatus('403 Forbidden')

    def test_get_keyring_for_other_host(self):
        self.getPage("/host02/fullreport", method="GET", headers=self.keyring_headers())
        self.assertStatus('403 Forbidden')

    def test_fullreport_with_valid_hostname(self):
        self.getPage("/host02/fullreport", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_fullreport_no_hostname(self):
        self.getPage("/fullreport", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_fullreport_with_invalid_hostname(self):
        self.getPage("/host03/fullreport", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_summary_with_valid_hostname(self):
        self.getPage("/host02/summary", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_summary_no_hostname(self):
        self.getPage("/summary", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_summary_with_invalid_hostname(self):
        self.getPage("/host03/summary", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_criticals_with_valid_hostname(self):
        self.getPage("/host02/criticals", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_criticals_no_hostname(self):
        self.getPage("/criticals", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_criticals_with_invalid_hostname(self):
        self.getPage("/host03/criticals", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_memory_with_valid_hostname(self):
        self.getPage("/host02/memory", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_memory_no_hostname(self):
        self.getPage("/memory", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_memory_with_invalid_hostname(self):
        self.getPage("/host03/memory", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_network_with_valid_hostname(self):
        self.getPage("/host02/network", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_network_no_hostname(self):
        self.getPage("/network", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_network_with_invalid_hostname(self):
        self.getPage("/host03/network", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_processors_with_valid_hostname(self):
        self.getPage("/host02/processors", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_processors_no_hostname(self):
        self.getPage("/processors", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_processors_with_invalid_hostname(self):
        self.getPage("/host03/processors", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_storage_with_valid_hostname(self):
        self.getPage("/host02/storage", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_storage_no_hostname(self):
        self.getPage("/storage", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_storage_with_invalid_hostname(self):
        self.getPage("/host03/storage", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_power_with_valid_hostname(self):
        self.getPage("/host02/power", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_power_no_hostname(self):
        self.getPage("/power", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_power_with_invalid_hostname(self):
        self.getPage("/host03/power", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_fans_with_valid_hostname(self):
        self.getPage("/host02/fans", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_fans_no_hostname(self):
        self.getPage("/fans", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_fans_with_invalid_hostname(self):
        self.getPage("/host03/fans", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_temperatures_with_valid_hostname(self):
        self.getPage("/host02/temperatures", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_temperatures_no_hostname(self):
        self.getPage("/temperatures", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_temperatures_with_invalid_hostname(self):
        self.getPage("/host03/temperatures", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_fcm_with_valid_hostname(self):
        self.getPage("/host02/fcm", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_fcm_no_hostname(self):
        self.getPage("/fcm", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_fcm_with_invalid_hostname(self):
        self.getPage("/host03/fcm", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_firmware_with_valid_hostname(self):
        self.getPage("/host02/firmware", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')

    def test_firmware_no_hostname(self):
        self.getPage("/firmware", method="GET", headers=self.keyring_headers())
        self.assertStatus('200 OK')

    def test_firmware_with_invalid_hostname(self):
        self.getPage("/host03/firmware", method="GET",
                     headers=self.keyring_headers('host03', 'fake-secret03'))
        self.assertStatus('502 Bad Gateway')

    def test_firmwares_legacy_endpoint(self):
        self.getPage("/host02/firmwares", method="GET",
                     headers=self.keyring_headers('host02', 'fake-secret02'))
        self.assertStatus('200 OK')


class TestNodeProxyCacheSave:
    def _make_cache(self) -> NodeProxyCache:
        mgr = MagicMock()
        mgr.get_store = MagicMock(return_value='{}')
        mgr.set_store = MagicMock()
        mgr.inventory = {}
        cache = NodeProxyCache(mgr)
        return cache

    def test_save_updates_in_memory_data(self):
        cache = self._make_cache()
        data = {'status': {'storage': {}, 'fans': {}}, 'sn': 'ABC123'}
        cache.save(host='host01', data=data)
        assert cache.data['host01'] == data

    def test_save_in_memory_data_includes_new_keys_after_upgrade(self):
        cache = self._make_cache()
        # Simulate what load() would have populated from the old KV store
        cache.data['host01'] = {'status': {'storage': {}, 'fans': {}}, 'sn': 'ABC123'}
        # Node-proxy POSTs new data with fcm after upgrade
        new_data = {'status': {'storage': {}, 'fans': {}, 'fcm': {'local': {'nvme0n1': {}}}}, 'sn': 'ABC123'}
        cache.save(host='host01', data=new_data)
        assert 'fcm' in cache.data['host01']['status']

    def test_save_normalizes_hostname(self):
        cache = self._make_cache()
        data = {'status': {}, 'sn': 'XYZ'}
        cache.save(host='HOST01', data=data)
        assert 'host01' in cache.data
        assert 'HOST01' not in cache.data

    def test_save_writes_to_kv_store(self):
        cache = self._make_cache()
        data = {'status': {}, 'sn': 'XYZ'}
        cache.save(host='host01', data=data)
        cache.mgr.set_store.assert_called_once()
        key, value = cache.mgr.set_store.call_args[0]
        assert 'host01' in key
        assert json.loads(value) == data


class TestNodeProxyCacheCommon:
    def _make_cache(self) -> NodeProxyCache:
        mgr = MagicMock()
        mgr.get_store = MagicMock(return_value='{}')
        mgr.set_store = MagicMock()
        mgr.inventory = {}
        cache = NodeProxyCache(mgr)
        cache.data = {
            'at4n1': {'sn': '1', 'status': {'storage': {}, 'fcm': {'local': {'nvme0n1': {}}}}},
            'at4n2': {'sn': '2', 'status': {'storage': {}, 'fans': {}}},
        }
        return cache

    def test_common_includes_hosts_missing_component(self):
        cache = self._make_cache()
        result = cache.common('fcm')
        assert result['at4n1'] == {'local': {'nvme0n1': {}}}
        assert result['at4n2'] == {}

    def test_common_hostname_without_component_returns_empty(self):
        cache = self._make_cache()
        assert cache.common('fcm', hostname='at4n2') == {'at4n2': {}}

    def test_common_hostname_with_component(self):
        cache = self._make_cache()
        result = cache.common('fcm', hostname='at4n1')
        assert result == {'at4n1': {'local': {'nvme0n1': {}}}}


def test_node_proxy_get_dependencies():
    from cephadm.services.node_proxy import NodeProxy

    mgr = MagicMock()
    mgr.get_mgr_ip.return_value = '10.0.0.1'
    mgr.http_server.agent.server_port = 7150
    mgr.cert_mgr.get_root_ca.return_value = 'ROOT-CA'
    mgr.hw_monitoring_vendor = 'generic'

    assert NodeProxy.get_dependencies(mgr) == sorted([
        '10.0.0.1',
        '7150',
        'ROOT-CA',
        'generic',
    ])


def test_node_proxy_cache_unknown_host_raises_hardware_no_report():
    cache = NodeProxyCache(MagicMock())
    cache.data = {'host01': node_proxy_data.full_set['host01']}
    with pytest.raises(HardwareNoReport):
        cache.fullreport(hostname='host02')


def test_node_proxy_cache_list_hosts_skips_incomplete_reports():
    cache = NodeProxyCache(MagicMock())
    cache.data = {
        'host02': node_proxy_data.full_set['host02'],
        'host01': node_proxy_data.full_set['host01'],
        'host03': {},
        'host04': {'host': 'host04', 'status': {}},
    }
    assert cache.list_hosts() == ['host01', 'host02']


def test_node_proxy_cache_summary_redfish_health():
    # node-proxy reports the Redfish health: Warning and Critical are errors
    host02 = json.loads(json.dumps(node_proxy_data.full_set['host02']))
    host02['status']['storage']['1']['disk.bay.8:enclosure.internal.0-1:nonraid.slot.2-1'][
        'status']['health'] = 'Warning'
    cache = NodeProxyCache(MagicMock())
    cache.data = {'host01': node_proxy_data.full_set_with_critical,
                  'host02': host02,
                  'host03': node_proxy_data.full_set['host01']}
    summary = cache.summary()
    assert summary['host01']['status']['storage'] == 'error'
    assert summary['host01']['status']['memory'] == 'error'
    assert summary['host01']['status']['processors'] == 'ok'
    assert summary['host02']['status']['storage'] == 'error'
    assert summary['host03']['status']['storage'] == 'ok'


class TestNodeProxyCachePersistence:

    def cache(self):
        mgr = MagicMock()
        mgr.inventory = ['host01']
        return mgr, NodeProxyCache(mgr)

    def report(self, reading=21.0, health='OK', drives=1):
        return {'status': {
            'temperatures': {'1': {'inlet': {'reading': reading, 'status': {'health': health}}}},
            'storage': {'1': {f'disk{i}': {'serial_number': f'SN{i:04d}' * 8,
                                           'status': {'health': 'OK'}}
                              for i in range(drives)}}}}

    def test_readings_are_not_persisted(self):
        mgr, cache = self.cache()
        cache.save('host01', self.report(21.0))
        assert mgr.set_store.call_count == 1
        cache.save('host01', self.report(23.5))
        assert mgr.set_store.call_count == 1
        assert cache.data['host01']['status']['temperatures']['1']['inlet']['reading'] == 23.5
        cache.save('host01', self.report(23.5, health='Critical'))
        assert mgr.set_store.call_count == 2
        stored = json.loads(mgr.set_store.call_args[0][1])
        assert 'reading' not in stored['status']['temperatures']['1']['inlet']

    def test_large_reports_are_compressed(self):
        from cephadm.inventory import NODE_PROXY_COMPRESSED, _decode_report
        mgr, cache = self.cache()
        report = self.report(drives=1000)
        cache.save('host01', report)
        stored = mgr.set_store.call_args[0][1]
        assert stored.startswith(NODE_PROXY_COMPRESSED)
        assert len(stored) < 64 * 1024 < len(json.dumps(report))
        assert _decode_report(stored)['status']['storage'] == report['status']['storage']
