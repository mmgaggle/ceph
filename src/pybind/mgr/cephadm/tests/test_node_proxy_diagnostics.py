import json
from email.message import Message
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

import pytest

from cephadm.services.node_proxy import NodeProxy
from orchestrator import OrchestratorError, raise_if_exception

STORAGE = {'CAN01': {'driveslot-5': {'redfish_endpoint': '/redfish/v1/Chassis/C/Drives/DriveSlot-5'}}}
ENTRY = {'id': 'S5_20261106035606', 'drive': 'driveslot-5', 'size': 1248895,
         'created': '2026-11-06T03:56:06+00:00', 'name': 'Diagnostic data of Enclosure0:Drive5'}


@pytest.fixture
def node_proxy():
    mgr = MagicMock()
    mgr.inventory.get_addr.return_value = '10.0.0.5'
    mgr.node_proxy_cache.oob = {'host1': {'username': 'u', 'password': 'p'}}
    service = NodeProxy(mgr)
    with patch.object(NodeProxy, 'get_ssl_ctx', return_value='ctx'):
        yield service


class TestNodeProxyService:
    def test_list(self, node_proxy):
        with patch('cephadm.services.node_proxy.http_req',
                   return_value=({}, json.dumps([ENTRY]), 200)) as p:
            assert node_proxy.diagnostics('host1') == [ENTRY]
        kwargs = p.call_args.kwargs
        assert (kwargs['hostname'], kwargs['port'], kwargs['endpoint']) == \
            ('10.0.0.5', '9456', '/diagnostics')
        assert kwargs['headers']['Authorization'].startswith('Basic ')

    def test_collect(self, node_proxy):
        with patch('cephadm.services.node_proxy.http_req',
                   return_value=({}, json.dumps({'id': '7', 'state': 'New'}), 202)) as p:
            assert node_proxy.collect_diagnostics('host1', 'driveslot-5')['id'] == '7'
        kwargs = p.call_args.kwargs
        assert kwargs['method'] == 'POST'
        assert json.loads(kwargs['data']) == {'drive': 'driveslot-5'}

    def test_task(self, node_proxy):
        with patch('cephadm.services.node_proxy.http_req',
                   return_value=({}, json.dumps({'id': '7', 'state': 'Completed'}), 200)) as p:
            assert node_proxy.diagnostics_task('host1', '7')['state'] == 'Completed'
        assert p.call_args.kwargs['endpoint'] == '/diagnostics/tasks/7'

    def test_download(self, node_proxy):
        headers = Message()
        headers['Content-Disposition'] = 'attachment; filename="S5.drive_dump"'
        with patch('cephadm.services.node_proxy.http_req',
                   return_value=(headers, b'\x01dump', 200)) as p:
            assert node_proxy.download_diagnostics('host1', 'S5') == ('S5.drive_dump', b'\x01dump')
        assert p.call_args.kwargs['raw'] is True
        assert p.call_args.kwargs['endpoint'] == '/diagnostics/S5'

    def test_delete(self, node_proxy):
        with patch('cephadm.services.node_proxy.http_req', return_value=({}, b'', 204)) as p:
            node_proxy.delete_diagnostics('host1', 'S5')
        assert p.call_args.kwargs['method'] == 'DELETE'

    def test_unreachable(self, node_proxy):
        with patch('cephadm.services.node_proxy.http_req', side_effect=URLError('refused')):
            with pytest.raises(RuntimeError):
                node_proxy.diagnostics('host1')


@pytest.fixture
def orch(cephadm_module):
    cephadm_module.node_proxy = MagicMock()
    cephadm_module.node_proxy_cache.data = {
        'host1': {'status': {'storage': STORAGE}},
    }
    return cephadm_module


class TestOrchestrator:
    def test_ls(self, orch):
        orch.node_proxy.diagnostics.return_value = [ENTRY]
        assert raise_if_exception(orch.hardware_diagnostics_ls('host1')) == {'host1': [ENTRY]}

    def test_ls_all_hosts_skips_failures(self, orch):
        orch.node_proxy_cache.data['host2'] = {}
        orch.node_proxy.diagnostics.side_effect = [[ENTRY], RuntimeError('down')]
        with patch.object(orch.node_proxy_cache, 'list_hosts', return_value=['host1', 'host2']):
            assert raise_if_exception(orch.hardware_diagnostics_ls()) == {'host1': [ENTRY]}

    def test_collect(self, orch):
        orch.node_proxy.collect_diagnostics.return_value = {'id': '7'}
        assert raise_if_exception(orch.hardware_diagnostics_collect('host1', 'driveslot-5')) == \
            {'id': '7'}
        orch.node_proxy.collect_diagnostics.assert_called_once_with('host1', 'driveslot-5')

    @pytest.mark.parametrize('hostname, device, message', [
        ('host1', 'slot-99', "'slot-99' not found on host1"),
        ('host9', 'driveslot-5', "'host9' has not reported hardware data"),
    ])
    def test_collect_refused(self, orch, hostname, device, message):
        with pytest.raises(OrchestratorError, match=message):
            raise_if_exception(orch.hardware_diagnostics_collect(hostname, device))
        orch.node_proxy.collect_diagnostics.assert_not_called()

    @pytest.mark.parametrize('error, message', [
        (HTTPError('/diagnostics', 501, 'Not Implemented', None, None),
         'does not collect the diagnostic data of driveslot-5'),
        (HTTPError('/diagnostics', 409, 'already being collected', None, None),
         'host1: 409 already being collected'),
        (RuntimeError('refused'), 'node-proxy agent is deployed and running'),
    ])
    def test_collect_errors(self, orch, error, message):
        orch.node_proxy.collect_diagnostics.side_effect = error
        with pytest.raises(OrchestratorError, match=message):
            raise_if_exception(orch.hardware_diagnostics_collect('host1', 'driveslot-5'))

    def test_get_and_rm(self, orch):
        orch.node_proxy.download_diagnostics.return_value = ('S5.drive_dump', b'dump')
        assert raise_if_exception(orch.hardware_diagnostics_get('host1', 'S5')) == \
            ('S5.drive_dump', b'dump')
        assert 'Removed' in raise_if_exception(orch.hardware_diagnostics_rm('host1', 'S5'))
        orch.node_proxy.delete_diagnostics.assert_called_once_with('host1', 'S5')
