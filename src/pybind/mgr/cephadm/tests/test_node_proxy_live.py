import json
from unittest.mock import MagicMock, patch
from urllib.error import URLError

import pytest

from cephadm.services.node_proxy import NodeProxy
from orchestrator import OrchestratorError, raise_if_exception

REPORT = {'host': 'host1', 'sn': 'CS1CAN01',
          'status': {'temperatures': {'CS1CAN01': {'cpu1': {'reading': 52}}}},
          'firmware': {}}
LIVE = {'report': REPORT, 'timestamp': 1793862223.5}


@pytest.fixture
def node_proxy():
    mgr = MagicMock()
    mgr.inventory.get_addr.return_value = '10.0.0.5'
    mgr.node_proxy_cache.keyrings = {'host1': 'AQBsecret=='}
    service = NodeProxy(mgr)
    with patch.object(NodeProxy, 'get_ssl_ctx', return_value='ctx'):
        yield service


class TestNodeProxyService:
    def test_report(self, node_proxy):
        with patch('cephadm.services.node_proxy.http_req',
                   return_value=({}, json.dumps(LIVE), 200)) as p:
            assert node_proxy.report('host1') == LIVE
        kwargs = p.call_args.kwargs
        assert (kwargs['hostname'], kwargs['port'], kwargs['endpoint']) == \
            ('10.0.0.5', '9456', '/report')
        assert kwargs['ssl_ctx'] == 'ctx'
        assert kwargs['headers']['Authorization'].startswith('Basic ')

    def test_pages_open_on_a_host_share_one_fetch(self, node_proxy):
        clock = MagicMock(return_value=100.0)
        with patch('cephadm.services.node_proxy.time.monotonic', clock), \
                patch('cephadm.services.node_proxy.http_req',
                      return_value=({}, json.dumps(LIVE), 200)) as p:
            node_proxy.report('host1')
            clock.return_value = 100.0 + NodeProxy.LIVE_TTL - 1
            node_proxy.report('host1')
            assert p.call_count == 1
            clock.return_value = 100.0 + NodeProxy.LIVE_TTL + 1
            node_proxy.report('host1')
            assert p.call_count == 2

    def test_unreachable(self, node_proxy):
        with patch('cephadm.services.node_proxy.http_req', side_effect=URLError('refused')):
            with pytest.raises(RuntimeError):
                node_proxy.report('host1')
        assert node_proxy._live == {}


@pytest.fixture
def orch(cephadm_module):
    cephadm_module.node_proxy = MagicMock()
    cephadm_module.node_proxy_cache.data = {'host1': REPORT}
    return cephadm_module


class TestOrchestrator:
    def test_live(self, orch):
        orch.node_proxy.report.return_value = LIVE
        assert raise_if_exception(orch.node_proxy_live('host1')) == LIVE
        orch.node_proxy.report.assert_called_once_with('host1')

    def test_unknown_host(self, orch):
        with pytest.raises(OrchestratorError, match="'host9' has not reported hardware data"):
            raise_if_exception(orch.node_proxy_live('host9'))
        orch.node_proxy.report.assert_not_called()

    def test_node_proxy_down(self, orch):
        orch.node_proxy.report.side_effect = RuntimeError('refused')
        with pytest.raises(OrchestratorError, match='node-proxy agent is deployed and running'):
            raise_if_exception(orch.node_proxy_live('host1'))
