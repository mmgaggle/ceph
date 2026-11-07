import json
import re
import secrets
import ssl
import base64

from urllib.error import HTTPError, URLError
from typing import List, Any, Dict, Tuple, Optional, MutableMapping, TYPE_CHECKING

from .service_registry import register_cephadm_service
from .cephadmservice import CephadmDaemonDeploySpec, CephService, DaemonDeployContext
from .. import utils
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec
from ceph.utils import http_req
from orchestrator import OrchestratorError

if TYPE_CHECKING:
    from ..module import CephadmOrchestrator


@register_cephadm_service
class NodeProxy(CephService):
    TYPE = 'node-proxy'
    PORT = 9456
    METRICS_USER_KEY = 'node_proxy/metrics_username'
    METRICS_PASS_KEY = 'node_proxy/metrics_password'

    @property
    def needs_monitoring(self) -> bool:
        # Prometheus scrapes the hardware metrics from each node-proxy
        return True

    @classmethod
    def metrics_credentials(cls, mgr: "CephadmOrchestrator") -> Tuple[str, str]:
        """Prometheus's credentials for node-proxy's /metrics, made once for
        the cluster. They open /metrics and nothing else."""
        user = mgr.get_store(cls.METRICS_USER_KEY)
        password = mgr.get_store(cls.METRICS_PASS_KEY)
        if not user or not password:
            user, password = 'prometheus', secrets.token_urlsafe(32)
            mgr.set_store(cls.METRICS_USER_KEY, user)
            mgr.set_store(cls.METRICS_PASS_KEY, password)
        return user, password

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host

        if not self.mgr.http_server.agent:
            raise OrchestratorError('Cannot deploy node-proxy before creating cephadm endpoint')

        super().prepare_certificates(daemon_spec)
        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_id, host=host), [])
        daemon_spec.keyring = keyring
        self.mgr.node_proxy_cache.update_keyring(host, keyring)

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)

        return daemon_spec

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        root_cert = ''
        server_port = ''
        try:
            server_port = str(mgr.http_server.agent.server_port)
            root_cert = mgr.cert_mgr.get_root_ca()
        except Exception:
            pass
        metrics_user, metrics_password = cls.metrics_credentials(mgr)
        return sorted([mgr.get_mgr_ip(), server_port, root_cert, mgr.hw_monitoring_vendor,
                       f'metrics-cred:{utils.config_hash(metrics_user + metrics_password)}'])

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        self.agent_endpoint = self.mgr.http_server.agent
        try:
            assert self.agent_endpoint
            assert self.agent_endpoint.server_port
        except Exception:
            raise OrchestratorError(
                'Cannot deploy node-proxy daemons until cephadm endpoint has finished generating certs')

        tls_pair = self.get_certificates(daemon_spec)
        metrics_user, metrics_password = self.metrics_credentials(self.mgr)
        cfg = {
            'target_ip': self.mgr.get_mgr_ip(),
            'target_port': self.agent_endpoint.server_port,
            'name': f'node-proxy.{daemon_spec.host}',
            'keyring': daemon_spec.keyring,
            'root_cert.pem': self.mgr.cert_mgr.get_root_ca(),
            'listener.crt': tls_pair.cert,
            'listener.key': tls_pair.key,
            'system': {'vendor': self.mgr.hw_monitoring_vendor},
            'metrics': {'username': metrics_user, 'password': metrics_password},
        }
        config = {'node-proxy.json': json.dumps(cfg)}

        return config, self.get_dependencies(
            self.mgr, spec, daemon_spec.daemon_type)

    def handle_hw_monitoring_setting(self) -> bool:
        # function to apply or remove node-proxy service spec depending
        # on whether the hw_mointoring config option is set or not.
        # It should return True when it either creates or deletes a spec
        # and False otherwise.
        if self.mgr.hw_monitoring:
            if 'node-proxy' not in self.mgr.spec_store:
                spec = ServiceSpec(
                    service_type='node-proxy',
                    placement=PlacementSpec(host_pattern='*')
                )
                self.mgr.spec_store.save(spec)
                return True
            return False
        else:
            if 'node-proxy' in self.mgr.spec_store:
                self.mgr.spec_store.rm('node-proxy')
                return True
            return False

    def get_ssl_ctx(self) -> ssl.SSLContext:
        ssl_root_crt = self.mgr.cert_mgr.get_root_ca()
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = True
        ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        ssl_ctx.load_verify_locations(cadata=ssl_root_crt)
        return ssl_ctx

    def led(self, light_type: str, action: str, hostname: str, device: Optional[str] = None) -> Dict[str, Any]:
        ssl_ctx: ssl.SSLContext = self.get_ssl_ctx()
        header: MutableMapping[str, str] = {}
        method: str = 'PATCH' if action in ['on', 'off'] else 'GET'
        payload: Optional[Dict[str, str]] = None
        addr: str = self.mgr.inventory.get_addr(hostname)
        endpoint: List[str] = ['led', light_type]
        _device: str = device if device else ''

        if light_type == 'drive':
            endpoint.append(_device)

        if method == 'PATCH':
            payload = dict(state=action)

        header = self.generate_auth_header(hostname)

        endpoint = f'/{"/".join(endpoint)}'

        try:
            headers, result, status = http_req(hostname=addr,
                                               port=str(self.PORT),
                                               headers=header,
                                               method=method,
                                               data=json.dumps(payload),
                                               endpoint=endpoint,
                                               ssl_ctx=ssl_ctx)
            result_json = json.loads(result)
        except HTTPError as e:
            self.mgr.log.error(e)
            raise
        except URLError as e:
            raise RuntimeError(e)

        return result_json

    def generate_auth_header(self, hostname: str) -> Dict[str, str]:
        # the node-proxy daemon's cephx identity: the out-of-band (BMC)
        # credentials stay between node-proxy and the Redfish service
        keyring = self.mgr.node_proxy_cache.keyrings.get(hostname)
        if keyring:
            username, password = f'node-proxy.{hostname}', keyring
        else:
            try:
                username = self.mgr.node_proxy_cache.oob[hostname]['username']
                password = self.mgr.node_proxy_cache.oob[hostname]['password']
            except KeyError as e:
                self.mgr.log.error(f'Check oob information is provided for {hostname}.')
                raise RuntimeError(e)
        auth: bytes = f'{username}:{password}'.encode('utf-8')
        auth_str: str = base64.b64encode(auth).decode('utf-8')
        return {'Authorization': f'Basic {auth_str}'}

    def shutdown(self, hostname: str, force: Optional[bool] = False) -> Dict[str, Any]:
        ssl_ctx: ssl.SSLContext = self.get_ssl_ctx()
        header: Dict[str, str] = self.generate_auth_header(hostname)
        addr: str = self.mgr.inventory.get_addr(hostname)

        endpoint = '/shutdown'
        payload: Dict[str, Optional[bool]] = dict(force=force)

        try:
            headers, result, status = http_req(hostname=addr,
                                               port=str(self.PORT),
                                               headers=header,
                                               data=json.dumps(payload),
                                               endpoint=endpoint,
                                               ssl_ctx=ssl_ctx)
            result_json = json.loads(result)
        except HTTPError as e:
            self.mgr.log.error(e)
            raise
        except URLError as e:
            raise RuntimeError(e)

        return result_json

    def powercycle(self, hostname: str) -> Dict[str, Any]:
        ssl_ctx: ssl.SSLContext = self.get_ssl_ctx()
        header: Dict[str, str] = self.generate_auth_header(hostname)
        addr: str = self.mgr.inventory.get_addr(hostname)

        endpoint = '/powercycle'

        try:
            headers, result, status = http_req(hostname=addr,
                                               port=str(self.PORT),
                                               headers=header,
                                               data="{}",
                                               endpoint=endpoint,
                                               ssl_ctx=ssl_ctx)
            result_json = json.loads(result)
        except HTTPError as e:
            self.mgr.log.error(e)
            raise
        except URLError as e:
            raise RuntimeError(e)

        return result_json

    # Diagnostic data (drive dumps) collected by the Redfish service of a
    # host, through its node-proxy API (/diagnostics).

    def _diagnostics_query(self,
                           hostname: str,
                           endpoint: str,
                           method: str = 'GET',
                           payload: Optional[Dict[str, Any]] = None,
                           raw: bool = False,
                           timeout: Optional[int] = None) -> Tuple[Any, Any, int]:
        addr: str = self.mgr.inventory.get_addr(hostname)
        try:
            return http_req(hostname=addr,
                            port=str(self.PORT),
                            headers=self.generate_auth_header(hostname),
                            method=method,
                            data=json.dumps(payload) if payload is not None else None,
                            endpoint=endpoint,
                            ssl_ctx=self.get_ssl_ctx(),
                            timeout=timeout,
                            raw=raw)
        except HTTPError as e:
            self.mgr.log.error(e)
            raise
        except URLError as e:
            raise RuntimeError(e)

    def diagnostics(self, hostname: str) -> List[Dict[str, Any]]:
        _, result, _ = self._diagnostics_query(hostname, '/diagnostics')
        entries: List[Dict[str, Any]] = json.loads(result)
        return entries

    def collect_diagnostics(self, hostname: str, device: str) -> Dict[str, Any]:
        _, result, _ = self._diagnostics_query(hostname, '/diagnostics', 'POST',
                                               {'drive': device})
        task: Dict[str, Any] = json.loads(result)
        return task

    @staticmethod
    def _check_id(value: str, what: str) -> str:
        # IDs go into URLs of node-proxy, then of the host's Redfish service
        if not re.fullmatch(r'[A-Za-z0-9._-]{1,128}', str(value)) or value in ('.', '..'):
            raise OrchestratorError(f'Invalid {what}: {value!r}')
        return value

    def diagnostics_task(self, hostname: str, task_id: str) -> Dict[str, Any]:
        task_id = self._check_id(task_id, 'task ID')
        _, result, _ = self._diagnostics_query(hostname, f'/diagnostics/tasks/{task_id}')
        task: Dict[str, Any] = json.loads(result)
        return task

    def download_diagnostics(self, hostname: str, entry_id: str) -> Tuple[str, bytes]:
        entry_id = self._check_id(entry_id, 'entry ID')
        headers, content, _ = self._diagnostics_query(
            hostname, f'/diagnostics/{entry_id}', raw=True, timeout=180)
        filename = headers.get_filename() if hasattr(headers, 'get_filename') else None
        return filename or f'{entry_id}.bin', content

    def delete_diagnostics(self, hostname: str, entry_id: str) -> None:
        entry_id = self._check_id(entry_id, 'entry ID')
        self._diagnostics_query(hostname, f'/diagnostics/{entry_id}', 'DELETE')
