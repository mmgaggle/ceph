
import io
from typing import Any, Dict, List, Optional

import cherrypy
from cherrypy.lib.static import serve_fileobj
from orchestrator import HardwareNoReport

from ..model.hardware import HardwareHostResponse, RawHostReport
from ..security import Scope
from ..services._paginate import ListPaginator
from ..services.exception import handle_custom_error, handle_orchestrator_error
from ..services.hardware import HardwareService
from ..services.orchestrator import HardwareManager, OrchClient
from . import APIDoc, APIRouter, Endpoint, EndpointDoc, Param, ReadPermission, \
    RESTController
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator

HARDWARE_CATEGORY_SCHEMA = {
    'health': (str, 'Worst health of the components: OK, Unknown, Warning or Critical'),
    'total': (int, 'Number of components'),
    'ok': (int, 'Components with health OK'),
    'warning': (int, 'Components with health Warning'),
    'critical': (int, 'Components with health Critical'),
    'unknown': (int, 'Components whose health is not reported'),
    'components': ([{
        'id': (str, 'Component id'),
        'chassis_id': (str, 'Chassis id'),
        'health': (str, 'OK, Unknown, Warning or Critical'),
        'state': (str, 'Redfish state, e.g. Enabled'),
    }], 'Components; each also carries the fields its category reports'),
}

HARDWARE_HOST_SCHEMA = {
    'hostname': (str, 'Host name'),
    'sn': (str, 'Serial number'),
    'health': (str, 'Worst health of the categories that have components'),
    'status': ({
        category: (HARDWARE_CATEGORY_SCHEMA, f'{category} components')
        for category in ('storage', 'processors', 'memory', 'network', 'power', 'fans',
                         'temperatures')
    }, 'Hardware per category'),
    'firmware': (dict, 'Installed firmware: name -> version'),
    'compression': ({
        'fcm_drive_count': (int, 'Number of FCM drives'),
        'phy_util_percent': (float, 'Physical space used, in percent'),
        'log_util_percent': (float, 'Logical space used, in percent'),
        'savings_bytes': (int, 'Logical minus physical bytes used'),
        'compression_ratio': (float, 'Logical / physical bytes used'),
        'efficiency_percent': (float, 'Space saved, in percent'),
    }, 'FCM compression of the host; null when it has no FCM drives. Ratio, savings, '
       'efficiency and logical use are null below 100 GiB of physical use'),
}


@APIRouter('/hardware', Scope.HOSTS)
@APIDoc("Hardware management API", "Hardware")
class Hardware(RESTController):

    @RESTController.Collection('GET', version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("Retrieve a summary of the hardware health status")
    def summary(self, categories: Optional[List[str]] = None, hostname: Optional[List[str]] = None):
        """
        Get the health status of as many hardware categories, or all of them if none is given
        :param categories: The hardware type, all of them by default
        :param hostname: The host to retrieve from, all of them by default
        """
        return HardwareService.get_summary(categories, hostname)


@APIRouter('/hardware/compression', Scope.HOSTS)
@APIDoc("Hardware compression API", "Hardware")
class HardwareCompression(RESTController):

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("Retrieve cluster-wide FCM hardware compression statistics")
    def list(self):
        return HardwareService.get_compression()


@APIRouter('/hardware/hosts', Scope.HOSTS)
@APIDoc("Hardware hosts API", "Hardware")
class HardwareHosts(RESTController):

    @EndpointDoc(
        "List the hardware of all hosts with node-proxy data, paginated.",
        parameters=ListPaginator.doc(default_limit=5),
        responses={200: [HARDWARE_HOST_SCHEMA]},
    )
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    def list(self, offset: int = 0, limit: int = 5, sort: str = '',
             search: str = '') -> List[Dict[str, object]]:
        hardware = OrchClient.instance().hardware

        # Paginate cheap hostname refs first, then load blobs only for this page:
        # O(limit) blobs per request regardless of cluster size.
        paginator = ListPaginator(
            int(offset), int(limit), sort, search,
            input_list=[{'hostname': h} for h in hardware.list_hosts()],
            searchable_params=['hostname'],
            sortable_params=['hostname'],
            default_sort='+hostname',
        )
        hosts = [
            HardwareHostResponse.from_raw(report).to_dict()
            for ref in paginator.list()
            if (report := _report_if_present(hardware, ref['hostname']))
        ]

        # The paginator only knows the filtered count once list() is consumed.
        cherrypy.response.headers['X-Total-Count'] = paginator.get_count()
        return hosts

    @EndpointDoc(
        "Get the hardware of a single host, with per-component data for all categories.",
        parameters={
            'hostname': Param(str, 'The name of the host to retrieve hardware detail for', False),
        },
        responses={200: HARDWARE_HOST_SCHEMA},
    )
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    @handle_custom_error('Hardware', http_status_code=404, exceptions=(HardwareNoReport,))
    def get(self, hostname: str) -> Dict[str, object]:
        return HardwareHostResponse.from_raw(
            OrchClient.instance().hardware.fullreport(hostname)).to_dict()


DIAGNOSTICS_ENTRY_SCHEMA = {
    'hostname': (str, 'Host name'),
    'id': (str, 'Entry id, unique on the host'),
    'name': (str, 'Name'),
    'created': (str, 'When the data was collected (ISO 8601)'),
    'message': (str, 'Description'),
    'size': (int, 'Size in bytes'),
    'drive': (str, 'Drive id, as in the hardware storage data, if known'),
    'diagnostic_data_type': (str, 'Redfish DiagnosticDataType, e.g. Device'),
}

DIAGNOSTICS_TASK_SCHEMA = {
    'hostname': (str, 'Host name'),
    'id': (str, 'Task id'),
    'name': (str, 'Name'),
    'state': (str, 'Redfish TaskState: New, Running, Completed, Exception...'),
    'status': (str, 'Redfish TaskStatus: OK, Warning or Critical'),
    'percent_complete': (int, 'Progress, in percent'),
    'messages': ([str], 'Messages of the task'),
    'entries': ([str], 'Ids of the entries collected'),
}


@APIRouter('/hardware/diagnostics', Scope.HOSTS)
@APIDoc("Hardware diagnostic data API: drive dumps collected by the hardware", "Hardware")
class HardwareDiagnostics(RESTController):
    RESOURCE_ID = 'entry_id'

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("List the diagnostic data (drive dumps) the hardware of the hosts keeps",
                 parameters={'hostname': (str, 'A host, all of them by default')},
                 responses={200: [DIAGNOSTICS_ENTRY_SCHEMA]})
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    def list(self, hostname: Optional[str] = None) -> List[Dict[str, Any]]:
        data = OrchClient.instance().hardware.diagnostics(hostname or None)
        return [_diagnostics_entry(host, entry)
                for host, entries in sorted(data.items()) for entry in entries]

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("Collect the diagnostic data of a drive; returns the collection task",
                 parameters={'hostname': (str, 'Host of the drive'),
                             'device': (str, 'Drive id, as in the hardware storage data')},
                 responses={202: DIAGNOSTICS_TASK_SCHEMA})
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    def create(self, hostname: str, device: str) -> Dict[str, Any]:
        task = OrchClient.instance().hardware.collect_diagnostics(hostname, device)
        cherrypy.response.status = 202  # the collection runs on: follow the task
        return dict(task, hostname=hostname)

    @RESTController.Collection('GET', path='/task', version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("Progress of a diagnostic data collection",
                 parameters={'hostname': (str, 'Host'), 'task_id': (str, 'Task id')},
                 responses={200: DIAGNOSTICS_TASK_SCHEMA})
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    def task(self, hostname: str, task_id: str) -> Dict[str, Any]:
        task = OrchClient.instance().hardware.diagnostics_task(hostname, task_id)
        return dict(task, hostname=hostname)

    @Endpoint('GET', path='/download', query_params=['hostname', 'entry_id'],
              json_response=False, version=None)
    @ReadPermission
    @EndpointDoc("Download diagnostic data (application/octet-stream)",
                 parameters={'hostname': (str, 'Host'), 'entry_id': (str, 'Entry id')})
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    def download(self, hostname: str, entry_id: str):
        filename, content = OrchClient.instance().hardware.download_diagnostics(
            hostname, entry_id)
        # a file object, not bytes: the dashboard would decode bytes as text
        return serve_fileobj(io.BytesIO(content), content_type='application/octet-stream',
                             disposition='attachment',
                             name=f'{hostname}-{filename}'.replace('"', ''))

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("Delete diagnostic data",
                 parameters={'entry_id': (str, 'Entry id'), 'hostname': (str, 'Host')})
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    def delete(self, entry_id: str, hostname: str) -> None:
        OrchClient.instance().hardware.delete_diagnostics(hostname, entry_id)


def _diagnostics_entry(hostname: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    """The fields of an entry the API documents, without Redfish URIs."""
    return dict({key: entry.get(key) for key in DIAGNOSTICS_ENTRY_SCHEMA}, hostname=hostname)


def _report_if_present(hardware: HardwareManager, hostname: str) -> Optional[RawHostReport]:
    """
    The host's node-proxy report, or None if it has none or it is incomplete.

    TOCTOU: a host removed between list_hosts() and this call makes
    fullreport() raise HardwareNoReport; treat it as gone and skip it.
    Any other orchestrator error is a real failure and propagates.
    """
    try:
        return hardware.fullreport(hostname)
    except HardwareNoReport:
        return None
