"""The Prometheus metrics of a host's hardware, from a node-proxy report.

node-proxy reports a host's hardware as::

    {'status': {'storage': {system: {id: {...}}}, 'processors': ...,
                'memory': ..., 'power': ..., 'network': ...,
                'fans': ..., 'temperatures': ...},
     'firmware': {id: {'name': ..., 'version': ...}}}

node-proxy serves these metrics itself at /metrics; the mgr's prometheus
module can export them from the reports it keeps. Both use this module, so
the series, which the alerts and the Grafana dashboards depend on, are the
same whichever exports them.
"""

import math
from typing import Any, Callable, Dict, Iterable, Iterator, NamedTuple, \
    Optional, Tuple

STORAGE_LABELS = ('hostname', 'device', 'model', 'protocol', 'firmware_version', 'slot',
                  'serial_number')
CPU_LABELS = ('hostname', 'cpu', 'manufacturer', 'model', 'total_threads')
MEMORY_LABELS = ('hostname', 'dimm', 'type')
STATUS_LABELS = ('hostname', 'component', 'category', 'state')
TEMP_LABELS = ('hostname', 'sensor_name')
FAN_LABELS = ('hostname', 'fan_name')
FIRMWARE_LABELS = ('hostname', 'component', 'version')

MIB_TO_BYTES = 1048576

HEALTH_STATUS_MAP = {
    'OK': 0,
    'Warning': 1,
    'Degraded': 1,
    'Error': 2,
    'Critical': 2,
    # Health not reported by the BMC
    'unknown': -1,
}


class MetricInfo(NamedTuple):
    mtype: str
    description: str
    labels: Tuple[str, ...]


# the metric names without the 'ceph_' prefix, as the mgr names its metrics
METRICS: Dict[str, MetricInfo] = {
    'hardware_storage_capacity_bytes': MetricInfo(
        'gauge', 'Storage device capacity in bytes', STORAGE_LABELS),
    'hardware_cpu_cores': MetricInfo(
        'gauge', 'Total CPU cores', CPU_LABELS),
    'hardware_memory_capacity_bytes': MetricInfo(
        'gauge', 'Memory capacity in bytes', MEMORY_LABELS),
    'hardware_status': MetricInfo(
        'gauge',
        'Hardware component health (-1=Unknown, 0=OK, 1=Warning, 2=Error), '
        'labelled with the Redfish state of the component',
        STATUS_LABELS),
    'hardware_fan_rpm': MetricInfo(
        'gauge', 'Hardware fan speed in RPM', FAN_LABELS),
    'hardware_temperature_celsius': MetricInfo(
        'gauge', 'Hardware temperature sensor reading in Celsius', TEMP_LABELS),
    'hardware_firmware_info': MetricInfo(
        'gauge', 'Firmware version information (value is always 1, version in label)',
        FIRMWARE_LABELS),
}

# the sensor categories of a report and the metric of their readings
SENSOR_METRICS = {
    'fans': 'hardware_fan_rpm',
    'temperatures': 'hardware_temperature_celsius',
}


class Sample(NamedTuple):
    metric: str
    labels: Tuple[Any, ...]
    value: float


#: called with (health, hostname, component, category) for a health that
#: HEALTH_STATUS_MAP does not know; the component's status is then left out
UnknownHealth = Callable[[str, str, str, str], None]


def hw_value(value: Any) -> float:
    """Numeric value of a node-proxy field, NaN when it is not reported.

    node-proxy reports the fields a Redfish service does not provide as the
    string 'unknown'. NaN keeps the series and its inventory labels while
    exporting the string would fail the whole collection.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return math.nan


def status_value(status: Any, field: str) -> str:
    """A field of a Redfish Status (e.g. state, health), 'unknown' if missing."""
    return status.get(field, 'unknown') if isinstance(status, dict) else 'unknown'


def health_name(status: Any) -> str:
    """The health of a Redfish Status, or the status itself when node-proxy
    reported it as a string."""
    return status if isinstance(status, str) else status_value(status, 'health')


def health_value(status: Any) -> Optional[int]:
    """-1=Unknown, 0=OK, 1=Warning, 2=Error; None for a health not known here."""
    return HEALTH_STATUS_MAP.get(health_name(status))


def _components(status: Dict[str, Any], category: str) -> Iterator[Tuple[str, Dict[str, Any]]]:
    for components in (status.get(category) or {}).values():
        for comp_id, comp in (components or {}).items():
            if isinstance(comp, dict):
                yield comp_id, comp


def samples(hostname: str, report: Dict[str, Any],
            unknown_health: Optional[UnknownHealth] = None) -> Iterator[Sample]:
    """The samples of every hardware metric of a host's report."""
    status = report.get('status') or {}

    def health(comp_status: Any, component: str, category: str) -> Iterator[Sample]:
        value = health_value(comp_status)
        if value is None:
            if unknown_health:
                unknown_health(health_name(comp_status), hostname, component, category)
            return
        yield Sample('hardware_status',
                     (hostname, component, category, status_value(comp_status, 'state')),
                     value)

    firmware = report.get('firmware', report.get('firmwares')) or {}
    for fw_id, fw in firmware.items():
        version = fw.get('version', 'unknown')
        if version and version != 'unknown':
            yield Sample('hardware_firmware_info',
                         (hostname, fw.get('name', fw_id), str(version)), 1)

    for device_id, device in _components(status, 'storage'):
        yield Sample('hardware_storage_capacity_bytes',
                     (hostname, device_id, device.get('model', 'unknown'),
                      device.get('protocol', 'unknown'),
                      device.get('firmware_version', 'unknown'),
                      device.get('slot', 'unknown'), device.get('serial_number', 'unknown')),
                     hw_value(device.get('capacity_bytes')))
        yield from health(device.get('status', {}), device_id, 'storage')

    for cpu_id, cpu in _components(status, 'processors'):
        yield Sample('hardware_cpu_cores',
                     (hostname, cpu_id, cpu.get('manufacturer', 'unknown'),
                      cpu.get('model', 'unknown'), cpu.get('total_threads', 'unknown')),
                     hw_value(cpu.get('total_cores')))
        yield from health(cpu.get('status', {}), cpu_id, 'processors')

    for dimm_id, dimm in _components(status, 'memory'):
        yield Sample('hardware_memory_capacity_bytes',
                     (hostname, dimm_id, dimm.get('memory_device_type', 'unknown')),
                     hw_value(dimm.get('capacity_mi_b')) * MIB_TO_BYTES)
        yield from health(dimm.get('status', {}), dimm_id, 'memory')

    # power supplies by their name (their key is a sensor number), network
    # adapters by their key (their name is a generic NetworkAdapter_<n>)
    for comp_id, comp in _components(status, 'power'):
        yield from health(comp.get('status', {}), comp.get('name', comp_id), 'power')
    for comp_id, comp in _components(status, 'network'):
        yield from health(comp.get('status', {}), comp_id, 'network')

    for category, metric in SENSOR_METRICS.items():
        for comp_id, comp in _components(status, category):
            name = comp.get('name', comp_id)
            reading = comp.get('reading')
            if reading is not None and reading != 'unknown':
                try:
                    yield Sample(metric, (hostname, name), float(reading))
                except (TypeError, ValueError):
                    pass
            yield from health(comp.get('status', {}), name, category)


def _float(value: float) -> str:
    """A Go-compatible float, as Prometheus parses it."""
    if math.isnan(value):
        return 'NaN'
    if math.isinf(value):
        return '+Inf' if value > 0 else '-Inf'
    return repr(float(value))


def _label(value: Any) -> str:
    return str(value).replace('\\', '\\\\').replace('\n', '\\n').replace('"', '\\"')


def render(all_samples: Iterable[Sample], prefix: str = 'ceph_') -> str:
    """The samples in the Prometheus text exposition format (version 0.0.4).

    Every metric of METRICS gets its HELP and TYPE lines, with or without
    samples; a series seen twice keeps its last value.
    """
    values: Dict[str, Dict[Tuple[Any, ...], float]] = {m: {} for m in METRICS}
    for sample in all_samples:
        values[sample.metric][sample.labels] = sample.value
    lines = []
    for metric, series in values.items():
        info = METRICS[metric]
        name = prefix + metric
        lines.append(f'# HELP {name} {info.description}')
        lines.append(f'# TYPE {name} {info.mtype}')
        for labels, value in series.items():
            pairs = ','.join(f'{k}="{_label(v)}"' for k, v in zip(info.labels, labels))
            lines.append(f'{name}{{{pairs}}} {_float(value)}')
    return '\n'.join(lines) + '\n'
