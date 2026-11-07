"""ceph.hardware_metrics: the hardware metrics of a node-proxy report, the
same whether node-proxy or the mgr's prometheus module exports them."""

import math

import pytest

from ceph import hardware_metrics
from ceph.hardware_metrics import METRICS, health_value, render, samples

HOST = 'host.example.com'

REPORT = {
    'host': 'host.example.com',
    'sn': '11S03NK990YM30ZP58E02X',
    'status': {
        'storage': {'Self': {
            'nvme_device0_nsid1': {
                'capacity_bytes': 512110190592,
                'model': 'Micron_2550_MTFDKBK512TGE',
                'protocol': 'NVMe',
                'serial_number': '24424BAA3C40',
                'firmware_version': 'V6MA001',
                'slot': '0',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
        }},
        'processors': {'Self': {
            'devtype1_cpu0': {
                'total_cores': 48,
                'total_threads': 96,
                'model': 'unknown',
                'manufacturer': 'Advanced Micro Devices, Inc.',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
        }},
        'memory': {'Self': {
            'devtype2_dimm0': {
                'memory_device_type': 'DDR5',
                'capacity_mi_b': 131072,
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
        }},
        'power': {'Self': {
            '38': {
                'name': 'PSU1_PIN',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
            '39': {
                'name': 'PSU1_POUT',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
        }},
        'network': {'Self': {
            'networkinterfaces_devtype7_nic0': {
                'name': 'NetworkAdapter_0',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
        }},
        'fans': {'Self': {
            '0': {
                'name': 'FAN1_TACH_IN',
                'reading': 23940,
                'reading_units': 'RPM',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
        }},
        'temperatures': {'Self': {
            '0': {
                'name': 'C1_DCSCM_TEMP',
                'reading': 32,
                'reading_units': 'Cel',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
            '6': {
                'name': 'C1_CPU_TEMP',
                'reading': 47,
                'reading_units': 'Cel',
                'status': {'health': 'OK', 'state': 'Enabled'},
            },
        }},
    },
    'firmware': {
        'bios': {
            'name': 'BIOS',
            'version': 'unknown',
        },
        'bmc': {
            'name': 'BMC',
            'version': '0.50.d57cfd',
        },
        'cpld': {
            'name': 'CPLD',
            'version': 'unknown',
        },
    },
}


def series(report, hostname=HOST, unknown_health=None):
    result = {}
    for sample in samples(hostname, report, unknown_health):
        result.setdefault(sample.metric, {})[sample.labels] = sample.value
    return result


def test_labels_match_the_metrics():
    for metric, values in series(REPORT).items():
        for labels in values:
            assert len(labels) == len(METRICS[metric].labels), metric


@pytest.mark.parametrize('status, value', [
    ({'health': 'OK'}, 0),
    ({'health': 'Warning'}, 1),
    ({'health': 'Degraded'}, 1),
    ({'health': 'Critical'}, 2),
    ('OK', 0),
    ({'health': 'unknown'}, -1),
    ({}, -1),
    ({'health': 'Exploded'}, None),
])
def test_health_value(status, value):
    assert health_value(status) == value


def test_unknown_health_is_reported_and_left_out():
    seen = []
    report = {'status': {'storage': {'1': {'d1': {'status': {'health': 'Exploded'}}}}}}
    values = series(report, unknown_health=lambda *args: seen.append(args))
    assert seen == [('Exploded', HOST, 'd1', 'storage')]
    assert 'hardware_status' not in values


@pytest.mark.parametrize('status, state, value', [
    ({'health': 'Warning', 'state': 'Enabled'}, 'Enabled', 1),
    ({'health': 'unknown', 'state': 'Disabled'}, 'Disabled', -1),
    ({'health': 'OK'}, 'unknown', 0),
    # node-proxy turns a null Redfish Status into the string 'unknown'
    ('unknown', 'unknown', -1),
])
def test_status_state_label(status, state, value):
    report = {'status': {'fans': {'1': {'0': {'name': 'FAN1', 'status': status}}}}}
    assert series(report)['hardware_status'] == {(HOST, 'FAN1', 'fans', state): value}


def test_storage():
    values = series(REPORT)
    labels = (HOST, 'nvme_device0_nsid1', 'Micron_2550_MTFDKBK512TGE', 'NVMe', 'V6MA001',
              '0', '24424BAA3C40')
    assert values['hardware_storage_capacity_bytes'][labels] == 512110190592
    assert values['hardware_status'][(HOST, 'nvme_device0_nsid1', 'storage', 'Enabled')] == 0


def test_processors_and_memory():
    values = series(REPORT)
    cpu = (HOST, 'devtype1_cpu0', 'Advanced Micro Devices, Inc.', 'unknown', 96)
    assert values['hardware_cpu_cores'][cpu] == 48
    assert values['hardware_memory_capacity_bytes'][(HOST, 'devtype2_dimm0', 'DDR5')] == \
        131072 * 1048576


def test_power_by_name_network_by_id():
    health = series(REPORT)['hardware_status']
    assert (HOST, 'PSU1_PIN', 'power', 'Enabled') in health
    assert (HOST, 'PSU1_POUT', 'power', 'Enabled') in health
    assert (HOST, '38', 'power', 'Enabled') not in health
    assert (HOST, 'networkinterfaces_devtype7_nic0', 'network', 'Enabled') in health
    assert (HOST, 'NetworkAdapter_0', 'network', 'Enabled') not in health


def test_sensors_by_name():
    values = series(REPORT)
    assert values['hardware_temperature_celsius'] == {
        (HOST, 'C1_DCSCM_TEMP'): 32.0, (HOST, 'C1_CPU_TEMP'): 47.0}
    assert values['hardware_fan_rpm'] == {(HOST, 'FAN1_TACH_IN'): 23940.0}
    health = values['hardware_status']
    assert (HOST, 'C1_CPU_TEMP', 'temperatures', 'Enabled') in health
    assert (HOST, '6', 'temperatures', 'Enabled') not in health


def test_sensor_without_reading_keeps_its_health():
    report = {'status': {'temperatures': {'Self': {'99': {
        'name': 'GHOST_TEMP', 'reading': 'unknown',
        'status': {'health': 'OK', 'state': 'Enabled'}}}}}}
    values = series(report)
    assert 'hardware_temperature_celsius' not in values
    assert (HOST, 'GHOST_TEMP', 'temperatures', 'Enabled') in values['hardware_status']


def test_firmware_with_a_version():
    assert series(REPORT)['hardware_firmware_info'] == {(HOST, 'BMC', '0.50.d57cfd'): 1}


def test_unreported_values_are_nan():
    report = {'status': {
        'storage': {'1': {'driveslot-2': {
            'capacity_bytes': 'unknown', 'model': 'FCM4', 'serial_number': 'FCM0002',
            'status': {'health': 'Warning', 'state': 'Enabled'}}}},
        'processors': {'1': {'cpu-1': {'total_cores': 'unknown', 'status': 'unknown'}}},
        'memory': {'1': {'dimm1': {'capacity_mi_b': None}}},
        'fans': {'1': {'0': {'name': 'FAN1', 'reading': 'fast'}}},
    }}
    values = series(report)
    for metric in ('hardware_storage_capacity_bytes', 'hardware_cpu_cores',
                   'hardware_memory_capacity_bytes'):
        (value,) = values[metric].values()
        assert math.isnan(value), metric
    assert 'hardware_fan_rpm' not in values
    assert values['hardware_status'][(HOST, 'driveslot-2', 'storage', 'Enabled')] == 1


def test_numeric_strings():
    report = {'status': {'storage': {'1': {'slot-1': {'capacity_bytes': '1000'}}}}}
    (value,) = series(report)['hardware_storage_capacity_bytes'].values()
    assert value == 1000.0


def test_render():
    text = render(samples(HOST, REPORT))
    assert text.endswith('\n')
    for metric, info in METRICS.items():
        assert f'# HELP ceph_{metric} {info.description}\n' in text
        assert f'# TYPE ceph_{metric} gauge\n' in text
    assert 'ceph_hardware_fan_rpm{hostname="host.example.com",fan_name="FAN1_TACH_IN"} ' \
        '23940.0\n' in text
    assert 'ceph_hardware_cpu_cores{hostname="host.example.com",cpu="devtype1_cpu0",' \
        'manufacturer="Advanced Micro Devices, Inc.",model="unknown",total_threads="96"} ' \
        '48.0\n' in text


def test_render_escapes_and_special_values():
    text = render([
        hardware_metrics.Sample('hardware_storage_capacity_bytes',
                                (HOST, 'd1', 'say "hi"\\\n', 'NVMe', 'x', 1, 'S'),
                                math.nan),
        hardware_metrics.Sample('hardware_fan_rpm', (HOST, 'f'), math.inf),
    ])
    assert 'model="say \\"hi\\"\\\\\\n"' in text
    assert '",serial_number="S"} NaN\n' in text
    assert 'ceph_hardware_fan_rpm{hostname="host.example.com",fan_name="f"} +Inf\n' in text
