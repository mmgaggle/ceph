from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.network import interface_details
from ceph_node_proxy.redfish import build_data


@pytest.fixture
def system() -> BaseRedfishSystem:
    with (
        patch(
            "ceph_node_proxy.baseredfishsystem.RedFishClient", return_value=MagicMock()
        ),
        patch(
            "ceph_node_proxy.baseredfishsystem.EndpointMgr", return_value=MagicMock()
        ),
    ):
        return BaseRedfishSystem(
            host="testhost", port="443", username="user", password="secret", config={}
        )


# Resources as IBM HAL serves them for a CS1 canister: a port of a
# ConnectX-6 Dx in slot 51, member of an 802.3ad bond, with a QSFP28 AOC
SYSTEM = "/redfish/v1/Systems/CS1CAN01"
SLOT51 = "/redfish/v1/Chassis/Canister-1/NetworkAdapters/Slot-51"
PORT = SLOT51 + "/Ports/ens51f1np1"

CONDITION = {
    "MessageId": "NetworkDevice.1.0.ConnectionDropped",
    "Message": "The connection is no longer active for network adapter 'Slot-51' "
    "port 'ens51f1np1' function '1'.",
    "MessageArgs": ["Slot-51", "ens51f1np1", "1"],
    "Severity": "Warning",
    "Timestamp": "2026-11-06T00:00:00Z",
}

INTERFACE: Dict[str, Any] = {
    "@odata.id": SYSTEM + "/EthernetInterfaces/ens51f1np1",
    "Id": "ens51f1np1",
    "Name": "ens51f1np1",
    "Description": "Port 2 of Mellanox Technologies MT2892 Family [ConnectX-6 Dx] "
    "in PCIe slot 51",
    "EthernetInterfaceType": "Physical",
    "InterfaceEnabled": True,
    "LinkStatus": "LinkDown",
    "SpeedMbps": None,
    "MTUSize": 9000,
    "MACAddress": "b8:ce:f6:34:03:ea",
    "Status": {
        "State": "Enabled",
        "Health": "Warning",
        "HealthRollup": "Warning",
        "Conditions": [CONDITION],
    },
    "Links": {
        "Ports": [{"@odata.id": PORT}],
        "AffiliatedInterfaces": [{"@odata.id": SYSTEM + "/EthernetInterfaces/bond0"}],
    },
}

RESOURCES: Dict[str, Dict[str, Any]] = {
    PORT: {
        "@odata.id": PORT,
        "Id": "ens51f1np1",
        "LinkStatus": "LinkDown",
        "MaxSpeedGbps": 100.0,
        "LinkTransitionIndicator": 8,
        "PortMedium": "Optical",
        "Location": {"PartLocation": {"ServiceLabel": "PCIe slot 51 port 2"}},
        "Ethernet": {
            "LLDPEnabled": True,
            "LLDPReceive": {
                "ChassisId": "c8:d9:95:88:ee:21",
                # the octets of the interface name et-0/0/0:1
                "PortId": "65:74:2D:30:2F:30:2F:30:3A:31",
                "PortIdSubtype": "IfName",
                "SystemName": "ceph-perf-bottom",
                "ManagementAddressIPv4": "9.11.120.7",
            },
        },
        "SFP": {
            "Type": "QSFP28",
            "MediumType": "FiberOptic",
            "FiberConnectionType": "MultiMode",
            "Manufacturer": "JUNIPER-1W2",
            "PartNumber": "1W2WUA",
            "SerialNumber": "1W2TWUAA33002",
            "Status": {"State": "Enabled", "Health": "OK"},
        },
        "Metrics": {"@odata.id": PORT + "/Metrics"},
        "EnvironmentMetrics": {"@odata.id": PORT + "/EnvironmentMetrics"},
        "Status": {"State": "Enabled", "Health": "Warning", "Conditions": [CONDITION]},
    },
    PORT
    + "/Metrics": {
        "RXBytes": 524684571418,
        "TXBytes": 1947722787884,
        "RXErrors": 0,
        "TXErrors": 3,
        "Networking": {"RXFCSErrors": 0, "RXDiscards": 2, "TXDiscards": 0},
        "Transceivers": [
            {
                "SupplyVoltage": 3.34,
                "WavelengthNanometers": "850",
                "ByLane": [
                    {
                        "LaneId": 1,
                        "RXInputPowerMilliWatts": 0.97,
                        "TXOutputPowerMilliWatts": 0.98,
                        "TXBiasCurrentMilliAmps": 7.0,
                    },
                    {
                        "LaneId": 2,
                        "RXInputPowerMilliWatts": 0.92,
                        "TXOutputPowerMilliWatts": 0.97,
                        "TXBiasCurrentMilliAmps": 6.9,
                    },
                ],
            }
        ],
    },
    PORT + "/EnvironmentMetrics": {"TemperatureCelsius": {"Reading": 68.04}},
}


def fetch(url: str) -> Dict[str, Any]:
    return RESOURCES.get(url, {})


class TestInterfaceDetails:
    def test_port_interface(self) -> None:
        details = interface_details(INTERFACE, fetch)
        assert details["link_status"] == "LinkDown"
        assert details["interface_type"] == "Physical"
        assert details["mtu_size"] == 9000
        assert details["affiliated_interfaces"] == ["bond0"]
        assert details["related_interfaces"] == []
        assert details["port"] == PORT
        assert details["max_speed_gbps"] == 100.0
        assert details["link_transitions"] == 8
        assert details["location"] == "PCIe slot 51 port 2"
        assert details["lldp"] == {
            "system_name": "ceph-perf-bottom",
            "chassis_id": "c8:d9:95:88:ee:21",
            "port_id": "et-0/0/0:1",
            "management_address": "9.11.120.7",
            "system_description": None,
        }
        assert details["sfp"]["type"] == "QSFP28"
        assert details["sfp"]["health"] == "OK"
        assert (details["rx_errors"], details["tx_errors"]) == (0, 3)
        assert details["rx_discards"] == 2
        assert details["transceiver"]["rx_power_mw"] == [0.97, 0.92]
        assert details["transceiver"]["tx_bias_ma"] == [7.0, 6.9]
        assert details["transceiver_temperature_c"] == 68.04
        # the port's condition is the interface's: listed once
        assert details["conditions"] == [
            {
                "message_id": "NetworkDevice.1.0.ConnectionDropped",
                "message": CONDITION["Message"],
                "severity": "Warning",
                "timestamp": "2026-11-06T00:00:00Z",
            }
        ]

    def test_bond_and_vlan(self) -> None:
        bond = {
            "EthernetInterfaceType": "Virtual",
            "TeamMode": "IEEE802_3ad",
            "LinkStatus": "LinkUp",
            "Links": {
                "RelatedInterfaces": [
                    {"@odata.id": SYSTEM + "/EthernetInterfaces/ens51f0np0"},
                    {"@odata.id": SYSTEM + "/EthernetInterfaces/ens51f1np1"},
                ],
                "AffiliatedInterfaces": [
                    {"@odata.id": SYSTEM + "/EthernetInterfaces/vlan1305"}
                ],
            },
        }
        details = interface_details(bond, fetch)
        assert details["team_mode"] == "IEEE802_3ad"
        assert details["related_interfaces"] == ["ens51f0np0", "ens51f1np1"]
        assert details["affiliated_interfaces"] == ["vlan1305"]
        assert "port" not in details
        vlan = {
            "VLAN": {"VLANEnable": True, "VLANId": 1305},
            "IPv4Addresses": [{"Address": "10.64.9.54", "SubnetMask": "255.255.254.0"}],
            "IPv6Addresses": [
                {
                    "Address": "fe80::1",
                    "PrefixLength": 64,
                    "AddressOrigin": "LinkLocal",
                },
                {
                    "Address": "2001:db8::54",
                    "PrefixLength": 64,
                    "AddressOrigin": "Static",
                },
            ],
        }
        details = interface_details(vlan, fetch)
        assert details["vlan_id"] == 1305
        assert details["addresses"] == ["10.64.9.54/23", "2001:db8::54/64"]

    def test_plain_service(self) -> None:
        # a BMC that has none of the links: the interface's own properties
        details = interface_details(
            {"Id": "NIC.Integrated.1-1-1", "LinkStatus": "LinkUp", "MTUSize": 1500},
            fetch,
        )
        assert details["link_status"] == "LinkUp"
        assert details["conditions"] == []
        assert "lldp" not in details

    def test_port_without_neighbor_or_transceiver(self) -> None:
        port = {"@odata.id": PORT, "LinkStatus": "NoLink", "PortMedium": "Electrical"}
        details = interface_details(INTERFACE, lambda url: port if url == PORT else {})
        assert details["port_link_status"] == "NoLink"
        assert "lldp" not in details
        assert "sfp" not in details
        assert "transceiver" not in details

    def test_unreadable_port(self) -> None:
        interface = dict(
            INTERFACE, Links={"Ports": [{"@odata.id": SLOT51 + "/Ports/gone"}]}
        )
        details = interface_details(interface, fetch)
        assert "port" not in details
        assert len(details["conditions"]) == 1


class TestLldpPortId:
    def test_decoded(self) -> None:
        from ceph_node_proxy.network import lldp_port_id

        assert lldp_port_id("65:74:2D:30:2F:30:2F:30:3A:31", "IfName") == "et-0/0/0:1"
        assert lldp_port_id("01:0A:02:00:3F", "NetworkAddr") == "10.2.0.63"
        # a MAC address stays one; text is kept as received
        assert lldp_port_id("C8:D9:95:89:1B:21", "MacAddr") == "C8:D9:95:89:1B:21"
        assert lldp_port_id("xe-0/0/38", "IfName") == "xe-0/0/38"
        assert lldp_port_id("FF:FE", "IfName") == "FF:FE"
        assert lldp_port_id(None, "IfName") is None


class TestEnrich:
    def test_build_data_adds_the_details(self) -> None:
        result = build_data(
            data={"ens51f1np1": INTERFACE},
            fields=["Name", "Id", "SpeedMbps", "Status"],
            log=MagicMock(),
            enrich=lambda member: interface_details(member, fetch),
        )
        record = result["ens51f1np1"]
        assert record["status"]["health"] == "Warning"
        assert record["lldp"]["system_name"] == "ceph-perf-bottom"
        # None is reported as unknown, like the other properties
        assert record["speed_mbps"] == "unknown"

    def test_build_data_survives_a_failing_enrich(self) -> None:
        def fail(member: Dict[str, Any]) -> Dict[str, Any]:
            raise KeyError("Links")

        log = MagicMock()
        result = build_data(
            data={"ens51f1np1": INTERFACE}, fields=["Name"], log=log, enrich=fail
        )
        assert result["ens51f1np1"]["name"] == "ens51f1np1"
        assert "Can't add the details of ens51f1np1" in log.error.call_args[0][0]

    def test_system_follows_the_links(self, system: Any) -> None:
        system._get_path = lambda path: RESOURCES[path]
        details = system.network_details(INTERFACE)
        assert details["sfp"]["part_number"] == "1W2WUA"

    def test_system_fetch_failure_is_empty(self, system: Any) -> None:
        def fail(path: str) -> Dict[str, Any]:
            raise RuntimeError(path)

        system._get_path = fail
        assert system._fetch(PORT) == {}
        assert "port" not in system.network_details(INTERFACE)

    def test_network_spec_enriches(self, system: Any) -> None:
        spec = system.get_specs("network")[0]
        assert (spec.path, spec.enrich) == ("EthernetInterfaces", "network_details")
