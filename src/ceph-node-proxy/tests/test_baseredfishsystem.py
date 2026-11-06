import json
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

import pytest

from ceph_node_proxy.baseredfishsystem import (
    BaseRedfishSystem,
    fill_drive_slot_and_firmware,
)
from ceph_node_proxy.redfish import ComponentUpdateSpec


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def mock_endpoints():
    return MagicMock()


@pytest.fixture
def system(mock_client, mock_endpoints):
    with (
        patch(
            "ceph_node_proxy.baseredfishsystem.RedFishClient", return_value=mock_client
        ),
        patch(
            "ceph_node_proxy.baseredfishsystem.EndpointMgr", return_value=mock_endpoints
        ),
    ):
        return BaseRedfishSystem(
            host="testhost",
            port="443",
            username="user",
            password="secret",
            config={},
        )


class TestBaseRedfishSystemInit:
    def test_init_sets_client_and_endpoints(self, system, mock_client, mock_endpoints):
        assert system.client is mock_client
        assert system.endpoints is mock_endpoints

    def test_init_default_component_list(self, system):
        assert "memory" in system.component_list
        assert "power" in system.component_list
        assert "network" in system.component_list
        assert "processors" in system.component_list
        assert "storage" in system.component_list
        assert "firmware" in system.component_list
        assert "fans" in system.component_list
        assert "temperatures" in system.component_list

    def test_init_update_funcs_populated(self, system):
        # Should have one callable per component that has _update_<name>
        assert len(system.update_funcs) >= 6

    def test_init_custom_component_list(self, mock_client, mock_endpoints):
        with (
            patch(
                "ceph_node_proxy.baseredfishsystem.RedFishClient",
                return_value=mock_client,
            ),
            patch(
                "ceph_node_proxy.baseredfishsystem.EndpointMgr",
                return_value=mock_endpoints,
            ),
        ):
            sys = BaseRedfishSystem(
                host="h",
                port="443",
                username="u",
                password="p",
                config={},
                component_list=["memory", "network"],
            )
        assert sys.component_list == ["memory", "network"]
        assert len(sys.update_funcs) == 2


class TestBaseRedfishSystemGetters:
    def test_get_sn_empty(self, system):
        assert system.get_sn() == ""

    def test_get_sn_from_sys(self, system):
        system._sys["SN"] = "ABC123"
        assert system.get_sn() == "ABC123"

    def test_get_memory_empty(self, system):
        assert system.get_memory() == {}

    def test_get_memory_from_sys(self, system):
        system._sys["memory"] = {"slot1": {"capacity_mib": 16384}}
        assert system.get_memory() == {"slot1": {"capacity_mib": 16384}}

    def test_get_processors_empty(self, system):
        assert system.get_processors() == {}

    def test_get_network_empty(self, system):
        assert system.get_network() == {}

    def test_get_storage_empty(self, system):
        assert system.get_storage() == {}

    def test_get_power_empty(self, system):
        assert system.get_power() == {}

    def test_get_fans_empty(self, system):
        assert system.get_fans() == {}

    def test_get_temperatures_empty(self, system):
        assert system.get_temperatures() == {}

    def test_get_firmware_empty(self, system):
        assert system.get_firmware() == {}

    def test_get_status_empty(self, system):
        assert system.get_status() == {}


class TestBaseRedfishSystemGetSystem:
    def test_get_system_structure(self, system):
        system._sys["SN"] = "SN1"
        system._sys["memory"] = {}
        system._sys["processors"] = {}
        system._sys["network"] = {}
        system._sys["storage"] = {}
        system._sys["power"] = {}
        system._sys["fans"] = {}
        system._sys["temperatures"] = {}
        system._sys["firmware"] = {}
        result = system.get_system()
        assert "host" in result
        assert "sn" in result
        assert result["sn"] == "SN1"
        assert "status" in result
        assert result["status"]["memory"] == {}
        assert result["status"]["processors"] == {}
        assert result["status"]["network"] == {}
        assert result["status"]["storage"] == {}
        assert result["status"]["power"] == {}
        assert result["status"]["fans"] == {}
        assert result["status"]["temperatures"] == {}
        assert "fcm" not in result["status"]
        assert "firmware" in result

    def test_get_fcm_empty(self, system):
        assert system.get_fcm() == {}


class TestBaseRedfishSystemGetSpecs:

    def test_get_specs_network(self, system):
        specs = system.get_specs("network")
        assert len(specs) == 2
        assert all(isinstance(s, ComponentUpdateSpec) for s in specs)
        paths = [s.path for s in specs]
        assert "EthernetInterfaces" in paths
        assert "NetworkInterfaces" in paths
        assert specs[0].collection == "systems"
        assert "Name" in specs[0].fields
        assert specs[0].attribute is None

    def test_get_specs_memory(self, system):
        specs = system.get_specs("memory")
        assert len(specs) == 1
        assert specs[0].collection == "systems"
        assert specs[0].path == "Memory"
        assert "CapacityMiB" in specs[0].fields
        assert "Description" in specs[0].fields
        assert "Id" in specs[0].fields

    def test_get_specs_power(self, system):
        specs = system.get_specs("power")
        assert len(specs) == 1
        assert specs[0].collection == "chassis"
        assert "PowerSubsystem" in specs[0].path
        assert specs[0].attribute is None

    def test_get_specs_fans(self, system):
        specs = system.get_specs("fans")
        assert len(specs) == 1
        assert specs[0].collection == "chassis"
        assert specs[0].path == "Thermal"
        assert specs[0].attribute == "Fans"
        assert "Reading" in specs[0].fields

    def test_get_specs_temperatures(self, system):
        specs = system.get_specs("temperatures")
        assert len(specs) == 1
        assert specs[0].collection == "chassis"
        assert specs[0].path == "Thermal"
        assert specs[0].attribute == "Temperatures"
        assert "Reading" in specs[0].fields

    def test_get_component_spec_overrides_empty(self, system):
        assert system.get_component_spec_overrides() == {}


class TestBaseRedfishSystemFlush:
    def test_flush_clears_state(self, system):
        system._system = {"x": 1}
        system.previous_data = {"y": 2}
        system.data_ready = True
        system.flush()
        assert system._system == {}
        assert system.previous_data == {}
        assert system.data_ready is False


class TestBaseRedfishSystemUpdate:
    def test_update_calls_update_component(self, system):
        with patch("ceph_node_proxy.baseredfishsystem.update_component") as mock_update:
            system.update(
                collection="systems",
                component="memory",
                path="Memory",
                fields=BaseRedfishSystem.MEMORY_FIELDS,
                attribute=None,
            )
            mock_update.assert_called_once()
            call_kw = mock_update.call_args[1]
            assert call_kw.get("attribute") is None
            assert mock_update.call_args[0][1] == "systems"
            assert mock_update.call_args[0][2] == "memory"
            assert mock_update.call_args[0][3] == "Memory"


class TestBaseRedfishSystemNotImplemented:
    @pytest.mark.parametrize(
        "method,args",
        [
            ("shutdown_host", (False,)),
            ("powercycle", ()),
        ],
    )
    def test_not_implemented(self, system, method, args):
        with pytest.raises(NotImplementedError):
            getattr(system, method)(*args)


class TestBaseRedfishSystemComponentSpecs:
    def test_component_specs_has_expected_keys(self):
        assert "network" in BaseRedfishSystem.COMPONENT_SPECS
        assert "processors" in BaseRedfishSystem.COMPONENT_SPECS
        assert "memory" in BaseRedfishSystem.COMPONENT_SPECS
        assert "power" in BaseRedfishSystem.COMPONENT_SPECS
        assert "fans" in BaseRedfishSystem.COMPONENT_SPECS
        assert "temperatures" in BaseRedfishSystem.COMPONENT_SPECS
        assert "firmware" in BaseRedfishSystem.COMPONENT_SPECS

    def test_field_lists_non_empty(self):
        assert len(BaseRedfishSystem.NETWORK_FIELDS) > 0
        assert len(BaseRedfishSystem.MEMORY_FIELDS) > 0
        assert len(BaseRedfishSystem.POWER_FIELDS) > 0


class TestFillMissingIdentityDuringUpdate:
    def test_run_update_keeps_existing_description(self, system):
        with patch("ceph_node_proxy.baseredfishsystem.get_component_data") as mock_gcd:
            mock_gcd.return_value = {
                "Self": {
                    "dimm0": {
                        "id": "DevType2_DIMM0",
                        "description": "DIMM A1",
                        "memory_device_type": "DDR4",
                        "status": {"health": "OK", "state": "Enabled"},
                    }
                }
            }
            system._run_update("memory")
        assert system._sys["memory"]["Self"]["dimm0"]["description"] == "DIMM A1"

    def test_run_update_fills_unknown_description_from_id(self, system):
        with patch("ceph_node_proxy.baseredfishsystem.get_component_data") as mock_gcd:
            mock_gcd.return_value = {
                "Self": {
                    "dimm0": {
                        "id": "DevType2_DIMM0",
                        "description": "unknown",
                        "memory_device_type": "DDR4",
                        "status": {"health": "OK", "state": "Enabled"},
                    }
                }
            }
            system._run_update("memory")
        assert (
            system._sys["memory"]["Self"]["dimm0"]["description"] == "DevType2_DIMM0"
        )

    def test_run_update_fills_unknown_description_from_member_key(self, system):
        with patch("ceph_node_proxy.baseredfishsystem.get_component_data") as mock_gcd:
            mock_gcd.return_value = {
                "Self": {
                    "DevType2_DIMM3": {
                        "id": "unknown",
                        "description": "unknown",
                        "memory_device_type": "DDR4",
                        "status": {"health": "OK", "state": "Enabled"},
                    }
                }
            }
            system._run_update("memory")
        assert (
            system._sys["memory"]["Self"]["DevType2_DIMM3"]["description"]
            == "DevType2_DIMM3"
        )

    def test_run_update_fills_unknown_name_from_id(self, system):
        with patch("ceph_node_proxy.baseredfishsystem.get_component_data") as mock_gcd:
            mock_gcd.return_value = {
                "Self": {
                    "nic1": {
                        "id": "NIC.Slot.1",
                        "name": "unknown",
                        "description": "unknown",
                        "speed_mbps": 25000,
                        "status": {"health": "OK", "state": "Enabled"},
                    }
                }
            }
            system._run_update("network")
        nic = system._sys["network"]["Self"]["ethernetinterfaces_nic1"]
        assert nic["name"] == "NIC.Slot.1"
        assert nic["description"] == "NIC.Slot.1"


# A Drive resource as served by a generic Redfish service (e.g. IBM HAL)
HAL_DRIVE = {
    "@odata.id": "/redfish/v1/Chassis/ComputeModule-0/Drives/DriveSlot-2",
    "Id": "DriveSlot-2",
    "Description": "Enclosure0:Drive2",
    "CapacityBytes": 3840755982336,
    "Model": "101-FCM4",
    "Protocol": "NVMe",
    "SerialNumber": "FCM0002",
    "Revision": "4.1.2",
    "LocationIndicatorActive": True,
    "PhysicalLocation": {
        "PartLocation": {
            "ServiceLabel": "Enclosure0:Drive2",
            "LocationType": "Slot",
            "LocationOrdinalValue": 2,
        }
    },
    "Status": {"State": "Enabled", "Health": "OK"},
}


def storage_endpoints(drives):
    """endpoints['systems'] with one system 'CAN01' whose Storage 'Enclosure0'
    links the given drives."""
    storage = MagicMock()
    storage.get_members_data.return_value = {
        "Enclosure0": {"Drives": [{"@odata.id": d["@odata.id"]} for d in drives]}
    }
    system_member = MagicMock()
    system_member.__getitem__.side_effect = lambda key: {"Storage": storage}[key]
    systems = MagicMock()
    systems.get_members_names.return_value = ["CAN01"]
    systems.__getitem__.side_effect = lambda key: {"CAN01": system_member}[key]
    return systems


class TestStorageSlotAndFirmware:
    def test_update_storage_sets_slot_and_firmware(self, system, mock_endpoints):
        mock_endpoints.__getitem__.side_effect = lambda key: {
            "systems": storage_endpoints([HAL_DRIVE])
        }[key]
        with patch("ceph_node_proxy.baseredfishsystem.Endpoint") as mock_endpoint:
            mock_endpoint.return_value.data = HAL_DRIVE
            system._update_storage()
        drive = system.get_storage()["CAN01"]["driveslot-2"]
        assert drive["slot"] == 2
        assert drive["firmware_version"] == "4.1.2"
        assert drive["capacity_bytes"] == 3840755982336
        assert drive["location_indicator_active"] is True
        assert drive["redfish_endpoint"] == HAL_DRIVE["@odata.id"]

    def test_update_storage_reads_encryption(self, system, mock_endpoints):
        locked = dict(
            HAL_DRIVE,
            EncryptionAbility="SelfEncryptingDrive",
            EncryptionStatus="Locked",
            SecurityIDOwned=True,
            Status={
                "State": "Enabled",
                "Health": "Warning",
                "Conditions": [
                    {
                        "MessageId": "IBMEncryption.1.0.KeyManagerUnreachable",
                        "Message": "The drive is locked and HAL cannot read its "
                        "credential from Vault: connection refused.",
                        "Severity": "Warning",
                    }
                ],
            },
        )
        mock_endpoints.__getitem__.side_effect = lambda key: {
            "systems": storage_endpoints([locked])
        }[key]
        with patch("ceph_node_proxy.baseredfishsystem.Endpoint") as mock_endpoint:
            mock_endpoint.return_value.data = locked
            system._update_storage()
        drive = system.get_storage()["CAN01"]["driveslot-2"]
        assert drive["encryption_ability"] == "SelfEncryptingDrive"
        assert drive["encryption_status"] == "Locked"
        assert drive["security_id_owned"] is True
        assert drive["conditions"][0]["message"].startswith("The drive is locked")
        assert drive["conditions"][0]["severity"] == "Warning"

    def test_drive_without_encryption(self, system, mock_endpoints):
        mock_endpoints.__getitem__.side_effect = lambda key: {
            "systems": storage_endpoints([HAL_DRIVE])
        }[key]
        with patch("ceph_node_proxy.baseredfishsystem.Endpoint") as mock_endpoint:
            mock_endpoint.return_value.data = HAL_DRIVE
            system._update_storage()
        drive = system.get_storage()["CAN01"]["driveslot-2"]
        assert drive["encryption_status"] == "unknown"
        assert drive["conditions"] == []

    def test_unknown_values_are_not_used(self):
        drive = {
            "revision": "unknown",
            "physical_location": "unknown",
        }
        fill_drive_slot_and_firmware(drive)
        assert "firmware_version" not in drive
        assert "slot" not in drive

    def test_provider_values_are_kept(self):
        drive = {
            "revision": "4.1.2",
            "firmware_version": "V6MA001",
            "slot": "0",
            "physical_location": {"partlocation": {"locationordinalvalue": 7}},
        }
        fill_drive_slot_and_firmware(drive)
        assert drive["firmware_version"] == "V6MA001"
        assert drive["slot"] == "0"


class TestIdentifyLeds:
    @pytest.fixture
    def leds(self, system, mock_client, mock_endpoints):
        system._sys["storage"] = {
            "CAN01": {
                "driveslot-2": {"redfish_endpoint": HAL_DRIVE["@odata.id"]},
            },
            "CAN02": {
                "slot-17": {
                    "redfish_endpoint": "/redfish/v1/Chassis/ComposableModule-1/Drives/Slot-17"
                },
            },
        }
        systems = MagicMock()
        systems.get_members_names.return_value = ["CAN01"]
        chassis = MagicMock()
        chassis.get_members_endpoints.return_value = {
            "Canister-1": "/redfish/v1/Chassis/Canister-1",
            "ComputeModule-0": "/redfish/v1/Chassis/ComputeModule-0",
        }
        mock_endpoints.__getitem__.side_effect = lambda key: {
            "systems": systems,
            "chassis": chassis,
        }[key]
        return system

    def test_drive_endpoint_found_in_any_system(self, leds):
        assert leds.get_drive_endpoint("driveslot-2") == HAL_DRIVE["@odata.id"]
        assert leds.get_drive_endpoint("slot-17").endswith("/Drives/Slot-17")
        with pytest.raises(KeyError):
            leds.get_drive_endpoint("nope")

    @pytest.mark.parametrize(
        "method,expected", [("device_led_on", True), ("device_led_off", False)]
    )
    def test_device_led_on_off(self, leds, mock_client, method, expected):
        mock_client.query.return_value = ({}, "", 204)
        assert getattr(leds, method)("driveslot-2") == 204
        mock_client.query.assert_called_once_with(
            data=json.dumps({"LocationIndicatorActive": expected}),
            method="PATCH",
            endpoint=HAL_DRIVE["@odata.id"],
        )

    def test_get_device_led(self, leds, mock_client):
        mock_client.query.return_value = ({}, json.dumps(HAL_DRIVE), 200)
        assert leds.get_device_led("driveslot-2") == {
            "http_code": 200,
            "LocationIndicatorActive": True,
        }
        mock_client.query.assert_called_once_with(
            method="GET", endpoint=HAL_DRIVE["@odata.id"]
        )

    def test_get_device_led_not_reported(self, leds, mock_client):
        mock_client.query.return_value = ({}, json.dumps({"Id": "Slot-17"}), 200)
        assert leds.get_device_led("slot-17")["LocationIndicatorActive"] is None

    def test_unknown_device(self, leds, mock_client):
        with pytest.raises(KeyError):
            leds.device_led_on("nope")
        mock_client.query.assert_not_called()

    def test_chassis_led_uses_the_system_chassis_link(self, leds, mock_client):
        leds._system["CAN01"] = {
            "Links": {"Chassis": [{"@odata.id": "/redfish/v1/Chassis/ComputeModule-0"}]}
        }
        mock_client.query.return_value = ({}, "", 204)
        assert leds.chassis_led_on() == 204
        assert mock_client.query.call_args.kwargs["endpoint"] == (
            "/redfish/v1/Chassis/ComputeModule-0"
        )

    def test_chassis_led_falls_back_to_first_chassis(self, leds, mock_client):
        mock_client.query.return_value = (
            {},
            json.dumps({"LocationIndicatorActive": False}),
            200,
        )
        assert leds.get_chassis_led()["LocationIndicatorActive"] is False
        assert mock_client.query.call_args.kwargs["endpoint"] == (
            "/redfish/v1/Chassis/Canister-1"
        )

    def test_redfish_errors_are_raised(self, leds, mock_client):
        mock_client.query.side_effect = HTTPError(
            "/redfish/v1/Chassis/Canister-1", 405, "Method Not Allowed", None, None
        )
        with pytest.raises(HTTPError):
            leds.chassis_led_off()


class TestFirmwareFromSingletonUpdateService:
    """UpdateService is a singleton: its FirmwareInventory members are not
    reported per system and firmware stays flat, {fw_id: {...}}."""

    INVENTORY = {
        "Enclosure1": {
            "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/Enclosure1",
            "Id": "Enclosure1",
            "Name": "Enclosure1",
            "Description": "Firmware of Enclosure1",
            "Version": "4.1.0",
            "Updateable": False,
            "Status": {"State": "Enabled", "Health": "OK"},
        },
        "Enclosure0-Drive1": {
            "@odata.id": "/redfish/v1/UpdateService/FirmwareInventory/Enclosure0-Drive1",
            "Id": "Enclosure0-Drive1",
            "Name": "Enclosure0:Drive1",
            "Version": "4.1.2",
            "Status": {"State": "Enabled", "Health": "OK"},
        },
    }

    def test_update_firmware(self, system, mock_endpoints):
        inventory = MagicMock()
        inventory.get_members_data.return_value = self.INVENTORY
        update_service = MagicMock()
        update_service.get_members_names.return_value = []
        update_service.__getitem__.side_effect = lambda key: {
            "FirmwareInventory": inventory
        }[key]
        mock_endpoints.__getitem__.side_effect = lambda key: {
            "update_service": update_service
        }[key]

        system._update_firmware()

        firmware = system.get_firmware()
        assert set(firmware) == {"enclosure1", "enclosure0-drive1"}
        assert firmware["enclosure1"]["version"] == "4.1.0"
        assert firmware["enclosure1"]["name"] == "Enclosure1"
        assert firmware["enclosure1"]["status"] == {"state": "Enabled", "health": "OK"}
        assert firmware["enclosure0-drive1"]["version"] == "4.1.2"
        assert firmware["enclosure0-drive1"]["description"] == "Enclosure0-Drive1"
        assert system.get_system()["firmware"] == firmware
