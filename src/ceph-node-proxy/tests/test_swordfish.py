"""FCM compression from Swordfish storage pools. The resources are what HAL
serves for an FCM (its tests' drive in slot 2)."""

import copy

from ceph_node_proxy.swordfish import PoolUsage, pool_usage

STORAGE = "/redfish/v1/Systems/CAN01/Storage/Enclosure0"
POOL = STORAGE + "/StoragePools/DriveSlot-2"
VOLUME = STORAGE + "/Volumes/DriveSlot-2-1"
DRIVE = "/redfish/v1/Chassis/ComputeModule-0/Drives/DriveSlot-2"

HAL = {
    "/redfish/v1/Systems/CAN01/Storage/Enclosure0": {
        "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0",
        "StoragePools": {
            "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools"
        },
        "Volumes": {
            "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/Volumes"
        }
    },
    "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools": {
        "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools",
        "@odata.type": "#StoragePoolCollection.StoragePoolCollection",
        "Members": [
            {
                "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools/DriveSlot-2"
            }
        ]
    },
    "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools/DriveSlot-2": {
        "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools/DriveSlot-2",
        "@odata.type": "#StoragePool.v1_9_0.StoragePool",
        "AllocatedVolumes": {
            "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools/DriveSlot-2/AllocatedVolumes"
        },
        "Capacity": {
            "Data": {
                "AllocatedBytes": 33599931809792,
                "ConsumedBytes": 1000000000
            }
        },
        "Id": "DriveSlot-2"
    },
    "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools/DriveSlot-2/AllocatedVolumes": {
        "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/StoragePools/DriveSlot-2/AllocatedVolumes",
        "@odata.type": "#VolumeCollection.VolumeCollection",
        "Members": [
            {
                "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/Volumes/DriveSlot-2-1"
            }
        ]
    },
    "/redfish/v1/Systems/CAN01/Storage/Enclosure0/Volumes/DriveSlot-2-1": {
        "@odata.id": "/redfish/v1/Systems/CAN01/Storage/Enclosure0/Volumes/DriveSlot-2-1",
        "@odata.type": "#Volume.v1_9_0.Volume",
        "Capacity": {
            "Data": {
                "ConsumedBytes": 2300000000,
                "ProvisionedBytes": 33599931809792
            }
        },
        "CapacityBytes": 33599931809792,
        "Id": "DriveSlot-2-1",
        "Links": {
            "Drives": [
                {
                    "@odata.id": "/redfish/v1/Chassis/ComputeModule-0/Drives/DriveSlot-2"
                }
            ],
            "Drives@odata.count": 1
        }
    }
}


def fetch_from(resources):
    return lambda url: copy.deepcopy(resources.get(url, {}))


def test_hal_pool():
    assert pool_usage(HAL[STORAGE], fetch_from(HAL)) == [
        PoolUsage(DRIVE, 33599931809792, 1000000000, 33599931809792, 2300000000)
    ]


def test_storage_without_pools():
    assert pool_usage({"Drives": []}, fetch_from(HAL)) == []


def test_pools_left_out():
    cases = {
        "no physical use": lambda r: r[POOL]["Capacity"]["Data"].pop("ConsumedBytes"),
        "no volumes link": lambda r: r[POOL].pop("AllocatedVolumes"),
        "no volumes": lambda r: r[POOL + "/AllocatedVolumes"].update(Members=[]),
        "no logical use": lambda r: r[VOLUME]["Capacity"]["Data"].pop("ConsumedBytes"),
        "no drive": lambda r: r[VOLUME]["Links"].update(Drives=[]),
        "two drives": lambda r: r[VOLUME]["Links"]["Drives"].append(
            {"@odata.id": DRIVE.replace("DriveSlot-2", "DriveSlot-3")}),
        "not a number": lambda r: r[POOL]["Capacity"]["Data"].update(AllocatedBytes="1"),
        "unreadable pool": lambda r: r.pop(POOL),
    }
    for name, change in cases.items():
        resources = copy.deepcopy(HAL)
        change(resources)
        assert pool_usage(resources[STORAGE], fetch_from(resources)) == [], name


def test_volumes_add_up():
    resources = copy.deepcopy(HAL)
    second = dict(copy.deepcopy(resources[VOLUME]), Id="DriveSlot-2-2")
    second["@odata.id"] = STORAGE + "/Volumes/DriveSlot-2-2"
    second["Capacity"] = {"Data": {"ProvisionedBytes": 100, "ConsumedBytes": 7}}
    resources[second["@odata.id"]] = second
    resources[POOL + "/AllocatedVolumes"]["Members"].append({"@odata.id": second["@odata.id"]})
    (usage,) = pool_usage(resources[STORAGE], fetch_from(resources))
    assert (usage.log_size_bytes, usage.log_util_bytes) == (33599931809892, 2300000007)


def test_capacity_bytes_when_not_provisioned():
    resources = copy.deepcopy(HAL)
    resources[VOLUME]["Capacity"]["Data"].pop("ProvisionedBytes")
    (usage,) = pool_usage(resources[STORAGE], fetch_from(resources))
    assert usage.log_size_bytes == 33599931809792
