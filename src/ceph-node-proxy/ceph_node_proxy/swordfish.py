"""FCM compression from a Redfish service's Swordfish storage pools.

An FCM compresses data before it stores it. A Redfish service that models
it with SNIA Swordfish gives, for each FCM:

- a StoragePool, whose Capacity.Data AllocatedBytes and ConsumedBytes are
  the flash (physical) size and use;
- the pool's AllocatedVolumes, the namespaces, whose Capacity.Data
  ProvisionedBytes and ConsumedBytes are the logical size and use;
- the volumes' Links.Drives, the drive.

These are the numbers fcm_stats reads from log page 0xCA with an NVMe admin
pass-through, here without access to the device.
"""

from typing import Any, Callable, Dict, List, NamedTuple, Optional

Fetch = Callable[[str], Dict[str, Any]]


class PoolUsage(NamedTuple):
    drive: str  # the drive's @odata.id
    phy_size_bytes: int
    phy_util_bytes: int
    log_size_bytes: int
    log_util_bytes: int


def _link(resource: Dict[str, Any], name: str) -> str:
    value = resource.get(name)
    return str(value.get("@odata.id") or "") if isinstance(value, dict) else ""


def _members(fetch: Fetch, url: str) -> List[Dict[str, Any]]:
    if not url:
        return []
    members = fetch(url).get("Members") or []
    return [
        fetch(member["@odata.id"])
        for member in members
        if isinstance(member, dict) and member.get("@odata.id")
    ]


def _bytes(value: Any) -> Optional[int]:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _capacity(resource: Dict[str, Any], field: str) -> Optional[int]:
    data = (resource.get("Capacity") or {}).get("Data") or {}
    return _bytes(data.get(field)) if isinstance(data, dict) else None


def pool_usage(storage: Dict[str, Any], fetch: Fetch) -> List[PoolUsage]:
    """The usage of each storage pool of a Storage resource that is backed
    by one drive and whose numbers are all reported."""
    result = []
    for pool in _members(fetch, _link(storage, "StoragePools")):
        physical = _capacity(pool, "AllocatedBytes")
        physical_used = _capacity(pool, "ConsumedBytes")
        volumes = _members(fetch, _link(pool, "AllocatedVolumes"))
        drives = {
            drive["@odata.id"]
            for volume in volumes
            for drive in (volume.get("Links") or {}).get("Drives") or []
            if isinstance(drive, dict) and drive.get("@odata.id")
        }
        logical = [
            _capacity(v, "ProvisionedBytes") or _bytes(v.get("CapacityBytes"))
            for v in volumes
        ]
        logical_used = [_capacity(v, "ConsumedBytes") for v in volumes]
        if (
            physical is None
            or physical_used is None
            or not volumes
            or len(drives) != 1
            or None in logical
            or None in logical_used
        ):
            continue
        result.append(
            PoolUsage(
                drives.pop(),
                physical,
                physical_used,
                sum(v for v in logical if v is not None),
                sum(v for v in logical_used if v is not None),
            )
        )
    return result
