"""Details of a host network interface from standard Redfish resources.

An EthernetInterface (ComputerSystem.EthernetInterfaces) says what the
interface is: its link, MTU, MAC address and addresses, and how it stacks
(a team's members in Links.RelatedInterfaces, a VLAN's parent). Its
physical port (Links.Ports, a Port of a NetworkAdapter) adds the port's
link, the LLDP neighbor and the transceiver (SFP); the port's
PortMetrics its counters and optics by lane, and its EnvironmentMetrics
the transceiver's temperature. Status.Conditions say what is wrong.

All of these are optional: a Redfish service that has none of them gives
the interface's own properties only.
"""

import ipaddress
import re
from typing import Any, Callable, Dict, List, Optional

Fetch = Callable[[str], Dict[str, Any]]


def _dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _link_names(links: Any) -> List[str]:
    if not isinstance(links, list):
        return []
    return [
        link["@odata.id"].rstrip("/").split("/")[-1]
        for link in links
        if isinstance(link, dict) and isinstance(link.get("@odata.id"), str)
    ]


def _prefix_length(mask: Any) -> Optional[int]:
    try:
        return sum(bin(int(part)).count("1") for part in str(mask).split("."))
    except ValueError:
        return None


def conditions(resource: Dict[str, Any]) -> List[Dict[str, Any]]:
    status = resource.get("Status")
    items = status.get("Conditions") if isinstance(status, dict) else None
    result = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        result.append(
            {
                "message_id": item.get("MessageId"),
                "message": item.get("Message"),
                "severity": item.get("Severity"),
                "timestamp": item.get("Timestamp"),
            }
        )
    return result


def lldp_port_id(value: Any, subtype: Any) -> Any:
    """LLDPReceive.PortId is the octets of the TLV in hex ("65:74:2D:30"):
    the text they encode for a port name, alias or local id ("et-0"), the
    address for a network address, else as received."""
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9A-Fa-f]{2}(:[0-9A-Fa-f]{2})*", value
    ):
        return value
    octets = bytes.fromhex(value.replace(":", ""))
    if subtype in ("IfName", "IfAlias", "LocalAssign", "AgentId"):
        try:
            return octets.decode("utf-8")
        except UnicodeDecodeError:
            return value
    if subtype == "NetworkAddr" and len(octets) in (5, 17) and octets[0] in (1, 2):
        return str(ipaddress.ip_address(octets[1:]))
    return value


def lldp_details(port: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ethernet = port.get("Ethernet")
    received = ethernet.get("LLDPReceive") if isinstance(ethernet, dict) else None
    if not isinstance(received, dict) or not received:
        return None
    return {
        "system_name": received.get("SystemName"),
        "chassis_id": received.get("ChassisId"),
        "port_id": lldp_port_id(received.get("PortId"), received.get("PortIdSubtype")),
        "management_address": received.get("ManagementAddressIPv4")
        or received.get("ManagementAddressIPv6"),
        "system_description": received.get("SystemDescription"),
    }


def sfp_details(port: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    sfp = port.get("SFP")
    if not isinstance(sfp, dict) or not sfp:
        return None
    status = _dict(sfp.get("Status"))
    return {
        "type": sfp.get("Type"),
        "manufacturer": sfp.get("Manufacturer"),
        "part_number": sfp.get("PartNumber"),
        "serial_number": sfp.get("SerialNumber"),
        "medium_type": sfp.get("MediumType"),
        "fiber_connection_type": sfp.get("FiberConnectionType"),
        "health": status.get("Health"),
    }


def _lanes(transceiver: Dict[str, Any], key: str) -> List[Any]:
    lanes = transceiver.get("ByLane")
    if isinstance(lanes, list) and lanes:
        return [lane.get(key) for lane in lanes if isinstance(lane, dict)]
    return [transceiver[key]] if transceiver.get(key) is not None else []


def metrics_details(metrics: Dict[str, Any]) -> Dict[str, Any]:
    networking = _dict(metrics.get("Networking"))
    details: Dict[str, Any] = {
        "rx_bytes": metrics.get("RXBytes"),
        "tx_bytes": metrics.get("TXBytes"),
        "rx_errors": metrics.get("RXErrors"),
        "tx_errors": metrics.get("TXErrors"),
        "rx_fcs_errors": networking.get("RXFCSErrors"),
        "rx_discards": networking.get("RXDiscards"),
        "tx_discards": networking.get("TXDiscards"),
    }
    transceivers = metrics.get("Transceivers")
    if (
        isinstance(transceivers, list)
        and transceivers
        and isinstance(transceivers[0], dict)
    ):
        transceiver = transceivers[0]
        details["transceiver"] = {
            "supply_voltage": transceiver.get("SupplyVoltage"),
            "wavelength_nm": transceiver.get("WavelengthNanometers"),
            "rx_power_mw": _lanes(transceiver, "RXInputPowerMilliWatts"),
            "tx_power_mw": _lanes(transceiver, "TXOutputPowerMilliWatts"),
            "tx_bias_ma": _lanes(transceiver, "TXBiasCurrentMilliAmps"),
        }
    return details


def port_details(port: Dict[str, Any], fetch: Fetch) -> Dict[str, Any]:
    location = port.get("Location")
    part = location.get("PartLocation") if isinstance(location, dict) else None
    details: Dict[str, Any] = {
        "port": port.get("@odata.id"),
        "port_link_status": port.get("LinkStatus"),
        "current_speed_gbps": port.get("CurrentSpeedGbps"),
        "max_speed_gbps": port.get("MaxSpeedGbps"),
        "link_transitions": port.get("LinkTransitionIndicator"),
        "port_medium": port.get("PortMedium"),
        "location": part.get("ServiceLabel") if isinstance(part, dict) else None,
    }
    # the neighbor and the transceiver only when there is one
    for key, value in (("lldp", lldp_details(port)), ("sfp", sfp_details(port))):
        if value is not None:
            details[key] = value
    metrics = port.get("Metrics")
    if isinstance(metrics, dict) and metrics.get("@odata.id"):
        details.update(metrics_details(fetch(metrics["@odata.id"])))
    environment = port.get("EnvironmentMetrics")
    if isinstance(environment, dict) and environment.get("@odata.id"):
        temperature = fetch(environment["@odata.id"]).get("TemperatureCelsius")
        if isinstance(temperature, dict):
            details["transceiver_temperature_c"] = temperature.get("Reading")
    return details


def interface_details(interface: Dict[str, Any], fetch: Fetch) -> Dict[str, Any]:
    """The details of an EthernetInterface, following its links with fetch
    (which returns {} for a resource it cannot get)."""
    links = _dict(interface.get("Links"))
    vlan = _dict(interface.get("VLAN"))
    addresses = []
    for address in interface.get("IPv4Addresses") or []:
        if isinstance(address, dict) and address.get("Address"):
            length = _prefix_length(address.get("SubnetMask"))
            addresses.append(
                address["Address"] + (f"/{length}" if length is not None else "")
            )
    for address in interface.get("IPv6Addresses") or []:
        if (
            isinstance(address, dict)
            and address.get("Address")
            and address.get("AddressOrigin") != "LinkLocal"
        ):
            length = address.get("PrefixLength")
            addresses.append(
                address["Address"] + (f"/{length}" if length is not None else "")
            )
    details: Dict[str, Any] = {
        "link_status": interface.get("LinkStatus"),
        "interface_type": interface.get("EthernetInterfaceType"),
        "team_mode": interface.get("TeamMode"),
        "mac_address": interface.get("MACAddress"),
        "mtu_size": interface.get("MTUSize"),
        "interface_enabled": interface.get("InterfaceEnabled"),
        "vlan_id": vlan.get("VLANId"),
        "addresses": addresses,
        "related_interfaces": _link_names(links.get("RelatedInterfaces")),
        "affiliated_interfaces": _link_names(links.get("AffiliatedInterfaces")),
        "conditions": conditions(interface),
    }
    ports = links.get("Ports")
    if isinstance(ports, list) and ports and isinstance(ports[0], dict):
        url = ports[0].get("@odata.id")
        port = fetch(url) if isinstance(url, str) else {}
        if port:
            details.update(port_details(port, fetch))
            for condition in conditions(port):
                if condition not in details["conditions"]:
                    details["conditions"].append(condition)
    return details
