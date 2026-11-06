import concurrent.futures
import dataclasses
import json
from time import sleep
from typing import Any, Callable, Dict, List, Optional

from ceph_node_proxy.basesystem import BaseSystem
from ceph_node_proxy.network import conditions, interface_details
from ceph_node_proxy.redfish import (
    ComponentUpdateSpec,
    Endpoint,
    EndpointMgr,
    get_component_data,
    update_component,
)
from ceph_node_proxy.redfish_client import RedFishClient
from ceph_node_proxy.util import (
    DEFAULTS,
    fill_missing_identity,
    get_logger,
    is_unknown,
    normalize_dict,
    to_snake_case,
)


def fill_drive_slot_and_firmware(drive: Dict[str, Any]) -> None:
    """Set 'slot' and 'firmware_version' from the standard Drive properties
    (PhysicalLocation, Revision) when they are not known yet."""
    if is_unknown(drive.get("firmware_version")) and not is_unknown(
        drive.get("revision")
    ):
        drive["firmware_version"] = drive["revision"]
    location = drive.get("physical_location")
    part = location.get("partlocation") if isinstance(location, dict) else None
    if (
        is_unknown(drive.get("slot"))
        and isinstance(part, dict)
        and not is_unknown(part.get("locationordinalvalue"))
    ):
        drive["slot"] = part["locationordinalvalue"]


class BaseRedfishSystem(BaseSystem):
    NETWORK_FIELDS: List[str] = ["Description", "Name", "Id", "SpeedMbps", "Status"]
    PROCESSORS_FIELDS: List[str] = [
        "Description",
        "Id",
        "TotalCores",
        "TotalThreads",
        "ProcessorType",
        "Model",
        "Status",
        "Manufacturer",
    ]
    MEMORY_FIELDS: List[str] = [
        "Description",
        "Id",
        "MemoryDeviceType",
        "CapacityMiB",
        "Status",
    ]
    POWER_FIELDS: List[str] = ["Name", "Id", "Model", "Manufacturer", "Status"]
    FANS_FIELDS: List[str] = [
        "Name",
        "Id",
        "PhysicalContext",
        "Reading",
        "ReadingUnits",
        "Status",
    ]
    TEMPERATURES_FIELDS: List[str] = [
        "Name",
        "Id",
        "PhysicalContext",
        "Reading",
        "ReadingUnits",
        "Status",
    ]
    FIRMWARE_FIELDS: List[str] = [
        "Name",
        "Description",
        "Id",
        "ReleaseDate",
        "Version",
        "Updateable",
        "Status",
    ]

    COMPONENT_SPECS: Dict[str, List[ComponentUpdateSpec]] = {
        "network": [
            ComponentUpdateSpec(
                "systems",
                "EthernetInterfaces",
                NETWORK_FIELDS,
                None,
                enrich="network_details",
            ),
            ComponentUpdateSpec("systems", "NetworkInterfaces", NETWORK_FIELDS, None),
        ],
        "processors": [
            ComponentUpdateSpec("systems", "Processors", PROCESSORS_FIELDS, None),
        ],
        "memory": [
            ComponentUpdateSpec("systems", "Memory", MEMORY_FIELDS, None),
        ],
        # Power supplies: Chassis/.../PowerSubsystem/PowerSupplies (not like other components: like Systems/.../Memory)
        "power": [
            ComponentUpdateSpec(
                "chassis", "PowerSubsystem/PowerSupplies", POWER_FIELDS, None
            ),
        ],
        "fans": [
            ComponentUpdateSpec("chassis", "Thermal", FANS_FIELDS, "Fans"),
        ],
        "temperatures": [
            ComponentUpdateSpec(
                "chassis", "Thermal", TEMPERATURES_FIELDS, "Temperatures"
            ),
        ],
        "firmware": [
            ComponentUpdateSpec(
                "update_service", "FirmwareInventory", FIRMWARE_FIELDS, None
            ),
        ],
    }

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = get_logger(__name__)
        self.host: str = kw["host"]
        self.port: str = kw["port"]
        self.username: str = kw["username"]
        self.password: str = kw["password"]
        # move the following line (class attribute?)
        self.client: RedFishClient = RedFishClient(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
        )
        self.endpoints: EndpointMgr = EndpointMgr(self.client)
        self.log.info(
            f"redfish system initialization, host: {self.host}, user: {self.username}"
        )
        self.data_ready: bool = False
        self.previous_data: Dict = {}
        self.data: Dict[str, Dict[str, Any]] = {}
        self._system: Dict[str, Dict[str, Any]] = {}
        self._sys: Dict[str, Any] = {}
        self.job_service_endpoint: str = ""
        self.create_reboot_job_endpoint: str = ""
        self.setup_job_queue_endpoint: str = ""
        self.component_list: List[str] = kw.get(
            "component_list",
            [
                "memory",
                "power",
                "fans",
                "temperatures",
                "network",
                "processors",
                "storage",
                "firmware",
            ],
        )
        self.update_funcs: List[Callable] = []
        for component in self.component_list:
            self.log.debug(f"adding: {component} to hw component gathered list.")
            func = f"_update_{component}"
            if hasattr(self, func):
                f = getattr(self, func)
                self.update_funcs.append(f)
        config = kw.get("config") or {}
        self.refresh_interval: int = config.get("system", {}).get(
            "refresh_interval", DEFAULTS["system"]["refresh_interval"]
        )

    def initialize_redfish_session(self) -> None:
        self.client.login()
        self.endpoints.init()

    def run_update_cycle(self) -> None:
        self._update_system()
        self._update_sn()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(lambda f: f(), self.update_funcs)

    def update(
        self,
        collection: str,
        component: str,
        path: str,
        fields: List[str],
        attribute: Optional[str] = None,
    ) -> None:
        update_component(
            self.endpoints,
            collection,
            component,
            path,
            fields,
            self._sys,
            self.log,
            attribute=attribute,
        )

    def main(self) -> None:
        self.stop = False
        self.initialize_redfish_session()

        while not self.stop:
            self.log.debug("waiting for a lock in the update loop.")
            with self.lock:
                if not self.pending_shutdown:
                    self.log.debug("lock acquired in the update loop.")
                    try:
                        self.run_update_cycle()
                        self.data_ready = True
                    except RuntimeError as e:
                        self.stop = True
                        self.log.error(
                            f"Error detected, trying to gracefully log out from redfish api.\n{e}"
                        )
                        self.client.logout()
                        raise
                    sleep(self.refresh_interval)
            self.log.debug("lock released in the update loop.")
        self.log.debug("exiting update loop.")
        raise SystemExit(0)

    def flush(self) -> None:
        self.log.debug("Acquiring lock to flush data.")
        self.lock.acquire()
        self.log.debug("Lock acquired, flushing data.")
        self._system = {}
        self.previous_data = {}
        self.log.info("Data flushed.")
        self.data_ready = False
        self.log.debug("Data marked as not ready.")
        self.lock.release()
        self.log.debug("Released the lock after flushing data.")

    # @retry(retries=10, delay=2)
    def _get_path(self, path: str) -> Dict:
        result: Dict[str, Any] = {}
        try:
            if not self.pending_shutdown:
                self.log.debug(f"Getting path: {path}")
                result = self.client.get_path(path)
            else:
                self.log.debug(f"Pending shutdown, aborting query to {path}")
        except RuntimeError:
            raise

        return result

    def get_members(self, data: Dict[str, Any], path: str) -> List:
        return [self._get_path(member["@odata.id"]) for member in data["Members"]]

    def get_system(self) -> Dict[str, Any]:
        result = {
            "host": self.get_host(),
            "sn": self.get_sn(),
            "status": {
                "storage": self.get_storage(),
                "processors": self.get_processors(),
                "network": self.get_network(),
                "memory": self.get_memory(),
                "power": self.get_power(),
                "fans": self.get_fans(),
                "temperatures": self.get_temperatures(),
            },
            "firmware": self.get_firmware(),
        }
        return result

    def _update_system(self) -> None:
        system_members: Dict[str, Any] = self.endpoints["systems"].get_members_data()
        update_service_members: Endpoint = self.endpoints["update_service"]

        for member, data in system_members.items():
            self._system[member] = data
            self._sys[member] = dict()

        self._system[update_service_members.id] = update_service_members.data

    def get_sn(self) -> str:
        return str(self._sys.get("SN", ""))

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("status", {}))

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("memory", {}))

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("processors", {}))

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("network", {}))

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("storage", {}))

    def get_firmware(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("firmware", {}))

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("power", {}))

    def get_fans(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("fans", {}))

    def get_temperatures(self) -> Dict[str, Dict[str, Dict]]:
        return dict(self._sys.get("temperatures", {}))

    def get_component_spec_overrides(self) -> Dict[str, Dict[str, Any]]:
        return {}

    def get_specs(self, component: str) -> List[ComponentUpdateSpec]:
        return [
            self._apply_spec_overrides(component, spec)
            for spec in self.COMPONENT_SPECS[component]
        ]

    def _apply_spec_overrides(
        self, component: str, spec: ComponentUpdateSpec
    ) -> ComponentUpdateSpec:
        overrides = self.get_component_spec_overrides().get(component)
        if not overrides:
            return spec
        return dataclasses.replace(spec, **overrides)

    @staticmethod
    def _store_members(
        target: Dict[str, Any], members: Dict[str, Any], prefix: Optional[str]
    ) -> None:
        for member_id, data in members.items():
            key = member_id if prefix is None else f"{prefix}_{member_id}"
            fill_missing_identity(data, key)
            target[key] = data

    def _run_update(self, component: str) -> None:
        self.log.debug(f"Updating {component}")
        specs = self.get_specs(component)
        self._sys[component] = {}
        use_single_key = len(specs) == 1
        for spec in specs:
            try:
                result = get_component_data(
                    self.endpoints,
                    spec.collection,
                    spec.path,
                    spec.fields,
                    self.log,
                    attribute=spec.attribute,
                    enrich=getattr(self, spec.enrich) if spec.enrich else None,
                )
                path_prefix = spec.path.split("/")[-1].lower() or component
                prefix = None if use_single_key else path_prefix
                if self.endpoints[spec.collection].get_members_names():
                    # {sys_id: {member_id: data}}
                    for sys_id, members in result.items():
                        self._store_members(
                            self._sys[component].setdefault(sys_id, {}),
                            members,
                            prefix,
                        )
                else:
                    # singleton collection (UpdateService): get_component_data()
                    # returns the members themselves, {member_id: data}
                    self._store_members(self._sys[component], result, prefix)
            except Exception as e:
                self.log.debug(
                    "Skipping %s path %s (not available on this hardware): %s",
                    component,
                    spec.path,
                    e,
                )

    def _update_network(self) -> None:
        self._run_update("network")

    def _fetch(self, path: str) -> Dict[str, Any]:
        """A linked Redfish resource, {} when it cannot be read."""
        try:
            return self._get_path(path)
        except (RuntimeError, ValueError) as e:
            self.log.debug(f"Can't read {path}: {e}")
            return {}

    def network_details(self, interface: Dict[str, Any]) -> Dict[str, Any]:
        """Link, neighbor, transceiver, counters and conditions of a network
        interface, from its port and the port's metrics (ceph_node_proxy.network)."""
        return interface_details(interface, self._fetch)

    def _update_processors(self) -> None:
        self._run_update("processors")

    def _update_storage(self) -> None:
        fields = [
            "Description",
            "Id",
            "CapacityBytes",
            "Model",
            "Protocol",
            "LocationIndicatorActive",
            "SerialNumber",
            "Status",
            "PhysicalLocation",
            "Revision",
            # self-encrypting drives (Drive v1.22)
            "EncryptionAbility",
            "EncryptionStatus",
            "SecurityIDOwned",
        ]
        result: Dict[str, Dict[str, Dict]] = dict()
        self.log.debug("Updating storage")
        members_names = self.endpoints["systems"].get_members_names()
        for member in members_names:
            result[member] = {}
            members_data = self.endpoints["systems"][member][
                "Storage"
            ].get_members_data()
            for entity in members_data:
                for drive in members_data[entity]["Drives"]:
                    data: Dict[str, Any] = Endpoint(
                        drive["@odata.id"], self.endpoints.client
                    ).data
                    drive_id = data["Id"]
                    result[member][drive_id] = dict()
                    result[member][drive_id]["redfish_endpoint"] = data["@odata.id"]
                    for field in fields:
                        result[member][drive_id][to_snake_case(field)] = data.get(field)
                        result[member][drive_id]["entity"] = entity
                    # what is wrong, e.g. why a drive stays locked: the
                    # health warnings and the dashboard show the messages
                    result[member][drive_id]["conditions"] = conditions(data)
            # do not normalize the first level of the dictionary
            result[member] = normalize_dict(result[member])
            for drive_id, drive_data in result[member].items():
                fill_missing_identity(drive_data, drive_id)
                fill_drive_slot_and_firmware(drive_data)
            self._sys["storage"] = result

    def _update_sn(self) -> None:
        serials: List[str] = []
        self.log.debug("Updating serial number")
        data: Dict[str, Any] = self.endpoints["systems"].get_members_data()
        for chassis_id in data:
            serial = data[chassis_id].get("SerialNumber")
            if serial:
                serials.append(serial)
        self._sys["SN"] = ",".join(serials)

    def _update_memory(self) -> None:
        self._run_update("memory")

    def _update_power(self) -> None:
        self._run_update("power")

    def _update_fans(self) -> None:
        self._run_update("fans")

    def _update_temperatures(self) -> None:
        self._run_update("temperatures")

    def _update_firmware(self) -> None:
        self._run_update("firmware")

    # Identify LEDs, through the standard LocationIndicatorActive property of
    # the Drive and Chassis resources. Errors from the Redfish service
    # (HTTPError) are raised to the caller.

    def get_drive_endpoint(self, device: str) -> str:
        """Redfish endpoint of a drive, from its key in the storage data."""
        for drives in self._sys.get("storage", {}).values():
            if device in drives:
                return drives[device]["redfish_endpoint"]
        raise KeyError(f"drive '{device}' not found")

    def get_chassis_endpoint(self) -> str:
        """Chassis containing the system: the first one the system links to,
        else the first member of the Chassis collection."""
        for member in self.endpoints["systems"].get_members_names():
            links = self._system.get(member, {}).get("Links") or {}
            for chassis in links.get("Chassis") or []:
                if isinstance(chassis, dict) and chassis.get("@odata.id"):
                    return chassis["@odata.id"]
        members = self.endpoints["chassis"].get_members_endpoints()
        if not members:
            raise KeyError("no chassis found")
        return list(members.values())[0]

    def get_led_state(self, endpoint: str) -> Dict[str, Any]:
        _, data, status = self.client.query(method="GET", endpoint=endpoint)
        resource = json.loads(data) if data else {}
        return {
            "http_code": status,
            "LocationIndicatorActive": resource.get("LocationIndicatorActive"),
        }

    def set_led_state(self, endpoint: str, data: Dict[str, Any]) -> int:
        _, _, status = self.client.query(
            data=json.dumps(data), method="PATCH", endpoint=endpoint
        )
        return status

    def device_led_on(self, device: str) -> int:
        return self.set_device_led(device, {"LocationIndicatorActive": True})

    def device_led_off(self, device: str) -> int:
        return self.set_device_led(device, {"LocationIndicatorActive": False})

    def chassis_led_on(self) -> int:
        return self.set_chassis_led({"LocationIndicatorActive": True})

    def chassis_led_off(self) -> int:
        return self.set_chassis_led({"LocationIndicatorActive": False})

    def get_device_led(self, device: str) -> Dict[str, Any]:
        return self.get_led_state(self.get_drive_endpoint(device))

    def set_device_led(self, device: str, data: Dict[str, Any]) -> int:
        return self.set_led_state(self.get_drive_endpoint(device), data)

    def get_chassis_led(self) -> Dict[str, Any]:
        return self.get_led_state(self.get_chassis_endpoint())

    def set_chassis_led(self, data: Dict[str, Any]) -> int:
        return self.set_led_state(self.get_chassis_endpoint(), data)

    # Diagnostic data (drive dumps), through the standard
    # LogService.CollectDiagnosticData action with DiagnosticDataType
    # "Device" (LogService v1.8) of the chassis holding the drive. A
    # collection runs as a Redfish task; each result is a LogEntry whose
    # AdditionalDataURI downloads the data.

    def _try_get_path(self, path: str) -> Dict[str, Any]:
        try:
            return self._get_path(path) if path else {}
        except RuntimeError:
            return {}

    def _drive_chassis(self, drive_endpoint: str) -> str:
        drive = self._try_get_path(drive_endpoint)
        chassis = (drive.get("Links") or {}).get("Chassis")
        if isinstance(chassis, list) and chassis:
            chassis = chassis[0]
        if isinstance(chassis, dict) and chassis.get("@odata.id"):
            return str(chassis["@odata.id"])
        if "/Drives/" in drive_endpoint:  # .../Chassis/<id>/Drives/<drive>
            return drive_endpoint.split("/Drives/")[0]
        raise KeyError(f"no chassis found for {drive_endpoint}")

    def _diagnostic_services(self, chassis_endpoint: str) -> List[Dict[str, Any]]:
        """Log services of a chassis that collect the diagnostic data of
        its devices."""
        chassis = self._try_get_path(chassis_endpoint)
        link = (chassis.get("LogServices") or {}).get("@odata.id")
        services: List[Dict[str, Any]] = []
        for member in self._try_get_path(link).get("Members", []) if link else []:
            service = self._try_get_path(member.get("@odata.id", ""))
            action = (service.get("Actions") or {}).get(
                "#LogService.CollectDiagnosticData"
            )
            if not action:
                continue
            allowed = action.get("DiagnosticDataType@Redfish.AllowableValues")
            if allowed is not None and "Device" not in allowed:
                continue
            services.append(service)
        return services

    def _drives_by_endpoint(self) -> Dict[str, str]:
        return {
            str(drive.get("redfish_endpoint", "")).rstrip("/"): drive_id
            for drives in self._sys.get("storage", {}).values()
            for drive_id, drive in drives.items()
        }

    def get_diagnostics(self) -> List[Dict[str, Any]]:
        """Diagnostic data kept by the Redfish service, oldest first."""
        drives = self._drives_by_endpoint()
        result: List[Dict[str, Any]] = []
        chassis = self.endpoints["chassis"].get_members_endpoints().values()
        for chassis_endpoint in chassis:
            for service in self._diagnostic_services(chassis_endpoint):
                entries = (service.get("Entries") or {}).get("@odata.id", "")
                for entry in self._try_get_path(entries).get("Members", []):
                    if "Id" not in entry:  # not expanded
                        entry = self._try_get_path(entry.get("@odata.id", ""))
                    if not entry.get("AdditionalDataURI"):
                        continue
                    origin = (
                        (entry.get("Links") or {}).get("OriginOfCondition") or {}
                    ).get("@odata.id")
                    result.append(
                        {
                            "id": entry.get("Id"),
                            "name": entry.get("Name"),
                            "created": entry.get("Created"),
                            "message": entry.get("Message"),
                            "size": entry.get("AdditionalDataSizeBytes"),
                            "diagnostic_data_type": entry.get("DiagnosticDataType"),
                            "drive": drives.get(str(origin or "").rstrip("/")),
                            "drive_endpoint": origin,
                            "redfish_endpoint": entry.get("@odata.id"),
                            "download_endpoint": entry.get("AdditionalDataURI"),
                            "service": service.get("@odata.id"),
                        }
                    )
        return sorted(result, key=lambda e: (str(e["created"]), str(e["id"])))

    def _diagnostics_entry(self, entry_id: str) -> Dict[str, Any]:
        for entry in self.get_diagnostics():
            if entry["id"] == entry_id:
                return entry
        raise KeyError(f"diagnostic data '{entry_id}' not found")

    @staticmethod
    def _task_summary(task: Dict[str, Any]) -> Dict[str, Any]:
        headers = (task.get("Payload") or {}).get("HttpHeaders") or []
        entries = [
            h.split(":", 1)[1].strip()
            for h in headers
            if isinstance(h, str) and h.lower().startswith("location:")
        ]
        return {
            "id": task.get("Id"),
            "name": task.get("Name"),
            "state": task.get("TaskState"),
            "status": task.get("TaskStatus"),
            "percent_complete": task.get("PercentComplete"),
            "start_time": task.get("StartTime"),
            "end_time": task.get("EndTime"),
            "messages": [m.get("Message") for m in task.get("Messages") or []],
            "entries": [e.rstrip("/").split("/")[-1] for e in entries],
        }

    def collect_diagnostics(self, device: str) -> Dict[str, Any]:
        """Start collecting the diagnostic data of a drive; returns the task."""
        drive_endpoint = self.get_drive_endpoint(device)
        services = self._diagnostic_services(self._drive_chassis(drive_endpoint))
        if not services:
            raise NotImplementedError()
        action = services[0]["Actions"]["#LogService.CollectDiagnosticData"]
        body = {
            "DiagnosticDataType": "Device",
            "TargetDevice": {"@odata.id": drive_endpoint},
        }
        headers, data, _ = self.client.query(
            data=json.dumps(body), method="POST", endpoint=action["target"]
        )
        task = json.loads(data) if data else {}
        location = headers.get("Location") or task.get("@odata.id") or ""
        if not task.get("Id"):
            task["Id"] = location.rstrip("/").split("/")[-1]
        return self._task_summary(task)

    def get_diagnostics_task(self, task_id: str) -> Dict[str, Any]:
        _, data, _ = self.client.query(
            method="GET",
            endpoint=f"{RedFishClient.PREFIX}TaskService/Tasks/{task_id}",
        )
        return self._task_summary(json.loads(data) if data else {})

    def download_diagnostics(self, entry_id: str) -> Dict[str, Any]:
        """The data of an entry: {'filename': ..., 'content': bytes}"""
        entry = self._diagnostics_entry(entry_id)
        headers, data, _ = self.client.query(
            method="GET", endpoint=entry["download_endpoint"], timeout=120, raw=True
        )
        get_filename = getattr(headers, "get_filename", None)
        filename = get_filename() if callable(get_filename) else None
        return {"filename": filename or f"{entry_id}.bin", "content": data}

    def delete_diagnostics(self, entry_id: str) -> int:
        entry = self._diagnostics_entry(entry_id)
        _, _, status = self.client.query(
            method="DELETE", endpoint=entry["redfish_endpoint"]
        )
        return int(status)

    def shutdown_host(self, force: bool = False) -> int:
        raise NotImplementedError()

    def powercycle(self) -> int:
        raise NotImplementedError()

    def create_reboot_job(self, reboot_type: str) -> str:
        raise NotImplementedError()

    def schedule_reboot_job(self, job_id: str) -> int:
        raise NotImplementedError()
