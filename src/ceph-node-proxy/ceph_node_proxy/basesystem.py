import copy
import socket
import threading
import time
from threading import Lock
from typing import Any, Dict, Optional, Tuple, Union

from ceph_node_proxy.baseclient import BaseClient
from ceph_node_proxy.util import DEFAULTS, BaseThread, Config, get_logger


class BaseSystem(BaseThread):
    def __init__(self, **kw: Any) -> None:
        super().__init__()
        self.lock: Lock = Lock()
        self._system: Dict = {}
        self.config: Optional[Union[Config, Dict[str, Any]]] = kw.get("config")
        self.client: BaseClient
        self.log = get_logger(__name__)
        self._snapshot: Tuple[Dict[str, Any], Optional[float]] = ({}, None)
        self.data_ready: bool = False
        config = self.config or {}
        self.refresh_interval: int = config.get("system", {}).get(
            "refresh_interval", DEFAULTS["system"]["refresh_interval"]
        )
        # set to poll at once (the Redfish service sent an event) or to stop
        self.wake = threading.Event()

    def keep_snapshot(self) -> None:
        """Keep a copy of the data an update just read, with its time. The
        caller holds the lock. Readers that must not wait for the lock while
        the next update runs (/metrics) read the copy."""
        self._snapshot = (copy.deepcopy(self.get_system()), time.time())

    def drop_snapshot(self) -> None:
        self._snapshot = ({}, None)

    def snapshot(self) -> Tuple[Dict[str, Any], Optional[float]]:
        """The data of the last complete update and when it was read
        (None before the first)."""
        return self._snapshot

    def start_session(self) -> None:
        """Log in to the Redfish service before the first update."""
        raise NotImplementedError()

    def run_update_cycle(self) -> None:
        """Read the hardware once."""
        raise NotImplementedError()

    def main(self) -> None:
        self.stop = False
        self.start_session()
        while not self.stop:
            self.log.debug("waiting for a lock in the update loop.")
            with self.lock:
                if not self.pending_shutdown:
                    self.log.debug("lock acquired in the update loop.")
                    try:
                        self.run_update_cycle()
                        self.data_ready = True
                        self.keep_snapshot()
                    except RuntimeError as e:
                        self.stop = True
                        self.log.error(
                            f"Error detected, trying to gracefully log out from redfish api.\n{e}"
                        )
                        self.client.logout()
                        raise
            self.log.debug("lock released in the update loop.")
            # wait without the lock: the API and the reporter read meanwhile;
            # an event of the Redfish service (event_stream) or a shutdown
            # ends the wait
            if not self.stop and self.wake.wait(self.refresh_interval):
                self.wake.clear()
                self.log.debug("woken before the refresh interval.")
        self.log.debug("exiting update loop.")
        raise SystemExit(0)

    def shutdown(self) -> None:
        super().shutdown()
        self.wake.set()

    def get_system(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def get_status(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_metadata(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_processors(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_memory(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_fans(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_temperatures(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_power(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_network(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_storage(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_fcm(self) -> Dict[str, Dict[str, Dict]]:
        return {}

    def get_firmware(self) -> Dict[str, Dict[str, Dict]]:
        raise NotImplementedError()

    def get_sn(self) -> str:
        raise NotImplementedError()

    def get_led(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_led(self, data: Dict[str, str]) -> int:
        raise NotImplementedError()

    def get_chassis_led(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_chassis_led(self, data: Dict[str, str]) -> int:
        raise NotImplementedError()

    def device_led_on(self, device: str) -> int:
        raise NotImplementedError()

    def device_led_off(self, device: str) -> int:
        raise NotImplementedError()

    def get_device_led(self, device: str) -> Dict[str, Any]:
        raise NotImplementedError()

    def set_device_led(self, device: str, data: Dict[str, bool]) -> int:
        raise NotImplementedError()

    def chassis_led_on(self) -> int:
        raise NotImplementedError()

    def chassis_led_off(self) -> int:
        raise NotImplementedError()

    def get_host(self) -> str:
        return socket.gethostname()

    def stop_update_loop(self) -> None:
        raise NotImplementedError()

    def flush(self) -> None:
        raise NotImplementedError()

    def shutdown_host(self, force: bool = False) -> int:
        raise NotImplementedError()

    def powercycle(self) -> int:
        raise NotImplementedError()
