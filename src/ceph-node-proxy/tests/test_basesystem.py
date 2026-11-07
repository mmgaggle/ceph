from typing import Any, Dict

from ceph_node_proxy.basesystem import BaseSystem


class FakeSystem(BaseSystem):
    def __init__(self) -> None:
        super().__init__()
        self.data: Dict[str, Any] = {"status": {"fans": {"1": {"0": {"reading": 1}}}}}

    def get_system(self) -> Dict[str, Any]:
        return self.data


def test_snapshot_is_a_copy_of_the_last_update() -> None:
    system = FakeSystem()
    assert system.snapshot() == ({}, None)
    system.keep_snapshot()
    report, when = system.snapshot()
    assert report == system.data and when is not None
    # the next update changes the data in place: the snapshot stays whole
    system.data["status"]["fans"]["1"]["0"]["reading"] = 2
    assert system.snapshot()[0]["status"]["fans"]["1"]["0"]["reading"] == 1
    system.drop_snapshot()
    assert system.snapshot() == ({}, None)
