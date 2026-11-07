from unittest.mock import MagicMock, patch

from ceph_node_proxy.redfish_client import RedFishClient


def _logged_in_client() -> RedFishClient:
    client = RedFishClient(host="bmc", username="u", password="p")
    client.token = "tok"
    client.location = "/redfish/v1/SessionService/Sessions/1"
    return client


def test_logout_empty_body() -> None:
    client = _logged_in_client()
    with (
        patch.object(client, "is_logged_in", return_value=True),
        patch.object(client, "query", return_value=(MagicMock(), "", 204)),
    ):
        result = client.logout()
    assert result == {}
    assert client.token == ""
    assert client.location == ""


def test_logout_json_body() -> None:
    client = _logged_in_client()
    with (
        patch.object(client, "is_logged_in", return_value=True),
        patch.object(client, "query", return_value=(MagicMock(), '{"ok": true}', 200)),
    ):
        result = client.logout()
    assert result == {"ok": True}
    assert client.token == ""
    assert client.location == ""


def test_logout_invalid_json_body() -> None:
    client = _logged_in_client()
    with (
        patch.object(client, "is_logged_in", return_value=True),
        patch.object(client, "query", return_value=(MagicMock(), "not-json", 200)),
    ):
        result = client.logout()
    assert result == {}
    assert client.token == ""
    assert client.location == ""


def test_query_goes_through_the_kept_connections() -> None:
    client = _logged_in_client()
    with patch.object(
        client.connections, "request", return_value=(MagicMock(), "{}", 200)
    ) as request:
        assert client.get_path("Systems") == {}
    kwargs = request.call_args.kwargs
    assert kwargs["endpoint"] == "/redfish/v1/Systems"
    assert kwargs["headers"]["X-Auth-Token"] == "tok"


def test_query_logs_in_again_on_401() -> None:
    from urllib.error import HTTPError

    client = _logged_in_client()
    expired = HTTPError("https://bmc/redfish/v1/Systems", 401, "", MagicMock(), None)

    def login() -> None:
        client.token = "new"

    with (
        patch.object(
            client.connections,
            "request",
            side_effect=[expired, (MagicMock(), "{}", 200)],
        ) as request,
        patch.object(client, "login", side_effect=login),
    ):
        client.get_path("Systems")
    assert request.call_args.kwargs["headers"]["X-Auth-Token"] == "new"


def test_logout_closes_the_kept_connections() -> None:
    client = _logged_in_client()
    with (
        patch.object(client, "is_logged_in", return_value=True),
        patch.object(client, "query", return_value=(MagicMock(), "", 204)),
        patch.object(client.connections, "close") as close,
    ):
        client.logout()
    close.assert_called_once()
