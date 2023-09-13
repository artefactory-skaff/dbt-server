import sys
sys.path.insert(1, './package/')
from src.dbt_remote.dbt_server_detector import check_if_server_is_dbt_server, get_cloud_run_service_list


class TestService:
    def __init__(self, new_uri):
        self._uri = new_uri

    @property
    def uri(self) -> str:
        return self._uri


def test_check_if_server_is_dbt_server():
    service = TestService("http://0.0.0.0:8001")
    assert check_if_server_is_dbt_server(service)

    service = TestService("http://0.0.0.0:8002")
    assert not check_if_server_is_dbt_server(service)


def test_get_cloud_run_service_list():
    project_id, location = "stc-dbt-test-9e19", "us-central1"
    services = get_cloud_run_service_list(project_id, location)

    test_dbt_server_is_present = False
    for service in services:
        if service.name == "projects/stc-dbt-test-9e19/locations/us-central1/services/server-dev":
            test_dbt_server_is_present = True
    assert test_dbt_server_is_present
