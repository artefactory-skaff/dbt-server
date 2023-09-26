import sys
from google.cloud import run_v2
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
    project_id, location = "stc-dbt-test-9e19", "europe-west9"
    cloud_run_client = run_v2.ServicesClient()
    services = get_cloud_run_service_list(project_id, location, cloud_run_client)

    test_dbt_server_is_present = False
    for service in services:
        if service.name == "projects/stc-dbt-test-9e19/locations/europe-west9/services/server-dev-tf":
            test_dbt_server_is_present = True
    assert test_dbt_server_is_present
