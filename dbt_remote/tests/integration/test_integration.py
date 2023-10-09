import sys
from google.cloud import run_v2
sys.path.insert(1, './dbt_remote/')
from src.dbt_remote.dbt_server_detector import get_cloud_run_service_list


class TestService:
    def __init__(self, new_uri):
        self._uri = new_uri

    @property
    def uri(self) -> str:
        return self._uri


def test_get_cloud_run_service_list():
    project_id, location = "stc-dbt-test-9e19", "europe-west9"
    services = get_cloud_run_service_list(project_id, location)

    test_dbt_server_is_present = False
    for service in services:
        if service.name == "projects/stc-dbt-test-9e19/locations/europe-west9/services/dbt-server-test":
            test_dbt_server_is_present = True
    assert test_dbt_server_is_present
