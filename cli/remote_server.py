from typing import Dict
from io import BytesIO
from subprocess import check_output
import requests
from google.cloud import iam_credentials_v1
import google.oauth2.id_token


class Server:
    def __init__(self, server_url):
        self.server_url = server_url if server_url.endswith("/") else server_url + "/"
        self.session = self.get_auth_session()

    def get_auth_session(self) -> requests.Session:
        id_token = self.get_auth_token()
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {id_token}"})
        return session

    def get_auth_token(self):
        try:
            # Assumes a GCP service account is available, e.g. in a CI/CD pipeline
            client = iam_credentials_v1.IAMCredentialsClient()
            response = client.generate_id_token(
                name=self.get_service_account_email(),
                audience=self.server_url,
            )
            id_token = response.token
        except (google.api_core.exceptions.PermissionDenied, AttributeError):
            # No GCP service account available, assumes a local env where gcloud is installed
            id_token_raw = check_output("gcloud auth print-identity-token", shell=True)
            id_token = id_token_raw.decode("utf8").strip()

        return id_token


class DbtServer(Server):
    def __init__(self, server_url):
        super().__init__(server_url)

    def send_task(self, dbt_remote_artifacts: BytesIO, dbt_runtime_config: Dict[str, str], server_runtime_config: Dict[str, str]) -> requests.Response:
        response = self.session.post(
            url=self.server_url + "run",
            files={"dbt_remote_artifacts": dbt_remote_artifacts},
            data={
                "dbt_runtime_config": dbt_runtime_config,
                "server_runtime_config": server_runtime_config
            }
        )
        return response

    def check_version_match(self):
        raw_response = self.session.get(url=self.server_url + "version")
        response = raw_response.json()
        print(f"Server version: {response}")
        # server_version = response["version"]

    def is_dbt_server(self):
        try:
            response = self.session.get(url=self.server_url + "api/check")
            if "dbt-server" in response.json()["response"]:
                return True
            return False
        except Exception:  # request timeout or max retries
            return False
