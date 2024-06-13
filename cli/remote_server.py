import json
from typing import Dict, Iterator, Any
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

    def send_task(
            self,
            dbt_remote_artifacts: BytesIO,
            dbt_runtime_config: Dict[str, str],
            server_runtime_config: Dict[str, str]
    ) -> Iterator[str]:
        run_id = self.create_task(
            dbt_remote_artifacts, dbt_runtime_config, server_runtime_config
        )

        # TODO: add option to not stream log (fire and forget for client)
        for log in self.stream_log(f"{self.server_url}/api/logs/{run_id}"):
            yield log.get("info", {}).get("msg", "")

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

    def create_task(
            self,
            dbt_remote_artifacts: BytesIO,
            dbt_runtime_config: Dict[str, str],
            server_runtime_config: Dict[str, str]
    ) -> str:
        res = self.session.post(
            url=self.server_url + "api/run",
            files={"dbt_remote_artifacts": dbt_remote_artifacts},
            data={
                "dbt_runtime_config": json.dumps(dbt_runtime_config),
                "server_runtime_config": json.dumps(
                    {**server_runtime_config, "cron_schedule": "@now"})
            },
        )
        if not res.ok:
            if res.status_code == 423:
                raise ServerLocked(res.json()["lock_info"])
            elif 400 <= res.status_code < 500:
                raise ValueError(f"Error {res.status_code}: {res.content}")
            else:
                raise Exception(f"Server Error {res.status_code}: {res.content}")
        else:
            return res.json()["run_id"]

    def stream_log(self, url) -> Iterator[dict[str, Any]]:
        with self.session.get(
                url=url,
                stream=True
        ) as response:
            for chunk in response.iter_lines():
                yield json.loads(chunk.decode("utf-8"))

    def unlock(self):
        res = self.session.post(url=self.server_url + "api/unlock")
        if not res.ok:
            raise Exception(f"Failed to unlock the server: {res.status_code} {res.content}")
        return res.json()


class ServerLocked(Exception):
    pass
