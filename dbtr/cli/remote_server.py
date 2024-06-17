import json
from typing import Callable, Dict, Iterator, Any
from io import BytesIO
import requests


class Server:
    def __init__(self, server_url, token_generator: Callable = None):
        self.server_url = server_url if server_url.endswith("/") else server_url + "/"
        self.token_generator = token_generator

        self.session = self.get_auth_session()

    def get_auth_session(self) -> requests.Session:
        if self.token_generator is None:
            return requests.Session()

        token = self.token_generator(server_url=self.server_url)
        session = requests.Session()
        session.headers.update({"Authorization": f"Bearer {token}"})
        return session

class DbtServer(Server):
    def __init__(self, server_url, token_generator: Callable = None):
        super().__init__(server_url, token_generator)

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
