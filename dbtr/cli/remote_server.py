import json
from typing import Callable, Dict, Iterator, Any
from io import BytesIO
import requests

from dbtr.cli.exceptions import Server400, Server500, ServerConnectionError, ServerLocked, ServerUnlockFailed


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
        try:
            _server_runtime_config = self.format_server_runtime_config(server_runtime_config)
            res = self.session.post(
                url=self.server_url + "api/run",
                files={"dbt_remote_artifacts": dbt_remote_artifacts},
                data={
                    "dbt_runtime_config": json.dumps(dbt_runtime_config),
                    "server_runtime_config": json.dumps(_server_runtime_config)
                },
            )
        except requests.exceptions.ConnectionError as e:
            raise ServerConnectionError(
                f"Failed to connect to {self.server_url}, make sure the server is running and accessible.")

        if not res.ok:
            if res.status_code == 423:
                raise ServerLocked(res.json()["lock_info"])
            elif 400 <= res.status_code < 500:
                raise Server400(f"Error {res.status_code}: {res.content}")
            else:
                raise Server500(f"Server Error {res.status_code}: {res.content}")
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
            raise ServerUnlockFailed(f"Failed to unlock the server: {res.status_code} {res.content}")
        return res.json()

    @staticmethod
    def format_server_runtime_config(config: dict) -> dict:
        schedule_config = {}
        server_runtime_config = {}
        for key_param, val_param in config.items():
            if key_param.startswith("schedule"):
                new_key_param = key_param[len("schedule") + 1:]
                schedule_config[new_key_param] = val_param
            else:
                server_runtime_config[key_param] = val_param
        server_runtime_config["schedule"] = schedule_config
        return server_runtime_config
