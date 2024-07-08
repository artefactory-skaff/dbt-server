import json
from typing import Callable, Dict, Iterator, Any
from io import BytesIO
import requests

from cron_descriptor import get_description, Options

from dbtr.common.exceptions import Server400, Server500, ServerConnectionError, ServerLocked, ServerUnlockFailed


class AuthSession(requests.Session):
    def __init__(self, token_generator: Callable, server_url: str):
        super().__init__()
        self.token_generator = token_generator
        self.server_url = server_url
        self.update_token()

    def update_token(self):
        token = self.token_generator(server_url=self.server_url)
        self.headers.update({"Authorization": f"Bearer {token}"})

    def request(self, method, url, **kwargs) -> requests.Response:
        response = super().request(method, url, **kwargs)
        if response.status_code == 401:
            self.update_token()
            response = super().request(method, url, **kwargs)
        return response

class Server:
    def __init__(self, server_url, token_generator: Callable = None):
        self.server_url = server_url if server_url.endswith("/") else server_url + "/"
        self.token_generator = token_generator

        self.session = self.get_auth_session()

    def get_auth_session(self) -> AuthSession:
        if self.token_generator is None:
            return requests.Session()
        return AuthSession(self.token_generator, self.server_url)


class DbtServer(Server):
    def __init__(self, server_url, token_generator: Callable = None):
        super().__init__(server_url, token_generator)

    def send_task(
            self,
            dbt_remote_artifacts: BytesIO,
            server_runtime_config: Dict[str, str]
    ) -> Iterator[str]:
        result = self.create_task(dbt_remote_artifacts, server_runtime_config)
        if result["type"] == "static":
            for log in self.stream_log(result["next_url"]):
                yield log.get("info", {}).get("msg", "")
        elif result["type"] == "scheduled":
            cron_description_options = Options()
            cron_description_options.verbose = True
            yield f"Job {result['schedule_name']} has been created to run {get_description(result['schedule_cron'], options=cron_description_options)}."
            yield f"Job can be manually triggered at {result['next_url']}"

    def check_version_match(self):
        raw_response = self.session.get(url=self.server_url + "version")
        response = raw_response.json()
        print(f"Server version: {response}")

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
        server_runtime_config: Dict[str, Any]
    ) -> str:
        try:
            res = self.session.post(
                url=self.server_url + "api/run",
                files={"dbt_remote_artifacts": dbt_remote_artifacts},
                data={"server_runtime_config": json.dumps(server_runtime_config)},
            )
        except requests.exceptions.ConnectionError as e:
            raise ServerConnectionError(
                f"Failed to connect to {self.server_url}, make sure the server is running and accessible.")

        if res.ok:
            result = res.json()
            return result
        else:
            if res.status_code == 423:
                raise ServerLocked(res.json()["lock_info"])
            elif 400 <= res.status_code < 500:
                raise Server400(f"Error {res.status_code}: {res.content}")
            else:
                raise Server500(f"Server Error {res.status_code}: {res.content}")


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
