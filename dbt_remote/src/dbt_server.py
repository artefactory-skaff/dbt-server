from dataclasses import dataclass
from datetime import datetime, timezone
import io
from pathlib import Path
import re
from subprocess import check_output
from time import sleep
from typing import Dict, List, Optional
import zipfile
import requests

from pydantic import BaseModel
from termcolor import colored
from google.auth import default
from google.auth.transport.requests import Request
from google.cloud import iam_credentials_v1
import google.oauth2.id_token

@dataclass
class DbtServerCommand:
    user_command: str
    dbt_native_params_overrides: Dict[str, str] | str
    dbt_project: Path | str
    profiles: Path | str
    packages: Optional[Path] | str
    manifest: Path
    seeds: Optional[Path]
    zipped_artifacts: bytes = None
    schedule: Optional[str] = None
    schedule_name: Optional[str] = None

    @classmethod
    def from_cli_config(cls, cli_config):
        return cls(
            user_command=cli_config.command,
            dbt_native_params_overrides=str(cli_config.dbt_native_params_overrides),
            dbt_project=Path(cli_config.project_dir) / "dbt_project.yml",
            profiles=Path(cli_config.profiles_dir) / "profiles.yml",
            manifest=Path(cli_config.manifest) / "manifest.json",
            packages=Path(cli_config.extra_packages) / "packages.yml" if cli_config.extra_packages is not None else None,
            seeds=Path(cli_config.seeds_path) if cli_config.seeds_path is not None else {},
            schedule=cli_config.schedule,
            schedule_name=cli_config.schedule_name,
        )

    def __post_init__(self):
        self.dbt_project = self.read_file(self.dbt_project)
        self.profiles = self.read_file(self.profiles)
        self.packages = self.read_file(self.packages) if self.packages is not None else {}
        self.zipped_artifacts = self.zip_artifacts()

    def zip_artifacts(self) -> Path:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zipf:
            zipf.writestr('manifest.json', self.read_file(self.manifest))
            for seed_file in self.seeds.iterdir():
                if seed_file.name.lower().endswith('.csv'):
                    seed_name = seed_file.name
                    seed_content = self.read_file(seed_file)
                    zipf.writestr('seeds/' + seed_name, seed_content)
        zip_buffer.seek(0)
        return zip_buffer

    def read_file(self, file_path: Path) -> str:
        with open(file_path, 'r') as f:
            file_str = f.read()
        return file_str


class DbtServerResponse(BaseModel):
    status_code: Optional[str] = None
    uuid: Optional[str] = None
    message: Optional[str] = None
    detail: Optional[str] = None
    links: Optional[Dict[str, str]] = None


class DbtServerLogResponse(BaseModel):
    status_code: Optional[str] = None
    run_status: Optional[str] = None
    run_logs: Optional[List[str]] = None


@dataclass
class DbtLogEntry:
    timestamp: datetime
    log_level: str
    emitter: str
    message: str

    @classmethod
    def from_raw_entry(cls, raw_entry: str):
        parts = raw_entry.split('\t')
        timestamp = datetime.strptime(parts[0], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).astimezone(tz=None)
        log_level = parts[1]
        message = parts[-1]
        emitter_match = re.search(r'\[(.*?)\]', message)
        emitter = emitter_match.group(1) if emitter_match else ""
        return cls(timestamp=timestamp, log_level=log_level, emitter=emitter, message=message)

    def __str__(self):
        level_color = {
            "INFO": "green",
            "WARN": "yellow",
            "ERROR": "red"
        }.get(self.log_level, "white")

        message_color = {
            "job": "blue"
        }.get(self.emitter, "white")

        return f"{colored(self.log_level, level_color)}    {colored(self.message, message_color)}"


class DbtServer:
    def __init__(self, server_url: str):
        self.server_url = server_url
        self.auth_session = self.get_auth_session()

    def send_command(self, command: DbtServerCommand) -> DbtServerResponse:
        endpoint = "dbt" if command.schedule is None else "schedule"
        url = self.server_url + endpoint

        data = {
            "server_url": self.server_url,
            **{k: v for k, v in command.__dict__.items() if k not in ["manifest", "seeds", "zipped_artifacts"]}
        }

        raw_response = self.auth_session.post(url=url, data=data, files={"zipped_artifacts": command.zipped_artifacts})

        response = DbtServerResponse.parse_raw(raw_response.text)
        response.status_code = raw_response.status_code

        if response.status_code >= 400 or response.detail is not None:
            raise Exception(f"Error {response.status_code} sending command to server: {response.detail}")

        return response

    def stream_logs(self, logs_link: str):
        run_status = "pending"
        while run_status in ["pending", "running"]:
            sleep(1)
            raw_response = self.auth_session.get(url=logs_link)
            response = DbtServerLogResponse.parse_raw(raw_response.text)
            run_status = response.run_status

            for log in response.run_logs:
                yield DbtLogEntry.from_raw_entry(log)

    def get_logs(self, uuid: str):
        raw_response = self.auth_session.get(url=f"{self.server_url}job/{uuid}/logs")
        response = DbtServerLogResponse.parse_raw(raw_response.text)
        return response.run_logs

    def list_schedules(self) -> Dict[str, str]:
        raw_response = self.auth_session.get(url=f"{self.server_url}schedule")
        response = raw_response.json()
        return response["schedules"]

    def delete_schedule(self, name: str):
        raw_response = self.auth_session.delete(url=f"{self.server_url}schedule/{name}")
        response = raw_response.json()
        return response["message"]

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

    @staticmethod
    def get_service_account_email(scopes=["https://www.googleapis.com/auth/cloud-platform"]):
        credentials, _ = default(scopes=scopes)
        credentials.refresh(Request())
        return credentials.service_account_email
