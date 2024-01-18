from dataclasses import dataclass
from typing import Dict
from fastapi import File, Form, UploadFile
import yaml


@dataclass
class DbtCommand:
    server_url: str = Form(...)
    user_command: str = Form(...)
    dbt_native_params_overrides: str | Dict = Form("{}")
    dbt_project: str | Dict = Form(...)
    profiles: str | Dict = Form(...)
    packages: str | Dict = Form("{}")
    zipped_artifacts: UploadFile = File(...)  # Manifest and seeds

    def __post_init__(self):
        self.dbt_native_params_overrides = yaml.safe_load(self.dbt_native_params_overrides)
        self.dbt_project = yaml.safe_load(self.dbt_project)
        self.profiles = yaml.safe_load(self.profiles)
        self.packages = yaml.safe_load(self.packages)

@dataclass
class ScheduledDbtCommand(DbtCommand):
    schedule: str = Form(...)
    schedule_name: str = Form(None)
