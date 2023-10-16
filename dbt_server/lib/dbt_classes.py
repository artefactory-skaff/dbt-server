import base64
from typing import Dict
from fastapi import HTTPException

from pydantic import BaseModel, validator

from lib.command_processor import process_command

class DbtCommand(BaseModel):
    server_url: str
    user_command: str
    processed_command: str = None
    manifest: str                   # can be base64 encoded
    dbt_project: str                # can be base64 encoded
    profiles: str                   # can be base64 encoded
    seeds: Dict[str, str] = {}      # can be base64 encoded
    packages: str = ""              # can be base64 encoded

    @validator("manifest", "dbt_project", "profiles", "packages")
    def process_base_64_field(cls, value, values, config, field):
        try:
            if base64.b64encode(base64.b64decode(value)) == bytes(value, 'ascii'):
                return base64.b64decode(value)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Invalid base64 encoding for field {field}")

    @validator("seeds")
    def process_base_64_seeds(cls, value):
        seeds = {}
        for k, v in value.items():
            try:
                if base64.b64encode(base64.b64decode(v)) == bytes(v, 'ascii'):
                    seeds[k] = base64.b64decode(v)
                else:
                    HTTPException(status_code=400, detail=f"Invalid base64 encoding for seed {k}")
            except Exception:
                HTTPException(status_code=400, detail=f"Invalid base64 encoding for seed {k}")
        return seeds

    @validator('processed_command', always=True)
    def process_user_command(cls, value, values):
        return process_command(values["user_command"])
