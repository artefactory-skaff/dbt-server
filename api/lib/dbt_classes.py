from pydantic import BaseModel
from typing import Dict


class DbtCommand(BaseModel):
    user_command: str
    processed_command: str = ''
    manifest: str
    dbt_project: str
    seeds: Dict[str, str] = None
    packages: str = None
    elementary: bool = False
