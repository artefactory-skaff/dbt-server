from pydantic import BaseModel
from typing import Dict
from dataclasses import dataclass


class DbtCommand(BaseModel):
    server_url: str
    user_command: str
    processed_command: str = ''
    manifest: str
    dbt_project: str
    seeds: Dict[str, str] = None
    packages: str = None
    elementary: bool = False


@dataclass
class FollowUpLink:
    """Links sent back by the dbt-server."""
    action_name: str
    link: str
