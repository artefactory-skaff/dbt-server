from pydantic import BaseModel
from typing import Dict
from dataclasses import dataclass


class DbtCommand(BaseModel):
    server_url: str
    user_command: str
    processed_command: str = ''
    manifest: str                   # can be base64 encoded
    dbt_project: str                # can be base64 encoded
    profiles: str                   # can be base64 encoded
    seeds: Dict[str, str] = None    # can be base64 encoded
    packages: str = None            # can be base64 encoded


@dataclass
class FollowUpLink:
    """Links sent back by the dbt-server."""
    action_name: str
    link: str
