from typing import Dict, Optional

from dataclasses import dataclass

from pydantic import BaseModel


class DbtCommand(BaseModel):
    server_url: str
    user_command: str
    processed_command: str = ""
    manifest: str
    dbt_project: str
    seeds: Optional[Dict[str, str]] = None
    packages: Optional[str] = None
    elementary: bool = False


@dataclass
class FollowUpLink:
    """Links sent back by the dbt-server."""

    action_name: str
    link: str
