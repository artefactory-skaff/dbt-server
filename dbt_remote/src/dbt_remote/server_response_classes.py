from typing import List, Optional
from pydantic import BaseModel
from dataclasses import dataclass


@dataclass
class FollowUpLink:
    """Links sent back by the dbt-server."""
    action_name: str
    link: str


class DbtResponse(BaseModel):
    status_code: Optional[str] = None
    uuid: Optional[str] = None
    detail: Optional[str] = None  # error message
    links: Optional[List[FollowUpLink]] = None


class DbtResponseRunStatus(BaseModel):
    status_code: Optional[str] = None
    run_status: Optional[str] = None


class DbtResponseLogs(BaseModel):
    status_code: Optional[str] = None
    run_logs: Optional[List[str]] = None
