from typing import List, Optional
from pydantic import BaseModel


class DbtResponse(BaseModel):
    status_code: Optional[str] = None
    uuid: Optional[str] = None
    detail: Optional[str] = None


class DbtResponseRunStatus(BaseModel):
    status_code: Optional[str] = None
    run_status: Optional[str] = None


class DbtResponseLogs(BaseModel):
    status_code: Optional[str] = None
    run_logs: Optional[List[str]] = None
