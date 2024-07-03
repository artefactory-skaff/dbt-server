from typing import Dict, Optional

from pydantic import Field, BaseModel


class DbtRemoteJob(BaseModel):
    run_id: Optional[str] = Field(default=None, validate_default=True)
    run_conf_version: int = 1
    project: str
    server_url: str
    cloud_provider: str
    provider_config: Dict = Field(default_factory=dict)
    requester: str = "unknown"
    dbt_runtime_config: Dict = Field(default_factory=dict)
    schedule_name: Optional[str] = None
    schedule_cron: Optional[str] = None
    schedule_description: Optional[str] = None
