from typing import Optional, Any, Dict
from pydantic import Field, field_validator, computed_field, BaseModel


class ServerRuntimeConfig(BaseModel):
    schedule: Optional[dict] = Field(default_factory=dict)
    requester: str = "unknown"
    cloud_provider: str
    server_url: str

    @field_validator("schedule")
    def validate_schedule(cls, v):
        if not v:
            return {}
        else:
            return v

    @computed_field
    def is_static_run(self) -> bool:
        return self.schedule.get("cron_expression", "@now") == "@now" or self.schedule.get("cron_expression") is None
