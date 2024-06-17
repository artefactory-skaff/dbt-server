from typing import Optional, Any

from pydantic import field_validator, computed_field, BaseModel


class ServerRuntimeConfig(BaseModel):
    cron_schedule: Optional[str] = "@now"
    requester: str = "unknown"

    @field_validator("cron_schedule")
    def validate_cron_expression(cls, cron_value: Any):
        # TODO: add validation
        return cron_value

    @computed_field
    def is_static_run(self) -> bool:
        return self.cron_schedule == "@now"
