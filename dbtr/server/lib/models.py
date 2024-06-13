from pydantic import field_validator, computed_field, BaseModel


class ServerRuntimeConfig(BaseModel):
    requester: str = "unknown"
    schedule: dict
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
        return self.schedule.get("cron_expression", "@now") == "@now"
