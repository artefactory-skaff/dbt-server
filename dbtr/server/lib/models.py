import json
from typing import Any, Dict
import random
import string

from pydantic import field_validator, computed_field, model_validator
from snowflake import SnowflakeGenerator

from dbtr.common.job import DbtRemoteJob
from dbtr.server.lib.database import Database


class ServerJob(DbtRemoteJob):
    @computed_field
    def run_now(self) -> bool:
        return self.schedule_cron is None

    @model_validator(mode="before")
    def set_run_id(cls, values: Dict[str, Any]):
        run_id = values.get("run_id", None)
        schedule_cron = values.get("schedule_cron", None)

        if run_id is None or run_id.strip() == "":
            prefix = "schedule-" if schedule_cron else ""
            values["run_id"] = generate_id(prefix=prefix)
        return values

    @field_validator("schedule_name")
    def set_schedule_name(cls, v):
        if v is None or v.strip() == "":
            random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
            return f"dbt-server-{random_suffix}"
        return v

    def to_db(self, db: Database):
        entry = self.model_dump(exclude={"run_now", "humanized_model_selection"})
        entry["provider_config"] = json.dumps(entry["provider_config"])
        entry["dbt_runtime_config"] = json.dumps(entry["dbt_runtime_config"])
        db.execute(
            f"""
            INSERT INTO RunConfiguration (
                {", ".join(entry.keys())}
            ) VALUES ({", ".join(["?"] * len(entry.keys()))})
            """,
            list(entry.values())
        )

    @classmethod
    def from_db(cls, db: Database, run_id: str):
        run_config = db.fetchone("SELECT * FROM RunConfiguration WHERE run_id = ?", (run_id,))
        if run_config:
            run_config["provider_config"] = json.loads(run_config["provider_config"])
            run_config["dbt_runtime_config"] = json.loads(run_config["dbt_runtime_config"])
            return cls(**run_config)
        else:
            raise Exception(f"Run {run_id} not found")

    @classmethod
    def from_scheduled_run(cls, db: Database, schedule_run_id: str):
        run_config = db.fetchone("SELECT * FROM RunConfiguration WHERE run_id = ?", (schedule_run_id,))
        if run_config:
            run_config["provider_config"] = json.loads(run_config["provider_config"])
            run_config["dbt_runtime_config"] = json.loads(run_config["dbt_runtime_config"])
            run_config["run_id"] = generate_id()
            run_config["schedule_cron"] = None
            run_config["schedule_name"] = None
            run_config["schedule_description"] = None
            return cls(**run_config)
        else:
            raise Exception(f"Scheduled run {schedule_run_id} not found")


def generate_id(prefix: str = "") -> str:
    id_generator = SnowflakeGenerator(instance=1)
    id = next(id_generator)
    return f"{prefix}{id}"
