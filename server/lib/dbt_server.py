import json
import logging
from dataclasses import dataclass
from typing import Optional, Union, Any

import uvicorn
import yaml
from fastapi import FastAPI, status, UploadFile, Form, File, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator, computed_field

__version__ = "0.0.1"  # TODO: handle this properly


class DBTRuntimeConfig(BaseModel):
    param_1: Optional[str] = "value_1"


class ServerRuntimeConfig(BaseModel):
    cron_schedule: Optional[str] = "@now"

    @field_validator("cron_schedule")
    def validate_cron_expression(cls, cron_value: Any):
        # TODO: add validation
        return cron_value

    @computed_field
    def is_static_run(self) -> bool:
        return self.cron_schedule == "@now"


@dataclass
class RunCommandParameters:
    dbt_runtime_config: Union[str, dict] = Form(...)
    server_runtime_config: Union[str, dict] = Form(...)


class DBTServer:
    def __init__(self, logger: logging.Logger, port: int, storage_backend=None, schedule_backend=None):

        self.app = FastAPI(
            title="dbt-server",
            description="A server to run dbt commands in the cloud",
            version=__version__,
            docs_url="/docs"
        )
        self.logger = logger
        self.port = port
        self.storage_backend = storage_backend
        self.schedule_backend = schedule_backend

    def start(self):
        self.__setup_api_routes()
        uvicorn.run(self.app, host="0.0.0.0", port=self.port)

    def __setup_api_routes(self):
        @self.app.post("/api/run")
        async def create_run(
                dbt_runtime_config: str = Form(...),
                server_runtime_config: str = Form("{}"),
                dbt_remote_artifacts: UploadFile = File(...)
        ):
            """
            if a schedule is given:
                - we upload the data into the scheduled run namespace via storage backend (after unzip)
                - create a scheduler
                - return schedule info and other metadata to user (+ route to trigger schedule in case of dev)
            if no schedule is given (default to @now):
                - we upload the data into the static run namespace via storage backend (after unzip)
                - we execute it locally
                - we save output manifest via storage backend
                - we return state of run (⚠︎ how to handle log)
            """
            dbt_runtime_config = DBTRuntimeConfig(**json.loads(dbt_runtime_config))
            server_runtime_config = ServerRuntimeConfig(**json.loads(server_runtime_config))
            if server_runtime_config.is_static_run:
                self.logger.info("Creating static run")
                self.logger.debug("Unzipping artifact files %s", dbt_remote_artifacts.filename)
                self.logger.debug("Uploading artifacts to storage backend")
                self.logger.debug("Running dbt command locally")
                self.logger.debug("Uploading artifacts to storage backend")
                return JSONResponse(status_code=status.HTTP_200_OK, content={"type": "static"})
            else:
                self.logger.info("Creating scheduled run")
                self.logger.debug("Unzipping artifacts")
                self.logger.debug("Uploading artifacts to storage backend")
                self.logger.debug("Creating scheduler")
                return JSONResponse(status_code=status.HTTP_201_CREATED, content={"type": "scheduled"})

        @self.app.get("/api/run/{run_id}")
        def get_run(run_id: str):
            self.logger.info("Retrieving artifacts from storage backend")
            return JSONResponse(status_code=status.HTTP_200_OK, content={"status": "in progress", "run_id": run_id})

        @self.app.post("/api/schedule/{schedule_run_id}/trigger")
        def trigger_scheduled_run(schedule_run_id: str):
            self.logger.info("Starting run from schedule %s", schedule_run_id)
            return JSONResponse(status_code=status.HTTP_200_OK, content={"schedule_run_id": schedule_run_id})

        @self.app.get("/api/version")
        async def get_version():
            return JSONResponse(status_code=status.HTTP_200_OK, content={"version": __version__})

        @self.app.get("/api/check")
        async def check():
            return { "response": f"Running dbt-server"}
