from fastapi import FastAPI, status

import os
import uuid
import uvicorn
import sys
import traceback
from fastapi import HTTPException


from api.config import Settings
from api.clients import LOGGER
from api.lib.job import Job, JobFactory
from api.lib.dbt_classes import DbtCommand, FollowUpLink
from api.lib.command_processor import process_command
from api.lib.state import State
from api.lib.cloud_storage import CloudStorageFactory
from api.lib.metadata_document import MetadataDocumentFactory


settings = Settings()
app = FastAPI()
CLOUD_STORAGE_INSTANCE = CloudStorageFactory().create(settings.cloud_storage_service)


@app.post("/dbt", status_code=status.HTTP_202_ACCEPTED)
def run_command(dbt_command: DbtCommand):
    request_uuid = str(uuid.uuid4())
    metadata_document = MetadataDocumentFactory().create(
        settings.metadata_document_service, settings.collection_name, request_uuid
    )
    state = State(request_uuid, metadata_document)
    state.run_status = "pending"
    LOGGER.uuid = request_uuid

    LOGGER.log("INFO", f"Received command '{dbt_command.user_command}'")
    state.user_command = dbt_command.user_command

    processed_command = process_command(dbt_command.user_command)
    LOGGER.log("INFO", f"Processed command: {processed_command}")
    dbt_command.processed_command = processed_command

    state.load_context(dbt_command)

    job = Job(JobFactory().create(settings.job_service))
    job_name = job.create(state, dbt_command)
    job.launch(state, job_name)

    return {
        "uuid": request_uuid,
        "links": [
            FollowUpLink("run_status", f"{dbt_command.server_url}job/{request_uuid}"),
            FollowUpLink(
                "last_logs", f"{dbt_command.server_url}job/{request_uuid}/last_logs"
            ),
        ],
    }


@app.get("/job/{uuid}", status_code=status.HTTP_200_OK)
def get_job_state(uuid: str):
    job_state = State(
        uuid,
        MetadataDocumentFactory().create(
            settings.metadata_document_service, settings.collection_name, uuid
        ),
    )
    run_status = job_state.run_status
    return {"run_status": run_status}


@app.get("/job/{uuid}/last_logs", status_code=status.HTTP_200_OK)
def get_last_logs(uuid: str):
    job_state = State(
        uuid,
        MetadataDocumentFactory().create(
            settings.metadata_document_service, settings.collection_name, uuid
        ),
    )
    logs = job_state.get_last_logs()
    return {"run_logs": logs}


@app.get("/job/{uuid}/report", status_code=status.HTTP_200_OK)
def get_report(uuid: str):
    state = State(
        uuid,
        MetadataDocumentFactory().create(
            settings.metadata_document_service, settings.collection_name, uuid
        ),
    )
    cloud_storage_folder = state.cloud_storage_folder
    report = CLOUD_STORAGE_INSTANCE.get_file(
        settings.bucket_name, cloud_storage_folder + "/elementary_report.html"
    )

    url = CLOUD_STORAGE_INSTANCE.get_file_console_url(
        settings.bucket_name, f"{cloud_storage_folder}/elementary_report.html"
    )

    return {"url": url}


@app.get("/check", status_code=status.HTTP_200_OK)
def check():
    LOGGER.log("INFO", f"Running dbt-server on port : {settings.port}")
    return {"response": f"Running dbt-server on port {settings.port}"}


if __name__ == "__main__":
    uvicorn.run(
        "dbt_server:app",
        port=settings.port,
        host="0.0.0.0",
        reload=True,
    )
