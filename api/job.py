import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging_v2.resource import Resource
from google.cloud.logging_v2.handlers._monitored_resources import retrieve_metadata_server, _REGION_ID, _PROJECT_NAME
import os

import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.events.base_types import EventMsg
from fastapi import HTTPException
from dbt.contracts.graph.manifest import Manifest

from utils import parse_manifest_from_json
from state import State
from lab_logger import logging


def init_logger():
    logger = logging.getLogger(__name__)

    # find metadata about the execution environment
    region = retrieve_metadata_server(_REGION_ID)
    project = retrieve_metadata_server(_PROJECT_NAME)

    # build a manual resource object
    cr_job_resource = Resource(
        type="cloud_run_job",
        labels={
            "job_name": os.environ.get('CLOUD_RUN_JOB', 'unknownJobId'),
            "location":  region.split("/")[-1] if region else "",
            "project_id": project,
            "uuid": os.environ.get("UUID"),
        }
    )
    labels = {"uuid": os.environ.get("UUID")}
    client = google.cloud.logging.Client()  # grpc disable
    handler = CloudLoggingHandler(client, resource=cr_job_resource, labels=labels)
    logger.addHandler(handler)
    return logger


logger = init_logger()


def logger_version_callback(event: EventMsg):
    msg = event.info.msg
    match event.info.level:
        case "debug":
            logger.debug(msg)
        case "info":
            logger.info(msg)
        case "warn":
            logger.warn(msg)
        case "error":
            logger.error(msg)


def run_job(manifest_json, state: State, dbt_command: str):

    state.run_status = "running"

    manifest: Manifest = parse_manifest_from_json(manifest_json)
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_version_callback])

    # ex: ['run', '--select', 'vbak_dbt', '--profiles-dir', '.']
    cli_args = dbt_command.split(' ')
    logger.info("cli args: {args}".format(args=cli_args))

    res: dbtRunnerResult = dbt.invoke(cli_args)
    if not (res.success):
        state.run_status = "failed"
        handle_exception(res)

    results = res.result
    state.run_status = "success"
    return results


def handle_exception(res: dbtRunnerResult):
    logger.error({"error": res})
    if res.exception is not None:
        raise HTTPException(status_code=400, detail=res.exception)
    else:
        raise HTTPException(status_code=404, detail="dbt command failed")


if __name__ == '__main__':

    logger.info("Job started")

    # we get all the environment variables
    bucket_name = os.getenv('BUCKET_NAME')
    dbt_command = os.environ.get("DBT_COMMAND")
    request_uuid = os.environ.get("UUID")

    logger.info("DBT_COMMAND: "+dbt_command)

    state = State(request_uuid)

    # we load manifest.json and dbt_project.yml locally for dbt
    state.get_context_to_local()

    # we extract the manifest
    with open('manifest.json', 'r') as f:
        manifest = json.loads(f.read())

    results = run_job(manifest, state, dbt_command)
