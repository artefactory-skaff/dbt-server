import os

import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.events.base_types import EventMsg
from fastapi import HTTPException
from dbt.contracts.graph.manifest import Manifest

from utils import parse_manifest_from_json
from state import State
from new_logger import init_logger

logger = init_logger()


def logger_callback(event: EventMsg):
    state = State(os.environ.get("UUID"))
    msg = event.info.msg
    user_log_level = state.log_level
    match event.info.level:
        case "debug":
            logger.debug(msg)
            if user_log_level == "debug":
                state.run_logs = "DEBUG\t" + msg
        case "info":
            logger.info(msg)
            if user_log_level in ["debug", "info"]:
                state.run_logs = "INFO\t" + msg
        case "warn":
            logger.warn(msg)
            if user_log_level in ["debug", "info", "warn"]:
                state.run_logs = "WARN\t" + msg
        case "error":
            logger.error(msg)
            state.run_logs = "ERROR\t" + msg


def run_job(manifest_json, state: State, dbt_command: str):

    state.run_status = "running"

    manifest: Manifest = parse_manifest_from_json(manifest_json)
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_callback])

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

    state.run_logs = "INFO\t END JOB"
