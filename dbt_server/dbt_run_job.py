import os
import msgpack
import json
from typing import TypedDict
import threading

from google.cloud import logging
from click.parser import split_arg_string
from dbt.cli.main import dbtRunner, dbtRunnerResult, cli
from dbt.events.base_types import EventMsg
from dbt.events.functions import msg_to_json
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SeedNode
from fastapi import HTTPException

from lib.state import State
from lib.cloud_storage import CloudStorage
from lib.set_environment import set_env_vars_job
from lib.firestore import get_collection

callback_lock = threading.Lock()

BUCKET_NAME, DBT_COMMAND, UUID, logger, STATE = set_env_vars_job(CloudStorage(), get_collection("dbt-status"), logging.Client())


def prepare_and_execute_job(state: State) -> None:

    state.get_context_to_local()

    manifest = get_manifest()
    manifest = override_manifest_with_correct_seed_path(manifest)
    install_dependencies(state, manifest)

    run_dbt_command(state, manifest, DBT_COMMAND)

    with callback_lock:
        log = "[job]Command successfully executed"
        logger.log("INFO", log)

    log = "[job]END JOB"
    logger.log("INFO", log)


def install_dependencies(state: State, manifest: Manifest) -> None:
    packages_path = './packages.yml'
    check_file = os.path.isfile(packages_path)
    if check_file:
        with open('packages.yml', 'r') as f:
            packages_str = f.read()
        if packages_str != '':
            run_dbt_command(state, manifest, 'deps')


def run_dbt_command(state: State, manifest: Manifest, dbt_command: str) -> None:

    state.run_status = "running"

    manifest.build_flat_graph()
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_callback])

    args_list = split_arg_string(dbt_command)
    res_dbt: dbtRunnerResult = dbt.invoke(args_list)

    if res_dbt.success:
        state.run_status = "success"
    else:
        state.run_status = "failed"

        log = "[job]END JOB"
        with callback_lock:
            logger.log("INFO", log)
        handle_exception(res_dbt.exception)


def logger_callback(event: EventMsg):
    state = STATE
    log_configuration = get_user_request_log_configuration(state.user_command)

    user_log_format = log_configuration['log_format']
    if user_log_format == "json":
        msg = msg_to_json(event).replace('\n', '  ')
    else:
        msg = "[dbt]"+event.info.msg.replace('\n', '  ')

    user_log_level = log_configuration['log_level']
    match event.info.level:
        case "debug":
            if user_log_level == "debug":
                with callback_lock:
                    logger.log(event.info.level.upper(), msg)
            else:
                logger.logger.debug(msg)

        case "info":
            if user_log_level in ["debug", "info"]:
                with callback_lock:
                    logger.log(event.info.level.upper(), msg)
            else:
                logger.logger.info(msg)

        case "warn":
            if user_log_level in ["debug", "info", "warn"]:
                with callback_lock:
                    logger.log(event.info.level.upper(), msg)
            else:
                logger.logger.warn(msg)

        case "error":
            with callback_lock:
                logger.log(event.info.level.upper(), msg)


LogConfiguration = TypedDict('LogConfiguration', {'log_format': str, 'log_level': str})


def get_user_request_log_configuration(user_command: str) -> LogConfiguration:
    command_args_list = split_arg_string(user_command)
    command_context = cli.make_context(info_name='', args=command_args_list)
    command_params = command_context.params
    log_format, log_level = command_params['log_format'], command_params['log_level']
    if command_params["quiet"]:
        log_level = 'error'
    return {"log_format": log_format, "log_level": log_level}


def handle_exception(dbt_exception: BaseException | None):
    logger.logger.error({"error": dbt_exception})
    if dbt_exception is not None:
        raise HTTPException(status_code=400, detail=dbt_exception)
    else:
        raise HTTPException(status_code=404, detail="dbt command failed")


def get_manifest() -> Manifest:
    with open('manifest.json', 'r') as f:
        manifest_json = json.loads(f.read())
    partial_parse = msgpack.packb(manifest_json)
    manifest: Manifest = Manifest.from_msgpack(partial_parse)
    return manifest


def override_manifest_with_correct_seed_path(manifest: Manifest) -> Manifest:
    """
        Seeds' node root_path will be '.' during the Cloud Run Job.
    """
    nodes_list = manifest.nodes.keys()
    for node_name in nodes_list:
        node = manifest.nodes[node_name]
        if isinstance(node, SeedNode):
            node.root_path = '.'
    return manifest


if __name__ == '__main__':

    logger.log("INFO", "[job]Job started")
    state = STATE
    prepare_and_execute_job(state)
