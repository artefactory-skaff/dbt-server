import logging
import os
import msgpack
import json
import threading

from click.parser import split_arg_string
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.events.base_types import EventMsg
from dbt.events.functions import msg_to_json
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import SeedNode
from fastapi import HTTPException

from lib.logger import DbtLogger
from lib.state import State

BUCKET_NAME = os.getenv("BUCKET_NAME")
DBT_COMMAND = os.getenv("DBT_COMMAND")
UUID = os.getenv("UUID")


callback_lock = threading.Lock()
logger = DbtLogger(server=False)
state = State.from_uuid(UUID)
logger.state = state

def prepare_and_execute_job() -> None:
    state.save_context_to_local()
    manifest = get_manifest()
    manifest = override_manifest_with_correct_seed_path(manifest)
    install_dependencies(manifest)
    run_dbt_command(manifest, DBT_COMMAND)

    with callback_lock:
        logger.log("INFO", "[job] Command successfully executed")
    logger.log("INFO", "[job] dbt-remote job finished")


def install_dependencies(manifest: Manifest) -> None:
    packages_path = './packages.yml'
    check_file = os.path.isfile(packages_path)
    if check_file:
        with open('packages.yml', 'r') as f:
            packages_str = f.read()
        if packages_str != '':
            run_dbt_command(manifest, 'deps')


def run_dbt_command(manifest: Manifest, dbt_command: str) -> None:

    state.run_status = "running"

    manifest.build_flat_graph()
    dbt = dbtRunner(manifest=manifest, callbacks=[logger_callback])

    args_list = split_arg_string(dbt_command)
    res_dbt: dbtRunnerResult = dbt.invoke(
        args_list,
        **dict(
            state.dbt_native_params_overrides,
             **{
                "log_format": "json",
                "log_level": "debug",
                "debug": True,
            }
        )
    )

    if res_dbt.success:
        state.run_status = "success"
    else:
        state.run_status = "failed"

        with callback_lock:
            logger.log("INFO", "[job] dbt-remote job finished")
        handle_exception(res_dbt.exception)


def logger_callback(event: EventMsg):
    user_log_format = state.dbt_native_params_overrides['log_format'] if 'log_format' in state.dbt_native_params_overrides else "default"
    user_log_level_str = state.dbt_native_params_overrides['log_level'] if 'log_level' in state.dbt_native_params_overrides else "info"

    if user_log_format == "json":
        msg = msg_to_json(event).replace('\n', '  ')
    else:
        msg = "[dbt] " + event.info.msg.replace('\n', '  ')

    user_log_level = logging.getLevelName(user_log_level_str.upper())
    event_log_level = logging.getLevelName(event.info.level.upper())

    if event_log_level >= user_log_level:
        with callback_lock:
            logger.log(event.info.level.upper(), msg)
    else:
        logger.logger.log(event_log_level, msg)


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
    logger.log("INFO", "[job] Job started")
    prepare_and_execute_job()
