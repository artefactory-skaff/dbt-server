import os
import json
from dbt.cli.main import dbtRunner, dbtRunnerResult
from fastapi import HTTPException
from dbt.contracts.graph.manifest import Manifest

from utils import parse_args, parse_manifest_from_json, parse_command
from state import State


def run_job(manifest_json, state, command, args):

    state.set_status("running")

    manifest: Manifest = parse_manifest_from_json(manifest_json)
    dbt = dbtRunner(manifest=manifest)

    cli_args = [command] + parse_args(args)
    # ex: ['run', '--select', 'vbak_dbt', '--profiles-dir', '.']
    print(cli_args)

    res: dbtRunnerResult = dbt.invoke(cli_args)
    if not (res.success):
        state.set_status("failed")
        handle_exception(res)

    results = res.result
    state.set_status("success")
    return results


def handle_exception(res):
    print(res)
    if res.exception is not None:
        raise HTTPException(status_code=400, detail=res.exception)
    else:
        raise HTTPException(status_code=404, detail="dbt command failed")


if __name__ == '__main__':

    # we get all the environment variables
    bucket_name = os.getenv('BUCKET_NAME')
    dbt_command = os.environ.get("DBT_COMMAND")
    request_uuid = os.environ.get("UUID")

    main_command, args = parse_command(dbt_command + " --profiles-dir .")
    print("args", args)

    state = State(request_uuid)

    # we load manifest.json, profiles.yml and dbt_project.yml locally for dbt
    state.get_context_to_local()

    # we extract the manifest
    f = open('manifest.json', 'r')
    manifest = json.loads(f.read())
    f.close()

    results = run_job(manifest, state, main_command, args)
