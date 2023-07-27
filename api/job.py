import os
from dotenv import load_dotenv
from dbt.cli.main import dbtRunner, dbtRunnerResult
from fastapi import HTTPException
from dbt.contracts.graph.manifest import Manifest

from cloud_storage import read_manifest,write_to_bucket,load_file
from utils import parse_args, parse_manifest_from_json, parse_command
from firestore import set_status

BUCKET_NAME='dbt-stc-test'

def run_job(manifest_json,request_uuid,command,args):

    set_status(request_uuid,"running")

    manifest: Manifest = parse_manifest_from_json(manifest_json)
    dbt = dbtRunner(manifest=manifest)

    cli_args = [command] + parse_args(args) # ex: ['run', '--select', 'vbak_dbt', '--profiles-dir', '.']
    print(cli_args)

    res: dbtRunnerResult = dbt.invoke(cli_args)
    if not(res.success):
        set_status(request_uuid,"failed")
        handle_exception(res)

    results = res.result
    set_status(request_uuid,"success")
    return results


def handle_exception(res):
    print(res)
    if res.exception != None:
        raise HTTPException(status_code=400, detail=res.exception)
    else:
        raise HTTPException(status_code=404, detail="dbt command failed")

if __name__ == '__main__':

    load_dotenv()
    # we get all the environment variables
    bucket_name = BUCKET_NAME
    dbt_command = os.environ.get("DBT_COMMAND")
    request_uuid = os.environ.get("UUID")

    main_command, args = parse_command(dbt_command + " --profiles-dir .")
    print("args",args)

    # we load profiles.yml and dbt_project.yml locally for dbt
    load_file(bucket_name,request_uuid+"_profiles.yml","profiles.yml")
    load_file(bucket_name,request_uuid+"_dbt_project.yml","dbt_project.yml")

    # we extract the manifest
    manifest_name = request_uuid + "_manifest.json"
    manifest = read_manifest(bucket_name,manifest_name)

    results = run_job(manifest,request_uuid,main_command,args)

    # we write the corresponding file on cloud storage
    write_to_bucket(bucket_name,'blob2',str(results))
