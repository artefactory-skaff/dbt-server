import click
import requests
import os
from dotenv import load_dotenv
from pathlib import Path
import json
import time
from timeit import default_timer as timer
from utils import current_time, handling_server_errors, show_last_logs, load_file, get_run_status


dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"


@click.command(context_settings=dict(ignore_unknown_options=True,),
               help='Enter dbt command, ex: dbt-remote run --select test')
@click.argument('user_command')
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option('--manifest', '-m', default='./target/manifest.json',
              help='Manifest file, by default: ./target/manifest.json')
@click.option('--dbt_project', default='./dbt_project.yml', help='dbt_project file, by default: ./dbt_project.yml')
@click.option('--set_timer', is_flag=True, help='Set flag to record the job execution duration')
def cli(user_command: str, manifest: str, dbt_project: str, set_timer: bool, args):
    dbt_args = ' '.join(args)
    dbt_command = user_command + ' ' + dbt_args
    click.echo('Command: dbt {0}'.format(dbt_command))

    starting_time = current_time()
    click.echo("Starting time: {0}".format(starting_time))

    if set_timer:
        click.echo('Starting timer for complete execution')
        start_all = timer()

    server_res = send_command(dbt_command, manifest, dbt_project)

    try:

        uuid = json.loads(server_res)['uuid']
        click.echo("uuid: {0}".format(uuid))

        if set_timer:
            click.echo('Starting timer for job execution')
            start_execution_job = timer()

        stream_log(uuid)

        if set_timer:
            end = timer()
            click.echo("total excution time\t{0}".format(str(end - start_all)))
            click.echo("dbt job excution time\t{0}".format(str(end - start_execution_job)))

    except json.decoder.JSONDecodeError:
        if set_timer:
            end = timer()

        click.echo(server_res)
        handling_server_errors(starting_time)


def send_command(command: str, manifest: str, dbt_project: str):
    url = SERVER_URL + "dbt"

    manifest_str = load_file(manifest)
    dbt_project_str = load_file(dbt_project)

    data = {
            "command": command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str
        }

    res = requests.post(url=url, json=data)
    return res.text


def stream_log(uuid: str):
    click.echo("Waiting for job execution...")
    time.sleep(16)
    run_status = json.loads(get_run_status(uuid))["run_status"]
    last_timestamp_str = current_time()

    while run_status == "running":
        time.sleep(1)
        run_status_json = json.loads(get_run_status(uuid))
        run_status = run_status_json["run_status"]
        last_log, last_timestamp_str = show_last_logs(uuid, last_timestamp_str)

    while "END JOB" not in last_log:
        time.sleep(1)
        last_log, last_timestamp_str = show_last_logs(uuid, last_timestamp_str)
