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
@click.option('--packages', default='', help='packages.yml file, by default none')
@click.option('--set_timer', is_flag=True, help='Set flag to record the job execution duration')
@click.option('--elementary', is_flag=True, help='Set flag to record the job execution duration')
def cli(user_command: str, manifest: str, dbt_project: str, packages: str, set_timer: bool, elementary: bool, args):
    dbt_args = ' '.join(args)
    dbt_command = user_command + ' ' + dbt_args
    click.echo('Command: dbt {0}'.format(dbt_command))

    starting_time = current_time()
    click.echo("Starting time: {0}".format(starting_time))

    if set_timer:
        click.echo('Starting timer for complete execution')
        start_all = timer()

    server_res = send_command(dbt_command, manifest, dbt_project, packages, elementary)

    try:

        uuid = json.loads(server_res)['uuid']
        click.echo("uuid: {0}".format(uuid))

        if set_timer:
            click.echo('Starting timer for job execution')
            start_execution_job = timer()

        stream_log(uuid, elementary)

        if set_timer:
            end = timer()
            click.echo("total excution time\t{0}".format(str(end - start_all)))
            click.echo("dbt job excution time\t{0}".format(str(end - start_execution_job)))

    except json.decoder.JSONDecodeError:
        if set_timer:
            end = timer()

        click.echo(server_res)
        handling_server_errors(starting_time)


def send_command(command: str, manifest: str, dbt_project: str, packages: str, elementary: bool):
    url = SERVER_URL + "dbt"

    manifest_str = load_file(manifest)
    dbt_project_str = load_file(dbt_project)

    data = {
            "command": command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str
        }

    if packages != '':
        packages_str = load_file(packages)
        data["packages"] = packages_str

    if elementary:
        data["elementary"] = True

    res = requests.post(url=url, json=data)
    return res.text


def stream_log(uuid: str, elementary: bool):
    click.echo("Waiting for job execution...")
    time.sleep(16)
    run_status = json.loads(get_run_status(uuid))["run_status"]
    last_timestamp_str = current_time()

    i, timeout = 0, 60
    while run_status == "running" and i < timeout:
        time.sleep(1)
        run_status_json = json.loads(get_run_status(uuid))
        run_status = run_status_json["run_status"]
        last_log, last_timestamp_str = show_last_logs(uuid, last_timestamp_str)
        i += 1

    if run_status == "success":
        i, timeout = 0, 60
        if elementary:  # elementary report takes longer to build
            timeout = 120
        while "END REPORT" not in last_log and i < timeout:
            time.sleep(1)
            last_log, last_timestamp_str = show_last_logs(uuid, last_timestamp_str)
            i += 1
        show_last_logs(uuid, last_timestamp_str)
        if i == timeout:
            click.echo(click.style("ERROR", fg="red") + '\t' + "Timeout while waiting for logs")
    else:
        click.echo(click.style("ERROR", fg="red") + '\t' + "Job failed")
