import click
import requests
import os
from dotenv import load_dotenv
from pathlib import Path
import json
import time


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
def cli(user_command: str, manifest: str, dbt_project: str, args):
    dbt_args = ' '.join(args)
    dbt_command = user_command + ' ' + dbt_args
    click.echo('Command: dbt {0}'.format(dbt_command))

    server_res = send_command(dbt_command, manifest, dbt_project)

    try:
        uuid = json.loads(server_res)['uuid']
        click.echo("uuid: {0}".format(uuid))

        stream_log(uuid)

    except json.decoder.JSONDecodeError:
        click.echo(server_res)


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
    last_logs = []

    while run_status == "running":
        time.sleep(1)
        run_status_json = json.loads(get_run_status(uuid))
        run_status = run_status_json["run_status"]
        last_logs = show_last_logs(uuid, last_logs)

    while "END JOB" not in last_logs[-1]:
        time.sleep(1)
        last_logs = show_last_logs(uuid, last_logs)


def get_run_status(uuid: str):
    url = SERVER_URL + "job/" + uuid
    res = requests.get(url=url)
    return res.text


def show_last_logs(uuid: str, last_logs=[]):
    run_status_json = json.loads(get_run_status(uuid))
    logs = run_status_json["entries"]  # gets 5 last logs from Firestore
    for log in logs:
        if log not in last_logs:
            show_log(log)
            last_logs.append(log)
    last_logs = last_logs[-5:]
    return last_logs


def show_log(log: str):
    parsed_log = log.split('\t')
    log_level = parsed_log[1]
    log_content = parsed_log[2]

    match (log_level):
        case 'INFO':
            log_color = 'green'
        case 'WARN':
            log_color = 'yellow'
        case 'ERROR':
            log_color = 'red'
        case _:
            log_color = 'black'

    click.echo(click.style(log_level, fg=log_color) + '\t' + log_content)


def load_file(filename):
    with open(filename, 'r') as f:
        file_str = f.read()
    return file_str
