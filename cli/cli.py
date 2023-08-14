import click
import requests
import os
from dotenv import load_dotenv
from pathlib import Path
import json
import time

from utils import load_file

dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"
MANIFEST_FILENAME = os.getenv('MANIFEST_FILENAME')
DBT_PROJECT_FILE = os.getenv('DBT_PROJECT_FILE')


@click.command(help='Enter dbt command, ex: dbt-remote run --select test')
@click.argument('dbt_command')
@click.option('--manifest', '-m', default='./target/manifest.json', help='Manifest file, ex: ./target/manifest.json')
def cli(dbt_command: str, manifest: str):
    click.echo('Command: {0}'.format(dbt_command))
    server_res = send_command(dbt_command, manifest)

    uuid = json.loads(server_res)['uuid']
    click.echo("uuid: {0}".format(uuid))

    stream_log(uuid)


def send_command(command: str, manifest: str):
    url = SERVER_URL + "dbt"

    manifest_str = load_file(manifest)
    dbt_project_str = load_file(DBT_PROJECT_FILE)

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
            print(log)
            last_logs.append(log)
    last_logs = last_logs[-5:]
    return last_logs
