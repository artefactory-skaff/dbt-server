import click
import requests
import os
from dotenv import load_dotenv
from pathlib import Path

from utils import load_file, extract_manifest_filename_from_command

dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"
MANIFEST_FILENAME = os.getenv('MANIFEST_FILENAME')
DBT_PROJECT_FILE = os.getenv('DBT_PROJECT_FILE')


@click.command(help='Enter dbt command, ex: dbt-remote run --select test')
@click.argument('dbt_command')
def cli(dbt_command: str):
    click.echo('Command: {0}'.format(dbt_command))
    res = send_command(dbt_command)
    click.echo(res)


def send_command(command: str):
    url = SERVER_URL + "dbt"

    manifest_filename, processed_command = extract_manifest_filename_from_command(command, MANIFEST_FILENAME)

    manifest_str = load_file(manifest_filename)
    dbt_project_str = load_file(DBT_PROJECT_FILE)

    data = {
            "command": processed_command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str
        }

    res = requests.post(url=url, json=data)
    return res.text
