import click
import requests
import os
from dotenv import load_dotenv
from pathlib import Path
import json
import time
from datetime import datetime, timezone


dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"


def handling_server_errors(starting_time: str):
    click.echo(click.style("Errors detected in execution. Fetching errors from server...", fg="red"))
    errors = wait_for_errors(starting_time_str=starting_time, timeout=60)
    for err in errors:
        click.echo(err)


def wait_for_errors(starting_time_str: str, timeout: int):
    k = 0
    errors = []
    while k < timeout and (errors == [] or errors == ['None']):
        time.sleep(1)
        server_res = get_errors(starting_time_str)
        errors = json.loads(server_res)["logs"]
        k += 1
    if k == timeout:
        click.echo(click.style("Timeout. Check server logs directly on Cloud Logging", fg="red"))
    return errors


def get_errors(starting_time_str: str):
    url = SERVER_URL + "errors/" + starting_time_str
    res = requests.get(url=url)
    return res.text


def get_run_status(uuid: str):
    url = SERVER_URL + "job/" + uuid
    res = requests.get(url=url)
    return res.text


def get_last_logs(uuid: str):
    url = SERVER_URL + "job/" + uuid + '/last_logs'
    res = requests.get(url=url)
    return res.text


def show_last_logs(uuid: str):

    last_logs_json = json.loads(get_last_logs(uuid))
    logs = last_logs_json["run_logs"]

    for log in logs:
        show_log(log)
    if len(logs) > 0:
        return logs[-1]
    else:
        return ""


def show_log(log: str):
    if log == '':
        click.echo('')
        return
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


def current_time():
    now = datetime.now(timezone.utc)
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt_string


def load_file(filename):
    with open(filename, 'r') as f:
        file_str = f.read()
    return file_str
