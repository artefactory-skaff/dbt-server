import requests
import json
import os
from dotenv import load_dotenv
from pathlib import Path
import re
import time
from timeit import default_timer as timer

dotenv_path = Path('.env.client')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"
MANIFEST_FILENAME = os.getenv('MANIFEST_FILENAME')
DBT_PROJECT_FILE = os.getenv('DBT_PROJECT_FILE')


def load_file(filename):
    f = open(filename, 'r')
    file_str = f.read()
    f.close()
    return file_str


def extract_manifest_filename_from_command(command):
    processed_command = command
    m = re.search('--manifest (.+?)( |$)', command)
    if m:
        manifest_filename = m.group(1)
        begin, end = m.span()
        if processed_command[end:] != "":
            processed_command = processed_command[:begin] + processed_command[end:]
        else:
            processed_command = processed_command[:begin-1]
    else:
        manifest_filename = MANIFEST_FILENAME
    return manifest_filename, processed_command


def send_command(command, dbt_project_file):
    url = SERVER_URL + "dbt"

    manifest_filename, processed_command = extract_manifest_filename_from_command(command)

    manifest_str = load_file(manifest_filename)
    dbt_project_str = load_file(dbt_project_file)

    data = {
            "command": processed_command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str
        }

    res = requests.post(url=url, json=data)
    return res.text


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


def handle_command(command: str):
    dbt_project_file = DBT_PROJECT_FILE

    start_all = timer()

    uuid = json.loads(send_command(command, dbt_project_file))["uuid"]
    print("uuid: "+uuid)

    start_execution_job = timer()

    time.sleep(16)
    run_status = json.loads(get_run_status(uuid))["run_status"]
    last_logs = []

    while run_status == "running":
        time.sleep(3)
        run_status_json = json.loads(get_run_status(uuid))
        run_status = run_status_json["run_status"]
        last_logs = show_last_logs(uuid, last_logs)

    end = timer()
    total_execution_time = end - start_all
    dbt_job_execution_time = end - start_execution_job

    while "END JOB" not in last_logs[-1]:
        time.sleep(1)
        last_logs = show_last_logs(uuid, last_logs)

    print("total excution time\t", total_execution_time)
    print("dbt job excution time\t", dbt_job_execution_time)


def main():

    commands = [
        "list",
        "--log-level info run --select vbak_dbt",
        "--debug list --manifest ../test-files/manifest.json"
    ]

    for command in commands:
        print()
        print(command)
        handle_command(command)


main()
