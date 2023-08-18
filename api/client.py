import requests
import json
import os
from dotenv import load_dotenv
from pathlib import Path
import re
import time
from timeit import default_timer as timer
import sys
from datetime import datetime, timezone

dotenv_path = Path('.env.client')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"

if len(sys.argv) == 2:
    if sys.argv[1] == "--local":
        SERVER_URL = os.getenv('LOCAL_URL')+"/"

MANIFEST_FILENAME = os.getenv('MANIFEST_FILENAME')
ELEMENTARY_MANIFEST_FILENAME = os.getenv('ELEMENTARY_MANIFEST_FILENAME')

DBT_PROJECT_FILE = os.getenv('DBT_PROJECT_FILE')
ELEMENTARY_DBT_PROJECT_FILE = os.getenv('ELEMENTARY_DBT_PROJECT_FILE')

PACKAGES_FILE = os.getenv('PACKAGES_FILE')


def load_file(filename):
    f = open(filename, 'r')
    file_str = f.read()
    f.close()
    return file_str


def extract_manifest_filename_from_command(command):
    processed_command = command

    if ' --elementary' in command:
        print('Elementary execution')
        manifest_filename = ELEMENTARY_MANIFEST_FILENAME
        processed_command = processed_command.replace(' --elementary', '')
        dbt_project_file = ELEMENTARY_DBT_PROJECT_FILE
        return manifest_filename, dbt_project_file, processed_command

    dbt_project_file = DBT_PROJECT_FILE
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
    return manifest_filename, dbt_project_file, processed_command


def send_command(command):
    url = SERVER_URL + "dbt"

    manifest_filename, dbt_project_file, processed_command = extract_manifest_filename_from_command(command)

    manifest_str = load_file(manifest_filename)
    dbt_project_str = load_file(dbt_project_file)
    print(PACKAGES_FILE)
    packages_str = load_file(PACKAGES_FILE)

    elementary_bool = False
    if ' --elementary' in command:
        elementary_bool = True

    data = {
            "command": processed_command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str,
            "packages": packages_str,
            "elementary": elementary_bool
        }

    res = requests.post(url=url, json=data)
    return res.text


def get_run_status(uuid: str):
    url = SERVER_URL + "job/" + uuid
    res = requests.get(url=url)
    return res.text


def show_last_logs(uuid: str, last_timestamp_str: str):
    last_timestamp = datetime.strptime(last_timestamp_str, '%Y-%m-%dT%H:%M:%SZ')

    run_status_json = json.loads(get_run_status(uuid))
    logs = run_status_json["entries"]  # gets last logs from Firestore

    for log in logs:
        log_timestamp_str = log.split('\t')[0]
        log_timestamp = datetime.strptime(log_timestamp_str, '%Y-%m-%dT%H:%M:%SZ')

        if log_timestamp > last_timestamp:
            print(log)

    if log_timestamp > last_timestamp:
        last_timestamp_str = log_timestamp_str
    last_log = logs[-1]

    return last_log, log_timestamp_str


def get_report(uuid: str):
    url = SERVER_URL + "report/" + uuid
    res = requests.get(url=url)
    return res.text


def handle_command(command: str):

    start_all = timer()

    try:
        server_response = send_command(command)
        uuid = json.loads(server_response)["uuid"]
        print("uuid: "+uuid)
    except json.decoder.JSONDecodeError:
        print(server_response)
        return 0

    start_execution_job = timer()

    time.sleep(26)
    run_status = json.loads(get_run_status(uuid))["run_status"]
    now = datetime.now(timezone.utc)
    last_timestamp_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    k, timeout = 0, 30
    while run_status == "running" and k < timeout:
        time.sleep(1)
        run_status_json = json.loads(get_run_status(uuid))
        run_status = run_status_json["run_status"]
        last_log, last_timestamp_str = show_last_logs(uuid, last_timestamp_str)
        k += 1

    if run_status == "success":
        k, timeout = 0, 30
        while "END JOB" not in last_log and k < timeout:
            time.sleep(1)
            last_log, last_timestamp_str = show_last_logs(uuid, last_timestamp_str)
            k += 1

    end = timer()
    total_execution_time = end - start_all
    dbt_job_execution_time = end - start_execution_job

    print("total excution time\t", total_execution_time)
    print("dbt job excution time\t", dbt_job_execution_time)


def main():

    commands = [
        "list --elementary",
        "list",
        "--log-level info run --select vbak_dbt --elementary",
        "--debug list --manifest ../test-files/manifest.json"
    ]

    for command in commands:
        print()
        print(command)
        handle_command(command)


if __name__ == '__main__':
    main()
