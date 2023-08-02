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
        print(m.span())
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


def main():
    dbt_project_file = DBT_PROJECT_FILE

    commands = [
        "dbt list",
        "dbt --log-level info run --select vbak_dbt",
        "dbt --debug list --manifest ../test-files/manifest.json"
    ]

    for command in commands[:1]:
        print()
        print(command)
        start_all = timer()
        uuid1 = json.loads(send_command(command, dbt_project_file))["uuid"]
        print("uuid: "+uuid1)
        start_execution_job = timer()

        time.sleep(16)
        run_status = json.loads(get_run_status(uuid1))["run_status"]
        buffer_logs = []
        while run_status == "running":
            time.sleep(3)
            run_status_json = json.loads(get_run_status(uuid1))
            run_status = run_status_json["run_status"]
            logs = run_status_json["entries"]
            for log in logs:
                if log not in buffer_logs:
                    print(log)
                    buffer_logs.append(log)
            print()
        print("run status", run_status)
        end = timer()
        print("total excution time", end - start_all)
        print("dbt job excution time", end - start_execution_job)
        print("\n Logs")
        time.sleep(6)
        entries = json.loads(get_run_status(uuid1))["entries"]
        for log in entries:
            print(log)


main()
