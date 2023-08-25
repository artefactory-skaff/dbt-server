import requests
import json
import os
from dotenv import load_dotenv
from pathlib import Path
import re
import time
from timeit import default_timer as timer
import sys

sys.path.insert(1, './lib')

dotenv_path = Path('.env.client')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"

if len(sys.argv) == 2:
    if sys.argv[1] == "--local":
        SERVER_URL = os.getenv('LOCAL_URL')+"/"
    if sys.argv[1] == "--dev":
        SERVER_URL = os.getenv('DEV_URL')+"/"

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
    packages_str = load_file(PACKAGES_FILE)

    elementary_bool = False
    if ' --elementary' in command:
        elementary_bool = True

    data = {
            "user_command": processed_command,
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


def get_last_logs(uuid: str):
    url = SERVER_URL + "job/" + uuid + '/last_logs'
    res = requests.get(url=url)
    return res.text


def show_last_logs(uuid: str):

    last_logs_json = json.loads(get_last_logs(uuid))
    logs = last_logs_json["run_logs"]

    for log in logs:
        print(log)
    if len(logs) > 0:
        return logs[-1]
    else:
        return ""


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
        print("Error from server")
        print(server_response)
        return 0
    except KeyError:
        error = json.loads(server_response)["detail"]
        print("Error from server")
        print(error)
        return 0

    start_execution_job = timer()

    time.sleep(26)
    run_status = json.loads(get_run_status(uuid))["run_status"]

    k, timeout = 0, 45
    last_log = show_last_logs(uuid)
    while run_status == "running" and k < timeout:
        time.sleep(1)
        run_status_json = json.loads(get_run_status(uuid))
        run_status = run_status_json["run_status"]
        last_log = show_last_logs(uuid)
        k += 1
    if k == timeout:
        print("Job timeout")

    if run_status == "success":
        k, timeout = 0, 45
        if "--elementary" in command:
            time.sleep(30)
            timeout = 120
        while "END JOB" not in last_log and k < timeout:
            time.sleep(1)
            last_log = show_last_logs(uuid)
            k += 1
        show_last_logs(uuid)
        if k == timeout:
            print("Logs or report timeout")
    else:
        print("job failed")

    end = timer()
    total_execution_time = end - start_all
    dbt_job_execution_time = end - start_execution_job

    print("total excution time\t", total_execution_time)
    print("dbt job excution time\t", dbt_job_execution_time)


def main():

    commands = [
        "--log-level debug list",
        "list --elementary",
        "--log-level info run --select vbak_dbt --elementary",
        "--log-level info run --select vbak_dbt",
        "--debug list --manifest ../test-files/manifest.json"
    ]

    for command in commands:
        print()
        print(command)
        handle_command(command)


if __name__ == '__main__':
    main()
