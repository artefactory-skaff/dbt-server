import requests
import json
import os
from dotenv import load_dotenv
from pathlib import Path
import re

dotenv_path = Path('.env.client')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"
MANIFEST_FILENAME = os.getenv('MANIFEST_FILENAME')
DBT_PROJECT_FILE = os.getenv('DBT_PROJECT_FILE')
PROFILES_FILE = os.getenv('PROFILES_FILE')


def load_file(filename):
    f = open(filename, 'r')
    file_str = f.read()
    f.close()
    return file_str


def send_command(command, dbt_project_file, profiles_file):
    url = SERVER_URL + "dbt"
    processed_command = command

    # handle manifest
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

    # handle log settings
    m = re.search('--log-level (.+?)( |$)', command)
    debug_level = False
    if m:
        log_level = m.group(1)
        if log_level == "debug":
            debug_level = True
        begin, end = m.span()
        if processed_command[end:] != "":
            processed_command = processed_command[:begin] + processed_command[end:]
        else:
            processed_command = processed_command[:begin-1]
    else:
        if " --debug" in processed_command:
            debug_level = True
            processed_command.replace(" --debug", "")

    manifest_str = load_file(manifest_filename)
    dbt_project_str = load_file(dbt_project_file)
    profiles_str = load_file(profiles_file)

    data = {
            "command": processed_command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str,
            "profiles": profiles_str,
            "debug_level": debug_level
        }

    res = requests.post(url=url, json=data)
    print(res.status_code)
    return res.text


def main():
    dbt_project_file, profiles_file = DBT_PROJECT_FILE, PROFILES_FILE

    command1 = "list"
    command2 = "run --select vbak_dbt"
    command3 = "list --manifest ../test-files/manifest.json"

    print(command1)
    uuid1 = json.loads(
        send_command(command1, dbt_project_file, profiles_file)
        )["uuid"]
    print("uuid: "+uuid1)

    print(command2)
    uuid2 = json.loads(
        send_command(command2, dbt_project_file, profiles_file)
        )["uuid"]
    print("uuid: "+uuid2)

    print(command3)
    uuid3 = json.loads(
        send_command(command3, dbt_project_file, profiles_file)
        )["uuid"]
    print("uuid: "+uuid3)


main()
