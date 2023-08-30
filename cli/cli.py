import requests
import os
from dotenv import load_dotenv
from pathlib import Path
import time
from datetime import datetime, timezone
import traceback
from typing import Dict, List

import click
from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from dbt.cli.main import dbtRunner

from classes import DbtResponse, DbtResponseRunStatus, DbtResponseLogs


dotenv_path = Path('.env')
load_dotenv(dotenv_path=dotenv_path)

SERVER_URL = os.getenv('SERVER_URL')+"/"


@click.command(context_settings=dict(ignore_unknown_options=True,),
               help="Enter dbt command, ex: dbt-remote run --select test")
@click.argument('user_command')
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option('--manifest', '-m', help='Manifest file path (ex: ./target/manifest.json), \
              by default: none and the cli compiles one from current dbt project')
@click.option('--dbt-project', default='./dbt_project.yml', help='dbt_project file, by default: ./dbt_project.yml')
@click.option('--extra-packages', help='packages.yml file, by default none')
@click.option('--seeds-path', default='./seeds/', help='Path to seeds directory. By default: seeds/')
@click.option('--elementary', is_flag=True, help='Set flag to run elementary report at the end of the job')
def cli(user_command: str, manifest: str | None, dbt_project: str, extra_packages: str | None, seeds_path: str,
        elementary: bool, args):

    dbt_cli_command(user_command, manifest, dbt_project, extra_packages, seeds_path, elementary, args)


def dbt_cli_command(user_command: str, manifest: str | None, dbt_project: str, extra_packages: str | None,
                    seeds_path: str, elementary: bool, args):

    args = ["\'"+arg+"\'" for arg in args]  # needed to handle case suche as --args '{key: value}'

    dbt_command = user_command + ' ' + ' '.join(args)
    click.echo(f'Command: dbt {dbt_command}')

    if manifest is None:
        compile_manifest()
        manifest = "./target/manifest.json"

    click.echo('Sending request to server. Waiting for job creation...')
    server_response = send_command(dbt_command, manifest, dbt_project, extra_packages, seeds_path, elementary)
    results = parse_server_response(server_response)

    if results is None:
        return 0

    if results.status_code != 202 or results.detail is not None:
        error_msg = results.detail
        click.echo(click.style("ERROR", fg="red") + '\t' + 'Status code: ' + str(results.status_code))
        click.echo(error_msg)
        return 0

    if results.uuid is not None:
        uuid = results.uuid
        click.echo(f"Job created with uuid: {uuid}")
        click.echo('Waiting for job execution...')

        stream_logs(uuid)


def compile_manifest():
    click.echo("Generating manifest.json")
    dbtRunner().invoke(["parse"])


def send_command(command: str, manifest: str, dbt_project: str, packages: str | None, seeds_path: str,
                 elementary: bool) -> requests.Response:
    url = SERVER_URL + "dbt"

    manifest_str = read_file(manifest)
    dbt_project_str = read_file(dbt_project)

    data = {
            "user_command": command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str
        }

    if 'seed' in command.split(' '):
        seeds_dict = get_seeds(seeds_path, command)
        data["seeds"] = seeds_dict

    if packages is not None:
        packages_str = read_file(packages)
        data["packages"] = packages_str

    if elementary:
        data["elementary"] = True

    res = requests.post(url=url, json=data)
    return res


def get_seeds(seeds_path: str, command: str) -> Dict[str, str]:

    seeds_dict: Dict[str, str] = dict()
    seed_files = get_files_from_dir(seeds_path)

    selected_seeds = get_selected_nodes(command)
    if len(selected_seeds) == 0:  # if no seed is selected, the command is executed on all seeds
        selected_seeds = get_all_seeds(seed_files)

    for seed_file in seed_files:
        if seed_file.replace(".csv", "") in selected_seeds:
            with open(seeds_path+seed_file, 'r') as f:
                seeds_dict['seeds/'+seed_file] = f.read()
    return seeds_dict


def get_selected_nodes(command: str) -> List[str]:

    args_list = split_arg_string(command)
    sub_command_click_context = args_to_context(args_list)
    selected_nodes = list(sub_command_click_context.params['select'])
    return selected_nodes


def get_all_seeds(seed_files: List[str]) -> List[str]:
    return [seed_file.replace('.csv', '') for seed_file in seed_files]


def parse_server_response(res: requests.Response) -> DbtResponse | None:
    try:
        results = DbtResponse.parse_raw(res.text)
    except Exception:
        traceback_str = traceback.format_exc()
        raise Exception("Error in parse_server: " + traceback_str)

    if dbtResponse_is_none(results):
        click.echo(click.style("ERROR", fg="red") + '\t' + 'Error in parsing: ')
        click.echo((res.text))
        return None

    else:
        results.status_code = res.status_code
        return results


def stream_logs(uuid: str):
    run_status = get_run_status(uuid).run_status

    while run_status == "running":
        time.sleep(1)
        run_status = get_run_status(uuid).run_status
        last_log = show_last_logs(uuid)

    if run_status == "success":
        while "END JOB" not in last_log:
            time.sleep(1)
            last_log = show_last_logs(uuid)
        show_last_logs(uuid)
    else:
        click.echo(click.style("ERROR", fg="red") + '\t' + "Job failed")


def get_run_status(uuid: str) -> DbtResponseRunStatus:
    url = SERVER_URL + "job/" + uuid
    res = requests.get(url=url)

    results = DbtResponseRunStatus.parse_raw(res.text)
    results.status_code = res.status_code
    return results


def get_last_logs(uuid: str) -> DbtResponseLogs:
    url = SERVER_URL + "job/" + uuid + '/last_logs'
    res = requests.get(url=url)

    results = DbtResponseLogs.parse_raw(res.text)
    results.status_code = res.status_code
    return results


def show_last_logs(uuid: str) -> str:

    logs = get_last_logs(uuid).run_logs

    for log in logs:
        show_log(log)
    if len(logs) > 0:
        return logs[-1]
    else:
        return ""


def show_log(log: str) -> ():

    parsed_log = parse_log(log)
    if parsed_log is None:
        return

    log_level, log_content = parsed_log

    if log_content == '':
        click.echo('')
        return

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


def parse_log(log: str) -> (tuple[str, str] | None):
    if log == '':
        click.echo('')
        return

    parsed_log = log.split('\t')

    if len(parsed_log) < 3:
        click.echo(click.style("ERROR", fg="red") + '\t' + "Error in log parsing:")
        click.echo(log)
        return

    log_level = parsed_log[1]
    log_content = '  '.join(parsed_log[2:])

    return log_level, log_content


def current_time() -> str:
    now = datetime.now(timezone.utc)
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt_string


def dbtResponse_is_none(results: DbtResponse):
    null_results = DbtResponse()
    return null_results == results


def read_file(filename) -> str:
    with open(filename, 'r') as f:
        file_str = f.read()
    return file_str


def get_files_from_dir(dir_path) -> List[str]:
    filename_list: List[str] = list()
    for file_path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, file_path)):
            filename_list.append(file_path)
    return filename_list
