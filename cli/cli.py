import requests
import os
import time
from datetime import datetime, timezone
import traceback
from typing import Dict, List
import json
import yaml

from google.cloud import run_v2
import click
from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from dbt.cli.main import dbtRunner

from classes import DbtResponse, DbtResponseRunStatus, DbtResponseLogs


@click.command(context_settings=dict(ignore_unknown_options=True,),
               help="Enter dbt command, ex: dbt-remote run --select test")
@click.argument('user_command')
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option('--manifest', '-m', help='Manifest file path (ex: ./target/manifest.json), \
              by default: none and the cli compiles one from current dbt project')
@click.option('--project-dir', default='.', help='Which directory to look in for the dbt_project.yml file. Default \
              is the current directory.')
@click.option('--dbt-project', default='dbt_project.yml', help='dbt_project file, by default: dbt_project.yml')
@click.option('--extra-packages', help='packages.yml file, by default none')
@click.option('--seeds-path', default='./seeds/', help='Path to seeds directory. By default: seeds/')
@click.option('--server-url', help='Give dbt server url (ex: https://server.com). If not given, dbt-remote will look \
              for a dbt server in GCP project\'s Cloud Run')
@click.option('--elementary', is_flag=True, help='Set flag to run elementary report at the end of the job')
def cli(user_command: str, project_dir: str, manifest: str | None, dbt_project: str, extra_packages: str | None,
        seeds_path: str, server_url: str | None, elementary: bool, args):

    args = ["\'"+arg+"\'" for arg in args]  # needed to handle case suche as --args '{key: value}'
    dbt_command = user_command + ' ' + ' '.join(args)
    click.echo(f'Command: dbt {dbt_command}')

    global SERVER_URL
    if server_url is None:
        click.echo("Looking for dbt server available on Cloud Run...")
        detected_server_url = get_dbt_server_uri(project_dir, dbt_project, dbt_command)
        if detected_server_url is None:
            click.echo(click.style("ERROR", fg="red") + '\t' + 'No dbt server found in Cloud Run')
            return
        else:
            SERVER_URL = detected_server_url + "/"
    else:
        SERVER_URL = server_url + "/"
    click.echo('dbt server url: '+SERVER_URL)

    dbt_cli_command(dbt_command, project_dir, manifest, dbt_project, extra_packages, seeds_path, elementary)


def get_dbt_server_uri(project_dir: str, dbt_project: str, command: str) -> str | None:

    target, profile = get_selected_target_and_profile(command)
    project_metadata = get_projectid_and_location_from_profiles(project_dir, dbt_project, target, profile)
    if project_metadata is not None:
        global PROJECT_ID
        global LOCATION
        PROJECT_ID, LOCATION = project_metadata
    else:
        return

    cloud_run_services = cloud_run_service_list()

    for service in cloud_run_services:
        if check_if_server_is_dbt_server(service):
            click.echo('Using Cloud Run `' + service.name + '` as dbt server')
            click.echo('uri: ' + service.uri)
            return service.uri

    return


def get_projectid_and_location_from_profiles(project_dir: str, dbt_project: str, selected_target: str | None,
                                             selected_profile: str | None) -> (tuple[str, str] | None):

    if selected_profile is None:
        selected_profile = read_yml_file(project_dir + '/' + dbt_project)['profile']

    profiles_dict = read_yml_file(project_dir + '/profiles.yml')

    if selected_profile in profiles_dict.keys():

        if selected_target is None:
            selected_target = deduce_target_from_profiles(profiles_dict[selected_profile])

        profile_config = profiles_dict[selected_profile]['outputs']
        if selected_target in profile_config.keys():
            location = profile_config[selected_target]['location']
            project_id = profile_config[selected_target]['project']
            return project_id, location
        else:
            click.echo(click.style("ERROR", fg="red")+'\tTarget: "'+selected_target+'" \
                       not found for profile '+selected_profile)
    else:
        click.echo(click.style("ERROR", fg="red") + '\tProfile: ' + selected_profile + ' not found in profiles.yml')
    return


def get_selected_target_and_profile(command: str) -> (str | None, str | None):

    args_list = split_arg_string(command)
    sub_command_click_context = args_to_context(args_list)
    target = sub_command_click_context.params['target']
    profile = sub_command_click_context.params['profile']
    return target, profile


def deduce_target_from_profiles(selected_profile_dict):
    if 'target' in selected_profile_dict.keys():
        return selected_profile_dict['target']
    elif 'default' in selected_profile_dict['outputs'].keys():
        return 'default'
    else:
        return selected_profile_dict['outputs'].keys()[0]


def cloud_run_service_list() -> List[run_v2.types.service.Service]:

    client = run_v2.ServicesClient()

    parent_value = f"projects/{PROJECT_ID}/locations/{LOCATION}"
    request = run_v2.ListServicesRequest(
        parent=parent_value,
    )

    service_list = client.list_services(request=request)
    return service_list


def check_if_server_is_dbt_server(service: run_v2.types.service.Service) -> bool:
    url = service.uri + '/check'
    res = requests.get(url)
    if res.status_code == 200:
        try:
            check = json.loads(res.text)['response']
            if 'Running dbt-server on port' in check:
                return True
            else:
                return False
        except Exception:
            return False
    else:
        return False


def dbt_cli_command(dbt_command: str, project_dir: str, manifest: str | None, dbt_project: str,
                    extra_packages: str | None, seeds_path: str, elementary: bool):

    if manifest is None:
        compile_manifest(project_dir)
        manifest = "./target/manifest.json"

    click.echo('Sending request to server. Waiting for job creation...')
    server_response = send_command(dbt_command, project_dir, manifest, dbt_project, extra_packages,
                                   seeds_path, elementary)
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


def compile_manifest(project_dir: str):
    click.echo("Generating manifest.json")
    dbtRunner().invoke(["parse", "--project-dir", project_dir])


def send_command(command: str, project_dir: str, manifest: str, dbt_project: str, packages: str | None, seeds_path: str,
                 elementary: bool) -> requests.Response:
    url = SERVER_URL + "dbt"

    manifest_str = read_file(project_dir + '/' + manifest)
    dbt_project_str = read_file(project_dir + '/' + dbt_project)

    data = {
            "user_command": command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str
        }

    if 'seed' in command.split(' '):
        seeds_dict = get_seeds(project_dir + "/" + seeds_path, command)
        data["seeds"] = seeds_dict

    if packages is not None:
        packages_str = read_file(project_dir + "/" + packages)
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
        stop = show_last_logs(uuid)

    if run_status == "success":
        while not stop:
            time.sleep(1)
            stop = show_last_logs(uuid)
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


def show_last_logs(uuid: str) -> bool:

    logs = get_last_logs(uuid).run_logs

    for log in logs:
        show_log(log)
        if "END JOB" in log:
            return True
    return False


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


def read_yml_file(filename: str) -> Dict[str, str]:
    with open(filename, 'r') as stream:
        try:
            d = yaml.safe_load(stream)
            return d
        except yaml.YAMLError as e:
            click.echo(click.style("ERROR", fg="red") + '\t' + "Incorrect profiles YAML file")
            click.echo(e)


def get_files_from_dir(dir_path) -> List[str]:
    filename_list: List[str] = list()
    for file_path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, file_path)):
            filename_list.append(file_path)
    return filename_list
