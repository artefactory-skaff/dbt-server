import requests
import os
import traceback
from typing import Dict, List, Any

import click
from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from dbt.cli.main import dbtRunner
from google.cloud import run_v2

from package.src.dbt_remote.dbt_server_detector import detect_dbt_server_uri
from package.src.dbt_remote.server_response_classes import DbtResponse
from package.src.dbt_remote.stream_logs import stream_logs


@click.command(context_settings=dict(ignore_unknown_options=True,),
               help="Run dbt commands on a dbt server.\n\n Commands: list, build, run, run-operation, compile, \
test, seed, snapshot.")
@click.argument('user_command')
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option('--manifest', '-m', help='Manifest file path (ex: ./target/manifest.json), \
by default: none and the cli compiles one from current dbt project')
@click.option('--project-dir', default='.', help='Which directory to look in for the dbt_project.yml file. Default \
is the current directory.')
@click.option('--dbt-project', default='dbt_project.yml', help='dbt_project file, by default: dbt_project.yml')
@click.option('--extra-packages', help='packages.yml file, by default none. Add this option is necessary to use\
external packages such as elementary.')
@click.option('--seeds-path', default='./seeds/', help='Path to seeds directory, this option is needed if you run `dbt\
-remote seed`. By default: seeds/')
@click.option('--server-url', help='Give dbt server url (ex: https://server.com). If not given, dbt-remote will look \
for a dbt server in GCP project\'s Cloud Run. In this case, you can give the location of the dbt server with --location\
.')
@click.option('--location', help='Location where the dbt server runs, ex: us-central1. Needed for server auto \
detection. If none is given, dbt-remote will look for the location given in the profiles.yml.')
@click.option('--elementary', is_flag=True, help='Set this flag to run elementary report at the end of the job')
def cli(user_command: str, project_dir: str, manifest: str | None, dbt_project: str, extra_packages: str | None,
        seeds_path: str, server_url: str | None, location: str | None, elementary: bool, args):

    dbt_command = assemble_dbt_command(user_command, args)
    click.echo(f'Command: dbt {dbt_command}')

    cloud_run_client = run_v2.ServicesClient()
    server_url = get_server_uri(dbt_command, project_dir, dbt_project, server_url, location, cloud_run_client)
    click.echo('dbt-server url: '+server_url)

    if manifest is None:
        compile_manifest(project_dir)
        manifest = "./target/manifest.json"

    click.echo('Sending request to server. Waiting for job creation...')
    server_response = send_command(server_url, dbt_command, project_dir, manifest, dbt_project, extra_packages,
                                   seeds_path, elementary)

    uuid, links = get_job_uuid_and_links(server_response)
    click.echo(f"Job created with uuid: {uuid}")
    click.echo(f"Job links: {links}")

    click.echo('Waiting for job execution...')
    stream_logs(links)


def assemble_dbt_command(user_command: str, args: Any) -> str:
    args = ["\'"+arg+"\'" for arg in args]  # needed to handle cases such as --args '{key: value}'
    dbt_command = user_command
    if args != [] and args is not None:
        dbt_command += ' ' + ' '.join(args)
    return dbt_command


def get_server_uri(dbt_command: str, project_dir: str, dbt_project: str, server_url: str | None,
                   location: str | None, cloud_run_client: run_v2.ServicesClient) -> str:
    if server_url is not None:
        server_url = server_url + "/"
    else:
        click.echo("Looking for dbt server available on Cloud Run...")
        server_url = detect_dbt_server_uri(project_dir, dbt_project, dbt_command, location, cloud_run_client) + "/"
    return server_url


def compile_manifest(project_dir: str):
    click.echo("Generating manifest.json")
    dbtRunner().invoke(["parse", "--project-dir", project_dir])


def send_command(server_url: str, command: str, project_dir: str, manifest: str, dbt_project: str, packages: str | None,
                 seeds_path: str, elementary: bool) -> requests.Response:
    url = server_url + "dbt"

    manifest_str = read_file(project_dir + '/' + manifest)
    dbt_project_str = read_file(project_dir + '/' + dbt_project)

    data = {
            "server_url": server_url,
            "user_command": command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str
        }

    if 'seed' in command.split(' ') or 'build' in command.split(' '):
        seeds_dict = get_selected_seeds_dict(project_dir + "/" + seeds_path, command)
        data["seeds"] = seeds_dict

    if packages is not None:
        packages_str = read_file(project_dir + "/" + packages)
        data["packages"] = packages_str

    if elementary:
        data["elementary"] = True

    res = requests.post(url=url, json=data)
    return res


def get_selected_seeds_dict(seeds_path: str, command: str) -> Dict[str, str]:

    seeds_dict: Dict[str, str] = dict()
    seed_files = get_filenames_from_dir(seeds_path)

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


def get_job_uuid_and_links(server_response: requests.Response) -> (str, Dict[str, str]):
    results = parse_server_response(server_response)

    if results.status_code != 202 or results.detail is not None:
        error_msg = results.detail
        click.echo(click.style("ERROR", fg="red") + '\t' + 'Status code: ' + str(results.status_code))
        raise click.ClickException(error_msg)

    if results.uuid is not None:
        uuid = results.uuid
        links = results.links
        return uuid, links


def parse_server_response(res: requests.Response) -> DbtResponse:
    try:
        results = DbtResponse.parse_raw(res.text)
    except Exception:
        traceback_str = traceback.format_exc()
        raise click.ClickException("Error in parse_server: " + traceback_str + "\n Original message: " + res.text)

    if dbtResponse_is_none(results):
        click.echo(click.style("ERROR", fg="red") + '\t' + 'Error in parsing: ')
        raise click.ClickException(res.text)

    else:
        results.status_code = res.status_code
        return results


def dbtResponse_is_none(results: DbtResponse):
    null_results = DbtResponse()
    return null_results == results


def read_file(filename) -> str:
    with open(filename, 'r') as f:
        file_str = f.read()
    return file_str


def get_filenames_from_dir(dir_path) -> List[str]:
    filename_list: List[str] = list()
    for file_path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, file_path)):
            filename_list.append(file_path)
    return filename_list


if __name__ == '__main__':
    cli()
