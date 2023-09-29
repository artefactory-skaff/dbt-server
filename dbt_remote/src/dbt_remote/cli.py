import requests
import os
import traceback
from typing import Dict, List, Any, Optional
import yaml
from dataclasses import dataclass

import click
from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from dbt.cli.main import dbtRunner
from google.cloud import run_v2

from dbt_remote.src.dbt_remote.dbt_server_detector import detect_dbt_server_uri
from dbt_remote.src.dbt_remote.server_response_classes import DbtResponse
from dbt_remote.src.dbt_remote.stream_logs import stream_logs
from dbt_remote.src.dbt_remote.config_command import config, CONFIG_FILE, DEFAULT_CONFIG
from dbt_remote.src.dbt_remote.authentication import get_auth_headers


@dataclass
class CliConfig:
    """Config file for dbt-remote."""
    manifest: Optional[str] = None
    project_dir: Optional[str] = None
    dbt_project: Optional[str] = None
    extra_packages: Optional[str] = None
    seeds_path: Optional[str] = None
    server_url: Optional[str] = None
    location: Optional[str] = None
    elementary: Optional[bool] = None
    creds_path: Optional[str] = None


help_msg = """
Run dbt commands on a dbt server.

Commands:

list, build, run, run-operation, compile, test, seed, snapshot. (dbt regular commands)

config: configure dbt-remote. See `dbt-remote config help` for more information.
"""


@click.command(context_settings=dict(ignore_unknown_options=True,), help=help_msg)
@click.argument('user_command')
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option('--credentials', help='Path to your service account json credentials file. Required to connect to your \
 dbt-server if authentication is enforced. Ex: "./creds.json"')
@click.option('--manifest', '-m', help='Manifest file path (ex: ./target/manifest.json), by default: none and the cli \
compiles one from current dbt project')
@click.option('--project-dir', help='Which directory to look in for the dbt_project.yml file. Default \
is the current directory.')
@click.option('--dbt-project', help='dbt_project file, by default: dbt_project.yml')
@click.option('--extra-packages', help='packages.yml file, by default none. Add this option is necessary to use\
external packages such as elementary.')
@click.option('--seeds-path', help='Path to seeds directory, this option is needed if you run `dbt-remote seed`. By \
default: seeds/')
@click.option('--server-url', help='Give dbt server url (ex: https://server.com). If not given, dbt-remote will look \
for a dbt server in GCP project\'s Cloud Run. In this case, you can give the location of the dbt server with --location\
.')
@click.option('--location', help='Location where the dbt server runs, ex: us-central1. Needed for server auto \
detection. If none is given, dbt-remote will look for the location given in the profiles.yml. \
/!\ Location should be a Cloud region, not multi region.')
@click.option('--elementary', is_flag=True, help='Set this flag to run elementary report at the end of the job')
@click.pass_context
def cli(ctx, user_command: str, credentials: str | None, project_dir: str | None, manifest: str | None, dbt_project:
        str | None, extra_packages: str | None, seeds_path: str | None, server_url: str | None, location: str | None,
        elementary: bool, args):

    if user_command == "config":
        if len(args) < 1:
            raise click.ClickException("You must provide a config command.")
        return ctx.invoke(config, config_command=args[0], args=args[1:])

    cli_config = CliConfig(
        manifest=manifest,
        project_dir=project_dir,
        dbt_project=dbt_project,
        extra_packages=extra_packages,
        seeds_path=seeds_path,
        server_url=server_url,
        location=location,
        elementary=elementary,
        creds_path=credentials
    )
    cli_config = load_config(cli_config)
    click.echo(click.style('Config: ', blink=True, bold=True)+str(cli_config.__dict__))

    dbt_command = assemble_dbt_command(user_command, args)
    click.echo(click.style('Command: ', blink=True, bold=True)+f'dbt {dbt_command}')

    check_if_dbt_project(cli_config)

    cloud_run_client = run_v2.ServicesClient()
    cli_config.server_url = get_server_uri(dbt_command, cli_config, cloud_run_client)
    click.echo(click.style('dbt-server url: ', blink=True, bold=True)+cli_config.server_url)
    auth_headers = get_auth_headers(cli_config.server_url, cli_config.creds_path)

    if cli_config.manifest is None:
        compile_manifest(cli_config.project_dir)
        cli_config.manifest = "./target/manifest.json"

    click.echo('\nSending request to server. Waiting for job creation...')
    server_response = send_command(dbt_command, cli_config, auth_headers)

    uuid, links = get_job_uuid_and_links(server_response)
    click.echo("Job created with uuid: " + click.style(uuid, blink=True, bold=True))
    display_links(links)

    click.echo('Waiting for job execution...')
    stream_logs(links, auth_headers)


def check_if_dbt_project(cli_config: CliConfig):
    files_to_check = dbt_files_to_check(cli_config)
    click.echo(click.style('Checking dbt files: ', blink=True, bold=True))
    click.echo(files_to_check)

    for filename in files_to_check.keys():
        path_to_file = files_to_check[filename]
        if not check_if_file_exist(path_to_file):
            click.echo(f"{filename} file not found.")
            raise click.ClickException("You are not in a dbt project directory or the dbt files are not in the \
expected place. Please check your dbt files or use the --project-dir option.")


def load_config(cli_config: CliConfig) -> CliConfig:
    if os.path.isfile(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = yaml.safe_load(f)
    else:
        config = DEFAULT_CONFIG

    cli_config_dict = cli_config.__dict__
    for key in cli_config_dict.keys():
        if cli_config_dict[key] is None or not cli_config_dict[key]:
            val = config[key]
            if val in ["True", "False"]:
                val = bool(val)
            cli_config_dict[key] = val
    return cli_config


def display_links(links: Dict[str, str]):
    click.echo("")
    click.echo("Following the job creation, you can access the following information using the links below:")
    for link in links:
        action, link_url = link.action_name, link.link
        click.echo(f"  - {action}: {link_url}")
    click.echo("")


def assemble_dbt_command(user_command: str, args: Any) -> str:
    args = ["\'"+arg+"\'" for arg in args]  # needed to handle cases such as --args '{key: value}'
    dbt_command = user_command
    if args != [] and args is not None:
        dbt_command += ' ' + ' '.join(args)
    return dbt_command


def get_server_uri(dbt_command: str, cli_config: CliConfig, cloud_run_client: run_v2.ServicesClient) -> str:
    if cli_config.server_url is not None:
        server_url = cli_config.server_url + "/"
    else:
        click.echo("\nNo server url given. Looking for dbt server available on Cloud Run...")
        server_url = detect_dbt_server_uri(cli_config.creds_path, cli_config.project_dir, cli_config.dbt_project,
                                           dbt_command, cli_config.location, cloud_run_client) + "/"
    return server_url


def compile_manifest(project_dir: str):
    click.echo("\nGenerating manifest.json")
    dbtRunner().invoke(["parse", "--project-dir", project_dir, "--quiet"])


def send_command(command: str, cli_config: CliConfig, auth_headers: Dict[str, str]) -> requests.Response:
    url = cli_config.server_url + "dbt"

    manifest_str = read_file(cli_config.project_dir + '/' + cli_config.manifest)
    dbt_project_str = read_file(cli_config.project_dir + '/' + cli_config.dbt_project)

    data = {
            "server_url": cli_config.server_url,
            "user_command": command,
            "manifest": manifest_str,
            "dbt_project": dbt_project_str
        }

    if 'seed' in command.split(' ') or 'build' in command.split(' '):
        seeds_dict = get_selected_seeds_dict(cli_config.project_dir + "/" + cli_config.seeds_path, command)
        data["seeds"] = seeds_dict

    if cli_config.extra_packages is not None:
        extra_packages_str = read_file(cli_config.project_dir + "/" + cli_config.extra_packages)
        data["packages"] = extra_packages_str

    if cli_config.elementary is True:
        data["elementary"] = True

    res = requests.post(url=url, headers=auth_headers, json=data)
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


def dbt_files_to_check(cli_config: CliConfig) -> Dict[str, str]:
    files_to_check = {}

    if cli_config.manifest is None:
        files_to_check['manifest'] = cli_config.project_dir + '/target/manifest.json'
    else:
        files_to_check['manifest'] = cli_config.project_dir + '/' + cli_config.manifest

    if cli_config.dbt_project is None:
        files_to_check['dbt_project'] = cli_config.project_dir + 'dbt_project.yml'
    else:
        files_to_check['dbt_project'] = cli_config.project_dir + '/' + cli_config.dbt_project

    return files_to_check


def check_if_file_exist(path_to_file: str) -> bool:
    if os.path.isfile(path_to_file):
        return True
    else:
        return False


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
