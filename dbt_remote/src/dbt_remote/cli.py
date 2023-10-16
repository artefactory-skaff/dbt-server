import base64
import requests
import os
from typing import Tuple, Dict, List, Any
import yaml
from pathlib import Path

import click
from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest
from dbt.parser.manifest import write_manifest

from dbt_remote.src.dbt_remote import cli_params as p
from dbt_remote.src.dbt_remote.dbt_server_detector import detect_dbt_server_uri
from dbt_remote.src.dbt_remote.server_response_classes import DbtResponse
from dbt_remote.src.dbt_remote.stream_logs import stream_logs
from dbt_remote.src.dbt_remote.config_command import CliConfig, config, CONFIG_FILE, init
from dbt_remote.src.dbt_remote.authentication import get_auth_session
from dbt_remote.src.dbt_remote.image_command import build_image


help_msg = """
Run dbt commands on a dbt server.

Commands:

list, build, run, run-operation, compile, test, seed, snapshot. (dbt regular commands)

config: configure dbt-remote. See `dbt-remote config help` for more information.

image: build and submit dbt-server image to your Artifact Registry. See `dbt-remote image help` for more information.
"""

@click.command(context_settings=dict(ignore_unknown_options=True,), help=help_msg)
@click.argument('user_command')
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@p.manifest
@p.project_dir
@p.dbt_project
@p.profiles_dir
@p.extra_packages
@p.seeds_path
@p.server_url
@p.location
@p.artifact_registry
@click.pass_context
def cli(
    ctx,
    user_command: str,
    project_dir: str | None,
    manifest: str | None,
    dbt_project: str | None,
    profiles_dir: str | None,
    extra_packages: str | None,
    seeds_path: str | None,
    server_url: str | None,
    location: str | None,
    artifact_registry: str | None,
    args
):

    if user_command == "config":
        if len(args) < 1:
            raise click.ClickException(f"{click.style('ERROR', fg='red')}\tYou must provide a config command.")
        return ctx.invoke(config, config_command=args[0], args=args[1:])

    if user_command == "image":  # expected: dbt-remote image submit
        return build_image(location, artifact_registry, args)

    dbt_command = assemble_dbt_command(user_command, args)
    click.echo(f"{click.style('Command:', blink=True, bold=True)} dbt {dbt_command}")

    cli_config = CliConfig(
        manifest=manifest,
        project_dir=project_dir,
        dbt_project=dbt_project,
        profiles=profiles_dir,
        extra_packages=extra_packages,
        seeds_path=seeds_path,
        server_url=server_url,
        location=location,
    )
    cli_config = load_config_file(cli_config)
    cli_config.profiles = search_profiles_file(dbt_command, cli_config)

    if cli_config.server_url is not None:
        click.echo(f"{click.style('dbt-server url:', blink=True, bold=True)} {cli_config.server_url}")
    else:
        click.echo(f"{click.style('dbt-server url:', blink=True, bold=True)} unknown.")

    click.echo(click.style('Config:', blink=True, bold=True))
    for key, value in cli_config.__dict__.items():
        click.echo(f"   {key}: {value}")

    check_if_dbt_project(cli_config)

    cli_config.server_url = get_server_uri(cli_config)
    auth_session = get_auth_session()

    if cli_config.manifest is None:
        compile_and_store_manifest(cli_config)
        cli_config.manifest = cli_config.project_dir+"/target/manifest.json"

    click.echo('\nSending request to server. Waiting for job creation...')
    server_response = send_command(dbt_command, cli_config, auth_session)

    uuid, links = get_job_uuid_and_links(server_response)
    click.echo(f"Job created with uuid: {click.style(uuid, blink=True, bold=True)}")
    display_links(links)

    click.echo('Waiting for job execution...')
    stream_logs(links, auth_session)


def load_config_file(cli_config: CliConfig) -> CliConfig:
    if not os.path.isfile(CONFIG_FILE):
        click.echo("No config file found. Creating config...")
        init()

    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)

    cli_config_dict = cli_config.__dict__
    for key in cli_config_dict.keys():
        if cli_config_dict[key] is None:
            cli_config_dict[key] = config[key]

    for key in cli_config_dict.keys():
        if cli_config_dict[key] is not None and key in ['manifest', 'dbt_project', 'extra_packages', 'seeds_path']:
            cli_config_dict[key] = cli_config_dict['project_dir']+"/"+cli_config_dict[key]

    return cli_config


def search_profiles_file(command: str, cli_config: CliConfig) -> str:
    if "--profiles-dir" in command:
        profiles_dir = get_profiles_dir_from_command(command)
        if os.path.isfile(profiles_dir + "/profiles.yml"):
            return profiles_dir + "/profiles.yml"
        else:
            raise click.ClickException(f"{click.style('ERROR', fg='red')}\tIncorrect --profiles-dir value given.")
    elif "DBT_PROFILES_DIR" in os.environ:
        return os.environ["DBT_PROFILES_DIR"]
    elif cli_config.profiles is not None and os.path.isfile(cli_config.profiles):
        return cli_config.profiles
    elif os.path.isfile(str(Path.home())+"/.dbt/profiles.yml"):
        return str(Path.home())+"/.dbt/profiles.yml"
    else:
        raise click.ClickException(f"{click.style('ERROR', fg='red')}\tYou must provide a profiles file.")


def get_profiles_dir_from_command(command: str) -> str:
    args_list = split_arg_string(command)
    sub_command_context = args_to_context(args_list)
    sub_command_params_dict = sub_command_context.params
    return sub_command_params_dict['profiles_dir']


def check_if_dbt_project(cli_config: CliConfig):
    files_to_check = dbt_files_to_check(cli_config)

    for filename in files_to_check.keys():
        path_to_file = files_to_check[filename]
        if not os.path.isfile(path_to_file):
            if filename == 'manifest':  # not mandatory because it can be generated
                click.echo(f"{click.style('WARNING', fg='red')} {filename} not found.")
            else:
                click.echo(f"{click.style('ERROR', fg='red')} {filename} not found.\n")
                raise click.ClickException(f"You are not in a dbt project directory or the dbt files are not in the \
expected place. {click.style('Please make sure that you are in a dbt project directory or create one using `dbt init --profiles-dir .`', blink=True, bold=True)}")


def display_links(links: Dict[str, str]):
    click.echo("")
    click.echo("Following the job creation, you can access the following information using the links below:")
    for action, link_url in links.items():
        click.echo(f"  - {action}: {link_url}")
    click.echo("")


def assemble_dbt_command(user_command: str, args: Any) -> str:
    args = ["\'"+arg+"\'" for arg in args]  # needed to handle cases such as --args '{key: value}'
    dbt_command = user_command
    if args != [] and args is not None:
        dbt_command += ' ' + ' '.join(args)
    return dbt_command


def get_server_uri(cli_config: CliConfig) -> str:
    if cli_config.server_url is not None:
        server_url = cli_config.server_url + "/"
    else:
        server_url = detect_dbt_server_uri(cli_config) + "/"
    return server_url


def compile_and_store_manifest(cli_config: CliConfig) -> ():
    if not os.path.isdir(cli_config.project_dir+'/target'):
        Path(cli_config.project_dir+'/target').mkdir(parents=True, exist_ok=True)
    manifest = compile_manifest(cli_config.project_dir)
    write_manifest(manifest, cli_config.project_dir + '/target')


def compile_manifest(project_dir: str) -> Manifest:
    click.echo("\nGenerating manifest.json")
    res: dbtRunnerResult = dbtRunner().invoke(["parse", "--project-dir", project_dir])
    manifest: Manifest = res.result
    return manifest


def send_command(command: str, cli_config: CliConfig, auth_session: requests.Session) -> requests.Response:
    url = cli_config.server_url + "dbt"

    manifest_str = read_file_as_b64(cli_config.manifest)
    dbt_project_str = read_file_as_b64(cli_config.dbt_project)
    profiles_str = read_file_as_b64(cli_config.profiles)

    data = {
        "server_url": cli_config.server_url,
        "user_command": command,
        "manifest": manifest_str,
        "dbt_project": dbt_project_str,
        "profiles": profiles_str,
    }

    if 'seed' in command.split(' ') or 'build' in command.split(' '):
        seeds_dict = get_selected_seeds_dict(cli_config.seeds_path, command)
        data["seeds"] = seeds_dict

    if cli_config.extra_packages is not None:
        extra_packages_str = read_file_as_b64(cli_config.extra_packages)
        data["packages"] = extra_packages_str

    res = auth_session.post(url=url, json=data)
    return res


def get_selected_seeds_dict(seeds_path: str, command: str) -> Dict[str, str]:

    seeds_dict: Dict[str, str] = dict()
    seed_files = get_filenames_from_dir(seeds_path)

    selected_seeds = get_selected_nodes(command)
    if len(selected_seeds) == 0:  # if no seed is selected, the command is executed on all seeds
        selected_seeds = get_all_seeds(seed_files)

    for seed_file in seed_files:
        if seed_file.replace(".csv", "") in selected_seeds:
            seeds_dict['seeds/'+seed_file] = read_file_as_b64(seeds_path+seed_file)
    return seeds_dict


def get_selected_nodes(command: str) -> List[str]:

    args_list = split_arg_string(command)
    sub_command_click_context = args_to_context(args_list)
    selected_nodes = list(sub_command_click_context.params['select'])
    return selected_nodes


def get_all_seeds(seed_files: List[str]) -> List[str]:
    return [seed_file.replace('.csv', '') for seed_file in seed_files]


def get_job_uuid_and_links(server_response: requests.Response) -> Tuple[str, Dict[str, str]]:
    results = parse_server_response(server_response)

    if results.status_code != 202 or results.detail is not None:
        error_msg = results.detail
        click.echo(f"{click.style('ERROR', fg='red')}\tStatus code: {str(results.status_code)}")
        raise click.ClickException(error_msg)

    if results.uuid is not None:
        uuid = results.uuid
        links = results.links
        return uuid, links


def parse_server_response(res: requests.Response) -> DbtResponse:
    try:
        results = DbtResponse.parse_raw(res.text)
    except Exception:
        raise click.ClickException(f"{click.style('ERROR', fg='red')} in while parsing server response (parse_server_response).\
\nReceived message: {res.text}")

    if dbtResponse_is_none(results):
        click.echo(f"{click.style('ERROR', fg='red')}\tError in parsing:")
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
        files_to_check['manifest'] = cli_config.manifest

    if cli_config.dbt_project is None:
        files_to_check['dbt_project'] = cli_config.project_dir + 'dbt_project.yml'
    else:
        files_to_check['dbt_project'] = cli_config.dbt_project

    if cli_config.profiles is None:
        files_to_check['profiles'] = 'profiles.yml'
    else:
        files_to_check['profiles'] = cli_config.profiles

    return files_to_check

def read_file_as_b64(filename) -> str:
    with open(filename, 'r') as f:
        file_str = f.read()
    file_bytes = base64.b64encode(bytes(file_str, 'ascii'))
    file_str = file_bytes.decode('ascii')
    return file_str


def get_filenames_from_dir(dir_path) -> List[str]:
    filename_list: List[str] = list()
    for file_path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path, file_path)):
            filename_list.append(file_path)
    return filename_list


if __name__ == '__main__':
    cli()
