import requests
from typing import Dict, List
import json
import yaml

import click
from click.parser import split_arg_string
from dbt.cli.flags import args_to_context
from google.cloud import run_v2


def detect_dbt_server_uri(project_dir: str, dbt_project: str, command: str, location: str | None) -> str:

    selected_target, selected_profile = get_selected_target_and_profile(command)

    if selected_profile is None:
        selected_profile = read_yml_file(project_dir + '/' + dbt_project)['profile']

    profiles_dict = read_yml_file(project_dir + '/profiles.yml')
    project_id = get_metadata_from_profiles_dict(profiles_dict, selected_profile, selected_target, 'project')
    if location is None:
        location = get_metadata_from_profiles_dict(profiles_dict, selected_profile, selected_target, 'location')

    cloud_run_services = get_cloud_run_service_list(project_id, location)
    for service in cloud_run_services:
        if check_if_server_is_dbt_server(service):
            click.echo('Using Cloud Run `' + service.name + '` as dbt server')
            click.echo('uri: ' + service.uri)
            return service.uri

    click.echo(click.style("ERROR", fg="red"))
    raise click.ClickException('No dbt server found in Cloud Run')


def get_selected_target_and_profile(command: str) -> (str | None, str | None):
    try:
        args_list = split_arg_string(command)
        sub_command_click_context = args_to_context(args_list)
        target = sub_command_click_context.params['target']
        profile = sub_command_click_context.params['profile']
        return target, profile
    except Exception:
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException("dbt command failed: " + command)


def get_metadata_from_profiles_dict(profiles_dict: Dict[str, str], selected_profile: str, selected_target: str | None,
                                    metadata_name: str) -> tuple[str, str]:

    if selected_profile in profiles_dict.keys():

        if selected_target is None:
            selected_target = deduce_target_from_profiles(profiles_dict[selected_profile])

        profile_config = profiles_dict[selected_profile]['outputs']
        if selected_target in profile_config.keys():

            if metadata_name not in profile_config[selected_target].keys():
                click.echo(click.style("ERROR", fg="red"))
                raise click.ClickException(metadata_name + ' not found for profile '+selected_profile+' and \
                                           target '+selected_target)
            metadata = profile_config[selected_target][metadata_name]
            return metadata

        else:
            click.echo(click.style("ERROR", fg="red"))
            raise click.ClickException('Target: "'+selected_target+'" not found for profile '+selected_profile)
    else:
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException('Profile: ' + selected_profile + ' not found in profiles.yml')


def deduce_target_from_profiles(selected_profile_dict):
    if 'target' in selected_profile_dict.keys():
        return selected_profile_dict['target']
    elif 'outputs' in selected_profile_dict.keys():
        if 'default' in selected_profile_dict['outputs'].keys():
            return 'default'
        else:
            print(list(selected_profile_dict['outputs'].keys()))
            return list(selected_profile_dict['outputs'].keys())[0]
    else:
        click.echo(click.style("ERROR", fg="red"))
        raise click.ClickException('Coulnd\'t find target in for given profile')


def get_cloud_run_service_list(project_id: str, location: str) -> List[run_v2.types.service.Service]:
    click.echo(f"Cloud Run location: {location}")

    client = run_v2.ServicesClient()

    parent_value = f"projects/{project_id}/locations/{location}"
    request = run_v2.ListServicesRequest(
        parent=parent_value,
    )

    service_list = client.list_services(request=request)
    return service_list


def check_if_server_is_dbt_server(service: run_v2.types.service.Service) -> bool:
    url = service.uri + '/check'
    try:
        res = requests.get(url)
    except Exception:
        return False
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


def read_yml_file(filename: str) -> Dict[str, str]:
    with open(filename, 'r') as stream:
        try:
            d = yaml.safe_load(stream)
            return d
        except yaml.YAMLError as e:
            click.echo(click.style("ERROR", fg="red") + '\t' + "Incorrect profiles YAML file")
            raise click.ClickException(e)
