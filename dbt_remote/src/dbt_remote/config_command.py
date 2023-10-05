import click
import yaml
from typing import List, Optional
from dataclasses import dataclass


@dataclass
class CliConfig:
    """Config file for dbt-remote."""
    manifest: Optional[str] = None
    project_dir: Optional[str] = None
    dbt_project: Optional[str] = None
    profiles: Optional[str] = None
    extra_packages: Optional[str] = None
    seeds_path: Optional[str] = None
    server_url: Optional[str] = None
    location: Optional[str] = None


CONFIG_FILE = "dbt_remote.yml"
DEFAULT_CONFIG = {
    'manifest': None,
    'project_dir': '.',
    'dbt_project': 'dbt_project.yml',
    'profiles': 'profiles.yml',
    'extra_packages': None,
    'seeds_path': './seeds/',
    'server_url': None,
    'location': None,
}


@click.command(help="Configure dbt-remote.")
@click.argument('config_command')
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def config(config_command: str, args: List[str]):
    match(config_command):
        case "help":
            help()
        case "init":
            init()
        case "list":
            list()
        case "set":
            set(args)
        case "get":
            get(args)
        case "delete":
            delete(args)
        case _:
            raise click.ClickException("dbt-remote config command not recognized")


def help():
    click.echo(f"""
    Configure dbt-remote parameters. This config is stored in {CONFIG_FILE}.

    Commands:
        init: create (or reset) a default config file ({CONFIG_FILE}). ex: dbt-remote config init
        list: list current config. ex: dbt-remote config list
        set: set config parameters. ex: dbt-remote config set server_url=https://server.com
        get: get config parameters. ex: dbt-remote config get server_url
        delete: delete config parameters or replace by default value. ex: dbt-remote config delete server_url
        help: see this message. ex: dbt-remote config help

    Config parameters:
        manifest, project_dir, dbt_project, profiles, extra_packages, seeds_path, server_url, location
""")


def init():
    with open(CONFIG_FILE, 'w') as f:
        yaml.dump(DEFAULT_CONFIG, f)


def list():
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)
    click.echo(config)


def set(args: List[str]):
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)

    for arg in args:
        key, value = arg.split("=")[0], arg.split("=")[1]

        if value == 'None':
            value = None
        elif value in ["True", "true"]:
            value = True
        elif value in ["False", "false"]:
            value = False

        config[key] = value
        click.echo(f"Configured: {key}={value}")

    with open(CONFIG_FILE, 'w') as f:
        yaml.dump(config, f)


def get(args: List[str]):
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)

    for arg in args:
        if arg in config.keys():
            click.echo(f"{arg}={config[arg]}")
        else:
            click.echo(click.style('Error ', fg='red')+f"{arg} not found in config")


def delete(args: List[str]):
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)

    for arg in args:
        if arg in config.keys() and arg in DEFAULT_CONFIG.keys():
            config[arg] = DEFAULT_CONFIG[arg]  # reset to default
            click.echo(f"{arg} reset to default: {DEFAULT_CONFIG[arg]}")
        elif arg in config.keys() and arg in DEFAULT_CONFIG.keys():
            del config[arg]
            click.echo(f"{arg} deleted from config")
        else:
            click.echo(click.style('Error ', fg='red')+f"{arg} not found in config")

    with open(CONFIG_FILE, 'w') as f:
        yaml.dump(config, f)
