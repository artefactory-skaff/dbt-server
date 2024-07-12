import functools
import io
import os
import zipfile
from pathlib import Path, PosixPath

import click
import humanize

from dbt_common.helper_types import WarnErrorOptions
from dbt.cli.main import cli as dbt_cli
from dbtr.common.exceptions import MissingServerURL
from dbtr.common.remote_server import DbtServer


def artifacts_archive(func):
    """Decorator that archives project artifacts.

    This decorator function wraps a command function to automatically archive the
    project's artifacts into a zip file, excluding files that match patterns in
    the `.dbtremoteignore` file or the default ignore list. The resulting zip
    file is stored in the command context for later use.

    Args:
        func: The command function to wrap.

    Returns:
        The wrapped command function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ctx = args[0]
        assert isinstance(ctx, click.Context)

        if ctx.params["dry_run"]:  # Skip building the artifacts archive in dry-run mode
            return func(*args, **kwargs)

        flags = ctx.obj["flags"]
        project_dir = Path(flags.project_dir)

        ignore = ["target/**", "logs/**", "venv/**"]
        dbt_remote_ignore_path = project_dir / ".dbtremoteignore"
        if dbt_remote_ignore_path.exists():
            with open(dbt_remote_ignore_path, "r") as f:
                dbt_remote_ignore = [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]
            ignore.extend(dbt_remote_ignore)

        paths_to_ignore = set()
        for pattern in ignore:
            matches = [p for p in Path(project_dir).rglob(pattern)]
            paths_to_ignore.update(matches)

        def should_ignore(path):
            return any(path.match(pattern) for pattern in ignore)

        files_to_keep = []
        for root, dirs, files in os.walk(project_dir):
            root_path = Path(root)
            if should_ignore(root_path.relative_to(project_dir)):
                dirs[:] = []  # Don't traverse into subdirectories
                continue
            for file in files:
                file_path = root_path / file
                if not should_ignore(file_path.relative_to(project_dir)):
                    files_to_keep.append(file_path)

        print(f"Building artifacts archive to send the dbt server with {len(files_to_keep)} files from {project_dir}")

        virtual_file = io.BytesIO()
        with zipfile.ZipFile(virtual_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in files_to_keep:
                zipf.write(file, file.relative_to(project_dir))

        virtual_file.seek(0, os.SEEK_END)
        archive_size = virtual_file.tell()
        print(f"Archive size: {humanize.naturalsize(archive_size)}")
        virtual_file.seek(0)

        ctx.obj['dbt_remote_artifacts'] = virtual_file

        # Uncomment this to save the zip file to disk for debugging
        # TODO: add argument to the decorator to toggle this. Ensure @artifacts_archive and @artifacts_archive(args) have the same behaviour
        # with open("dbt_remote_artifacts.zip", "wb") as f:
        #     f.write(virtual_file.read())
        # virtual_file.seek(0)

        return func(*args, **kwargs)

    return wrapper


def runtime_config(func):
    """
    Decorator that extracts runtime configuration from the Click context and stores it.

    This decorator is responsible for parsing the command-line arguments provided to the dbt command
    and separating them into two categories: native parameters that are inherent to dbt commands, and
    additional runtime configuration parameters that are specific to the server's execution context.

    The native dbt command parameters are stored in the 'dbt_runtime_config' key of the context object,
    while the additional server-specific runtime configuration parameters are stored in the
    'server_runtime_config' key of the context object.

    Args:
        func: The dbt command function to be decorated.

    Returns:
        The wrapped command function after runtime configuration has been extracted and stored.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ctx = args[0]
        assert isinstance(ctx, click.Context)

        native_params = {param.name for param in dbt_cli.commands[func.__name__].params}
        _dbt_runtime_config_args = {key: value for key, value in ctx.params.items() if key in native_params}
        dbt_runtime_config_args = {}
        for key, value in _dbt_runtime_config_args.items():
            if type(value) is PosixPath:
                dbt_runtime_config_args[key] = value.as_posix()
            elif type(value) is WarnErrorOptions:
                dbt_runtime_config_args[key] = value.to_dict()
            else:
                dbt_runtime_config_args[key] = value

        command = [func.__name__]
        if "macro" in ctx.params:
            command.append(ctx.params["macro"])

        server_runtime_config = {key: value for key, value in ctx.params.items() if key not in native_params}
        server_runtime_config["dbt_runtime_config"] = {"command": command, "flags": dbt_runtime_config_args}
        server_runtime_config["requester"] = os.getenv("USER") or os.getenv("USERNAME", "unknown")
        server_runtime_config["project"] = ctx.obj["project"].project_name

        ctx.obj["server_runtime_config"] = server_runtime_config
        return func(*args, **kwargs)

    return wrapper


def dbt_server(func):
    """
    Decorator that manages the dbt server interaction.

    This decorator is responsible for determining the server URL either from the provided command-line arguments or through automatic discovery if not provided. It supports Google Cloud as the cloud provider for now.

    If the server URL is not provided, it will attempt to discover a dbt server in the specified GCP project and location. If the project or location is not specified, it will use the default project from the gcloud configuration or search across all regions.

    Args:
        func: The dbt command function to be decorated.

    Returns:
        The wrapped command function after server setup has been handled.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        ctx = args[0]
        ctx.obj = ctx.obj or {}
        assert isinstance(ctx, click.Context)

        if ctx.params.get("cloud_provider") == "google":
            from dbtr.cli.cloud_providers import gcp

            if ctx.params.get("server_url") is None:
                click.echo("--server-url not set, performing server discovery...")
                if ctx.params.get("gcp_project") is None:
                    project_id = gcp.get_project_id()
                    click.echo(f"--gcp-project not set, defaulting to using the GCP project from your gcloud configuration: {project_id}")
                else:
                    project_id = ctx.params.get("gcp_project")

                server_url = gcp.find_dbt_server(ctx.params.get("gcp_location"), project_id)
            else:
                server_url = ctx.params["server_url"]
            server = DbtServer(server_url, token_generator=gcp.get_auth_token)

        elif ctx.params.get("cloud_provider") == "azure":
            from dbtr.cli.cloud_providers import az

            if ctx.params.get("server_url") is None:
                raise MissingServerURL("--server-url is required for Azure runs.")
            server = DbtServer(ctx.params["server_url"], token_generator=az.get_auth_token)

        elif ctx.params.get("cloud_provider") == "local":
            if ctx.params.get("server_url") is None:
                raise MissingServerURL("--server-url is required for local runs.")
            server = DbtServer(ctx.params["server_url"])


        ctx.obj["server"] = server

        return func(*args, **kwargs)

    return wrapper
