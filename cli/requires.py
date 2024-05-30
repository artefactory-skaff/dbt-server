import functools
import io
import os
import zipfile
from pathlib import Path

import click
import humanize
from dbt.cli.main import cli as dbt_cli


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

        flags = ctx.obj["flags"]
        project_dir = Path(flags.project_dir)

        ignore = ["target/*", "dbt_packages/*", "logs/*"]
        dbt_remote_ignore_path = project_dir / ".dbtremoteignore"
        if dbt_remote_ignore_path.exists():
            with open(dbt_remote_ignore_path, "r") as f:
                dbt_remote_ignore = [line.strip() for line in f.readlines() if line.strip() and not line.startswith("#")]
            ignore.extend(dbt_remote_ignore)

        paths_to_ignore = set()
        for pattern in ignore:
            matches = [p for p in Path(project_dir).rglob(pattern)]
            paths_to_ignore.update(matches)

        all_files = [p for p in Path(project_dir).rglob('*') if p.is_file()]
        files_to_keep = [file for file in all_files if not any(file.match(pattern) for pattern in ignore)]
        print(f"Building the artifacts to send the dbt server with {len(files_to_keep)} files from {project_dir}")

        virtual_file = io.BytesIO()
        with zipfile.ZipFile(virtual_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in files_to_keep:
                zipf.write(file, file)

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
        runtime_config = {key: value for key, value in ctx.params.items() if key not in native_params}

        ctx.obj["dbt_runtime_config"] = native_params
        ctx.obj["server_runtime_config"] = runtime_config
        return func(*args, **kwargs)
    return wrapper
