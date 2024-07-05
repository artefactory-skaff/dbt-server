import traceback as tb

import click


class DbtrException(click.ClickException):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class ServerLocked(DbtrException):
    def __init__(self, e):
        super().__init__(message=f"Run already in progress\n{e}\nYou can unlock the server by running 'dbtr remote unlock'")

class ServerUnlockFailed(DbtrException):
    pass

class MissingServerURL(DbtrException):
    pass

class UnsupportedCloudProvider(DbtrException):
    pass

class MissingLocation(DbtrException):
    pass

class MissingExtraPackage(DbtrException):
    pass

class ServerNotFound(DbtrException):
    pass

class Server400(DbtrException):
    pass

class Server500(DbtrException):
    pass

class AzureDeploymentFailed(DbtrException):
    pass

class MissingAzureParams(DbtrException):
    pass

class ServerConnectionError(DbtrException):
    pass


def handle_exceptions(e):
    if isinstance(e, DbtrException):
        click.echo(e)
    else:
        click.echo("".join(tb.format_exception(e)))
        click.echo(f"An unhandled exception occured, please report this issue with the above traceback to the dbtr team by creating an issue at https://github.com/artefactory-skaff/dbt-server/issues")
        exit(1)
