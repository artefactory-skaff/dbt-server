import click
from dbt_remote.src.cli_input import CliInput
from dbt_remote.src.dbt_server import DbtServer, DbtServerCommand


def run_and_echo(cli_input: CliInput) -> None:
    click.echo(click.style('Config:', blink=True, bold=True))
    for key, value in cli_input.__dict__.items():
        click.echo(f"   {key}: {value}")

    click.echo('\nSending request to server...')

    server = DbtServer(cli_input.server_url)
    command = DbtServerCommand.from_cli_config(cli_input)
    response = server.send_command(command)

    click.echo(click.style(response.message, blink=True, bold=True))

    if response.links is not None and "last_logs" in response.links:
        click.echo('Waiting for job execution...')
        logs = server.stream_logs(response.links["last_logs"])
        for log in logs:
            click.echo(log)
