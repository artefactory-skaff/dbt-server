import click
import uvicorn


@click.group()
def cli():
    pass


@cli.command(
    context_settings=dict(
        ignore_unknown_options=True,
    ),
    help="Starts the dbt server.",
)
def start():
    from dbt_server.config import Settings

    settings = Settings()
    uvicorn.run(
        "dbt_server.server:app",
        port=settings.port,
        host="0.0.0.0",
        reload=True,
    )


@cli.group(
    context_settings=dict(
        ignore_unknown_options=True,
    ),
    help="Commands that manage dbt jobs.",
)
def job():
    pass


@job.command(
    context_settings=dict(
        ignore_unknown_options=True,
    ),
    help="Runs a dbt command from the dbt server.",
)
def run():
    from dbt_server.job import prepare_and_execute_job

    prepare_and_execute_job()
