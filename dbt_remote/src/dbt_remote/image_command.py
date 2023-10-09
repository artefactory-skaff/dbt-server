import os
from subprocess import check_output
import click


def build_image(location: str | None, artifact_registry: str | None, args):
    if len(args) < 1:
        raise click.ClickException(f"{click.style('ERROR', fg='red')}\tYou must provide an `image` command. See `dbt-remote image help`.")

    match(args[0]):
        case "help":
            help_image()
        case "submit":
            submit_image(location, artifact_registry)
        case _:
            raise click.ClickException(f"`dbt-remote image {args[0]}` command unknown. See `dbt-remote image help`.")


def help_image():
    click.echo("""
    Build and submit dbt-server image to your Artifact Registry.

    Commands:
        help: see this message. ex: dbt-remote image help
        submit: build and submit dbt-server image to Artifact Registry.
            ex: dbt-remote image submit --location europe-west9 --artifact-registry \\
                europe-west9-docker.pkg.dev/my-project/test-repository

""")


def submit_image(location: str | None, artifact_registry: str | None) -> ():

    if location is None or artifact_registry is None:
        raise click.ClickException(f"{click.style('ERROR', fg='red')}\tYou must provide a location and an artifact-registry.")

    site_packages_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))  # /Users/.../dbt_remote
    dbt_server_dir = site_packages_path + "/dbt_server"

    click.echo("Submitting dbt-server image...")
    click.echo(f"`gcloud builds submit {dbt_server_dir} --region={location} --tag {artifact_registry}/server-image`\n")

    check_output(f"gcloud builds submit {dbt_server_dir} --region={location} --tag {artifact_registry}/server-image", shell=True)

    click.echo(f"\ndbt-server image submitted to {click.style(f'{artifact_registry}/server-image', blink=True, bold=True)}")
