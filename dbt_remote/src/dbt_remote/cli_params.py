import click

manifest = click.option(
    '--manifest',
    '-m',
    help='Manifest file path (ex: ./target/manifest.json), by default: none and the cli compiles one from current dbt project'
)

project_dir = click.option(
    '--project-dir',
    help='Which directory to look in for the dbt_project.yml file. Default is the current directory.'
)
dbt_project = click.option(
    '--dbt-project',
    help='dbt_project file, by default: dbt_project.yml'
)
profiles_dir = click.option(
    '--profiles-dir',
    help='profiles.yml file, by default: ./profiles.yml'
)
extra_packages = click.option(
    '--extra-packages',
    help='packages.yml file, by default none. Add this option is necessary to use external packages such as elementary.'
)
seeds_path = click.option(
    '--seeds-path',
    help='Path to seeds directory, this option is needed if you run `dbt-remote seed`. By default: seeds/'
)
server_url = click.option(
    '--server-url',
    help='Give dbt server url (ex: https://server.com). If not given, dbt-remote will look for a dbt server in GCP project\'s Cloud Run. In this case, you can give the location of the dbt server with --location.'
)
location = click.option(
    '--location',
    help='Location where the dbt server runs, ex: us-central1. Useful for server auto detection. If none is given, dbt-remote will look at all EU and US locations. /!\\ Location should be a Cloud region, not multi region.'
)
artifact_registry = click.option(
    '--artifact-registry',
    help='Your artifact registry. Ex: europe-west9-docker.pkg.dev/my-project/test-repository'
)
