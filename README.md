# dbtr

Run and schedule dbt jobs on a self hosted dbt remote server.

Dbtr is an for the dbt CLI that adds the `remote` subcommand. It lets you deploy a cloud server where you can schedule and execute dbt jobs.

This is a low-cost and practical alternative to dbt Cloud.

## Features

- Deploy a dbt server in one command locally, on GCP, or Azure. (AWS coming soon)
- Run dbt commands on a remote server rather than your machine
- Schedule dbt jobs
- Explore run logs, scheduled jobs, dbt docs, and Elementary reports in a web UI

## Installation

Install dbtr with the right extras depending on where your server is (or will be):
```shell
pip install 'dbtr[local]'
```

Available extras:
- `local`
- `google`
- `azure`


## Usage

The `dbtr` CLI is a drop-in replacement for `dbt` so you can just use it exactly as you would have otherwise ([`dbt` docs](https://docs.getdbt.com/reference/dbt-commands)):
```shell
dbtr debug

[...]

All checks passed!
```

Use the `remote` subcommand to deploy a dbt server locally:
```shell
dbtr remote deploy --cloud-provider local

[...]

Uvicorn running on http://0.0.0.0:8080 (Press CTRL+C to quit)
```

You can now submit jobs to the server rather than executing them locally:
```shell
dbtr remote debug --cloud-provider local --server-url http://localhost:8080
```

To see the full list of available commands that you can run remotely:
```shell
dbtr remote --help
```

## Moving to the cloud
For GCP, this will deploy the server on a Cloud Run instance:
```shell
dbtr remote deploy google
```
Other available options for deployment are `local`, `azure`, and soon, `aws`.


You can now run jobs on this instance:
```shell
dbtr remote debug --cloud-provider google --server-url https://my-dbt-server-abcdefghij-ew.a.run.app
```

## Scheduling jobs

If you want to schedule a dbt build every morning at 8 UTC:
```shell
dbtr remote build --cloud-provider google --server-url https://my-dbt-server-abcdefghij-ew.a.run.app --schedule-cron '0 8 * * *'
```

You can also schedule from a configuration file (`schedules.yaml`):
```yaml
job-1:
   command: dbtr remote build --server-url https://my-dbt-server-abcdefghij-ew.a.run.app --cloud-provider google
   schedule_cron: "0 8 * * 1"
   description: "Build all the models every Monday at 8 UTC"

job-2:
   command: dbtr remote build --select +customers+ --server-url https://my-dbt-server-abcdefghij-ew.a.run.app --cloud-provider google
   schedule_cron: "0 6 * * *"
   description: "Build all the customer models at 6 UTC"
```

Then deploy the schedules:
```shell
dbtr remote schedule set --schedule-file schedules.yaml --server-url https://my-dbt-server-abcdefghij-ew.a.run.app --cloud-provider google
```

## Frontend
Dbtr comes with a UI where you can take a look at logs, schedules, docs, etc...
```shell
dbtr remote frontend --server-url https://my-dbt-server-abcdefghij-ew.a.run.app  --cloud-provider google
```
