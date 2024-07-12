# dbtr

A CLI extension for dbt that adds the `remote` subcommand. It lets you deploy a cloud server where you can schedule and execute dbt jobs.

Having a permament server enables running jobs on a schedule, which is not practical without paying for dbt Cloud otherwise. Provides a UI to view run logs, scheduled jobs, dbt docs, and Elementary reports.

TODO: demo video

## Installation

Install dbtr with the right extras depending on where your server is (or will be):
```shell
pip install dbtr[local]
```

Available extras:
- `local`
- `google`
- `azure`

AWS support is coming soon.

## Usage

The `dbtr` CLI extends `dbt` so you can just use it exactly as you would have otherwise ([`dbt` docs](https://docs.getdbt.com/reference/dbt-commands)):
```shell
dbtr debug

[...]

All checks passed!
```

Start a dbt server locally:
```shell
dbtr remote deploy --cloud-provider local

[...]

Uvicorn running on http://0.0.0.0:8080 (Press CTRL+C to quit)
```

`dbtr` adds the `remote` subcommand which sends jobs to the server rather than executing them locally:
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
