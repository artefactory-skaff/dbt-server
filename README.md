# dbtr

A CLI extension for dbt that adds the `remote` subcommand. It lets you deploy a cloud server where you can schedule and execute dbt jobs.

Having a permament server enable running jobs on a schedule, which is not practical without paying for dbt Cloud otherwise. The server will also prevent concurrent jobs to avoid potentially breaking your warehouse. Soon we will provide Elementary integration to help monitor your runs.

TODO: demo video

## Installation

Install dbtr with the right extras depending on where your server is or will be:
```shell
pip install dbtr[local]
```

Available extras:
- `local`
- `google`

Azure and AWS support are coming soon.

## Usage

The `dbtr` CLI extends `dbt` so you can just use it exactly as you would have otherwise ([`dbt` docs](https://docs.getdbt.com/reference/dbt-commands)):
```shell
dbtr debug

[...]

All checks passed!
```

`dbtr` adds the `remote` subcommand which runs on a remote dbt server:
```shell
dbtr remote debug --cloud-provider local --server-url http://localhost:8080
```

Right now you do not have a dbt server running so the above command will not work, let's run one locally:
```shell
dbtr remote deploy --cloud-provider local

[...]

Uvicorn running on http://0.0.0.0:8080 (Press CTRL+C to quit)
```

To see the full list of available commands that you can run remotely:
```shell
dbtr remote --help
```

## Moving to the cloud
For GCP, this will deploy the server on a Cloud Run instance:
```shell
dbtr remote deploy \
    --cloud-provider google \
    --service my-dbt-server \
    --adapter dbt-bigquery
```

TODO: list required permissions

You can now run jobs on this instance:
```shell
dbtr remote debug --cloud-provider google --server-url https://my-dbt-server-abcdefghij-ew.a.run.app
```
