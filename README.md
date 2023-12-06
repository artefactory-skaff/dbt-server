# dbt-remote project

This package provides
- `dbt-remote`, a drop-in replacement for the dbt CLI for analytics engineers.
- `dbt-server`, a Cloud Run API that will need to be deployed to perform the remote dbt runs (for data platform engineers: [How to deploy?](./dbt_server/README.md)).

<center><img src="./docs/images/intro-README.png" width="100%"></center>


# dbt-remote

This CLI runs dbt commands remotely on GCP-hosted server.

## Requirements

- An initialized dbt core project. [(dbt core quickstart)](https://docs.getdbt.com/quickstarts/manual-install?step=1)
- The gcloud CLI. [(gcloud install guide)](https://cloud.google.com/sdk/docs/install)
- A deployed dbt-server. [(dbt-server deployment guide)](./dbt_server/README.md)

## Installation

```sh
python3 -m pip install --extra-index-url https://test.pypi.org/simple/ gcp-dbt-remote --no-cache-dir
```

Refresh your shell/venv to enable the cli:
```sh
source venv/bin/activate
```
OR
```sh
conda activate
```

### Setup your GCP project, gcloud CLI, and default credentials
```sh
export PROJECT_ID=<your-gcp-project-id>
```

```sh
gcloud auth login
gcloud auth application-default login
gcloud config set project $PROJECT_ID
```

### Make sure your dbt project is properly setup locally.
```sh
dbt debug
```
```sh
> All checks passed!
```


### Test the CLI installation (requires you to have deployed the `dbt-server`)
```sh
dbt-remote debug
```
```sh
> INFO    [dbt] All checks passed!
> INFO    [job] Command successfully executed
```

### Use `dbt-remote` just like you would do with the regular dbt CLI
```sh
dbt-remote run
```

```sh
dbt-remote run --select my_first_dbt_model
```

View all `dbt-remote` options
```sh
dbt-remote --help
```

### Schedule dbt runs

Use the --schedule option and a cron expression to schedule a run. [Help with cron expressions.](https://crontab.guru/#0_*_*_*_*)
```sh
dbt-remote run --schedule '0 8 * * *'
```
```sh
[...]
Sending request to server...
Job run scheduled at 0 8 * * * (At 08:00 AM) with uuid: e11f1085-8ad9-4dcd-b09f-d8a8369075b9
```
This will create a [cloud scheduler](https://console.cloud.google.com/cloudscheduler) that will call the dbt-server at the configured time.

You can also declare a run schedule using a config file. Be aware that this is authoritative and will replace any previously scheduled runs not in the file. It will also update the manifest for the scheduled runs.
```yaml
# schedules.yaml
schedule-1:
   command: build --select my_first_dbt_model --project-dir=tests/dbt_project
   schedule: "2 3 4 5 6"

schedule-3:
   command: build --select my_first_dbt_model --project-dir=tests/dbt_project
   schedule: "2 3 4 5 6"
```
```sh
dbt-remote schedules set schedules.yaml
```
```sh
[...]
The following actions will be performed:
+ Add: schedule-1
- Delete: schedule-2
~ Redeploy: schedule-3
Do you want to continue? [y/N]: y
```

To check your scheduled runs, either go to the [cloud scheduler UI of your project](https://console.cloud.google.com/cloudscheduler), or list them uting the cli:
```sh
dbt-remote schedules list
```
```sh
[...]
dbt-server-e11f1085-8ad9-4dcd-b09f-d8a8369075b9
   command: run
   schedule: 0 8 * * * (At 08:00 AM) UTC
   target: https://dbt-server-vo6sb27zvq-ew.a.run.app/schedule/e11f1085-8ad9-4dcd-b09f-d8a8369075b9/start
```

You can also delete them in the UI, or using the CLI:
```sh
dbt-remote schedules delete e11f1085-8ad9-4dcd-b09f-d8a8369075b9
```
```sh
[...]
Schedule dbt-server-e11f1085-8ad9-4dcd-b09f-d8a8369075b9 deleted
```

### (optional) Set persistent configurations for `dbt-remote` using `config` command
```sh
dbt-remote config set server_url=http://myserver.com location=europe-west9
```

View all configuration options
```sh
dbt-remote config help
```
