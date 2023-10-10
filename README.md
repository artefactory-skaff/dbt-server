# dbt-remote project

This package provides 
- `dbt-remote`, a drop-in replacement for the dbt CLI for analytics engineers. 
- `dbt-server`, a Cloud Run API that will need to be deployed to perform the remote dbt runs (for data platform engineers: [How to deploy?](./dbt_server/README.md)).

<center><img src="./intro-README.png" width="100%"></center>


# dbt-remote

This CLI runs dbt commands remotely on a GCP. 

## Requirements

- A deployed dbt-server. [(dbt-server deployment guide)](./dbt_server/README.md)
- An initialized dbt core project. [(dbt core quickstart)](https://docs.getdbt.com/quickstarts/manual-install?step=1)
- The gcloud CLI. [(gcloud install guide)](https://cloud.google.com/sdk/docs/install)

## Installation

```sh
python3 -m pip install --extra-index-url https://test.pypi.org/simple/ gcp-dbt-remote --no-cache-dir
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
> INFO    [dbt]All checks passed!
> INFO    [job]Command successfully executed
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

### (optional) Set persistant configurations for `dbt-remote` using `config` command
```sh
dbt-remote config set server_url=http://myserver.com location=europe-west9
```

View all configuration options
```sh
dbt-remote config help
```
