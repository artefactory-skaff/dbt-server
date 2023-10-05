# dbt-remote project

This package provides 
- `dbt-remote`, a drop-in replacement for the dbt CLI for analytics engineers. 
- `dbt-server`, a Cloud Run API that will need to be deployed to perform the remote dbt runs (for data platform engineers: [How to deploy?](./dbt_server/README.md)).

<center><img src="./intro-README.png" width="100%"></center>


# dbt-remote

This CLI runs dbt commands remotely on a GCP. 

## Requirements

- [A deployed dbt-server](./dbt_server/README.md)
- [An initialized dbt project](https://docs.getdbt.com/quickstarts/) > "Quickstart for dbt Core from a manual install" (end of page)
- [gcloud CLI](https://cloud.google.com/sdk/docs/authorizing) set up with your project. If not, run:
```sh
gcloud auth login
gcloud auth application-default login
gcloud config set project <your-project-id>
```

## Installation

```sh
python3 -m pip install --extra-index-url https://test.pypi.org/simple/ gcp-dbt-remote --no-cache-dir
```

### Check the installation

1. Make sure your dbt project is setup properly locally.
```sh
dbt debug --profiles-dir .
```
Expected: `All checks passed!`

2. If not done yet, export you GCP project ID:
```sh
export PROJECT_ID=<your-project-id>
```

3. Test the CLI installation (requires you to have deployed the `dbt-server`)
```sh 
dbt-remote debug
```
Expected:
```sh
INFO    [dbt]All checks passed!
INFO    [job]Command successfully executed
```

4. Use `dbt-remote` just like you would do with the regular dbt CLI
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

5. (optional) Set persistant configurations for `dbt-remote` using `config` command
```sh
dbt-remote config init
dbt-remote config set server_url=http://myserver.com location=europe-west9
```

View all configuration options
```sh
dbt-remote config help
```

