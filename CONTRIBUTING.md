## Deploy locally

```
poetry run python cli/main.py remote deploy --cloud-provider local
```

## Deploy a dev server to GCP

Submit a dev image
```
LOCATION=europe-west1 PROJECT_ID=dbt-server-sbx-01-caed ENV=dev TAG=avi task image
```
> Pick your own tag. Mine is `avi` because I'm Alexis Vialaret

Deploy a new dev server revision
```
poetry run python cli/main.py remote deploy --image europe-docker.pkg.dev/dbt-server-sbx-01-caed/dbt-server/dev:avi --service dbt-server-avi
```

## Publish the package

Increment the version number in `pyproject.toml`, `cli/version/py`, and `server/version.py`.

Publish to Pypi after your changes are validated.
```shell
task publish
```
