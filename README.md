# dbt-server

## Initialization

Make sure that the GCP service account has the following permissions:
```gcloud projects add-iam-policy-binding stc-dbt-test-9e19 --role roles/datastore.user --member serviceAccount:stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com```
```roles/storage.admin```
```roles/bigquery.dataEditor```
```roles/bigquery.jobUser```
```roles/bigquery.dataViewer```
```roles/bigquery.metadataViewer```
```roles/run.developer```
```roles/iam.serviceAccountUser```
```roles/logging.logWriter```
```roles/logging.viewer```

Send new Dockerfile image
```gcloud builds submit --region=us-central1 --tag us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod```

Launch Cloud Run dbt-server
```gcloud run deploy server-prod --image us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod --platform managed --region us-central1 --service-account=stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com --set-env-vars=BUCKET_NAME='dbt-stc-test' --set-env-vars=DOCKER_IMAGE='us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod' --set-env-vars=SERVICE_ACCOUNT='stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com' --set-env-vars=PROJECT_ID='stc-dbt-test-9e19' --set-env-vars=LOCATION='us-central1'```

Initialize Python environment
```poetry run pip install -r requirements.txt```

## Test dbt server with Client

In ```api``` directory:
```poetry run python3 client.py```

## Local run

First, export global variables:

```export BUCKET_NAME='dbt-stc-test'```
```export DOCKER_IMAGE='us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod'```
```export SERVICE_ACCOUNT='stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com'```
```export PROJECT_ID='stc-dbt-test-9e19'```
```export LOCATION='us-central1'```


In ```api``` directory:
```poetry run python3 dbt_server.py --local```

```poetry run python3 client.py --local```

## Build the package

From the root of the project:

```poetry build```