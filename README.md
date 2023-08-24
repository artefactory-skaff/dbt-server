# dbt-server

## Initialization

Make sure that the GCP service account has the following permissions:
```gcloud projects add-iam-policy-binding stc-dbt-test-9e19 --role roles/datastore.user --member serviceAccount:stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com```
```roles/storage.admin```
```roles/bigquery.dataEditor```
```roles/bigquery.jobUser```
```roles/run.developer```
```roles/iam.serviceAccountUser```
```roles/logging.logWriter```
```roles/logging.viewer```

For Elementary
```gcloud projects add-iam-policy-binding stc-dbt-test-9e19 --role roles/bigquery.jobUser --member serviceAccount:stc-dbt-elementary-sa@stc-dbt-test-9e19.iam.gserviceaccount.com```
```gcloud projects add-iam-policy-binding stc-dbt-test-9e19 --role roles/bigquery.dataViewer --member serviceAccount:stc-dbt-elementary-sa@stc-dbt-test-9e19.iam.gserviceaccount.com```
```gcloud projects add-iam-policy-binding stc-dbt-test-9e19 --role roles/bigquery.metadataViewer --member serviceAccount:stc-dbt-elementary-sa@stc-dbt-test-9e19.iam.gserviceaccount.com```

Add elementary's service account credentials to ```api/``` as ```elementary-sa.json```

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

## Use dbt-remote cli

In ```cli``` directory, execute:
```poetry run python3 -m pip install --editable .```

Make sure you are in a dbt project (```dbt_project.yml``` should be in your current directory and ```manifest.json``` should be in ```./target```)
Then run commands like ```poetry run dbt-remote run --select vbak_dbt```.

Otherwise, you can specify both files' path: ```poetry run dbt-remote run --manifest ../test-files/manifest.json --select vbak_dbt --dbt_project ../test-files/dbt_project.yml```.

You can precise the level of log you wish to see from the dbt execution. Ex: 
```poetry run dbt-remote --log-level warn run --select vbak```

Other examples:

- run --select: ```poetry run dbt-remote --log-level info run --manifest ../test-files/elementary/manifest.json --select vbak_dbt --dbt_project ../test-files/elementary/dbt_project.yml --packages ../test-files/elementary/packages.yml --elementary --set_timer```

without elementary: ```poetry run dbt-remote --log-level info run --manifest ../test-files/manifest.json --select vbak_dbt --dbt_project ../test-files/dbt_project.yml --set_timer```

- list: ```poetry run dbt-remote --log-level debug list --manifest ../test-files/elementary/manifest.json --dbt_project ../test-files/elementary/dbt_project.yml --packages ../test-files/elementary/packages.yml --elementary --set_timer```
