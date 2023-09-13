# dbt-server

```dbt-server``` is a Fastapi server allowing users to run ```dbt``` commands on Cloud Run jobs and following their execution in real-time.

## Deployment

To deploy this server on GCP, you can use our Terraform code.

[//]: #Make sure that the GCP service account has the following permissions:
[//]: #```gcloud projects add-iam-policy-binding stc-dbt-test-9e19 --role roles/datastore.user --member serviceAccount:stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com```
[//]: #```roles/storage.admin```
[//]: #```roles/bigquery.dataEditor```
[//]: #```roles/bigquery.jobUser```
[//]: #```roles/bigquery.dataViewer```
[//]: #```roles/bigquery.metadataViewer```
[//]: #```roles/run.developer```
[//]: #```roles/iam.serviceAccountUser```
[//]: #```roles/logging.logWriter```
[//]: #```roles/logging.viewer```

[//]: #Send new Dockerfile image
[//]: #```gcloud builds submit --region=us-central1 --tag us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod```

[//]: #Launch Cloud Run dbt-server
[//]: #```gcloud run deploy server-prod --image us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod --platform managed --region us-central1 --service-account=stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com --set-env-vars=BUCKET_NAME='dbt-stc-test' --set-env-vars=DOCKER_IMAGE='us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:prod' --set-env-vars=SERVICE_ACCOUNT='stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com' --set-env-vars=PROJECT_ID='stc-dbt-test-9e19' --set-env-vars=LOCATION='us-central1'```

## Local run

You can also run the server locally using:

```sh
python3 api/dbt_server.py --local
```

This configuration still connects to GCP and expects a project ID, bucket name, etc. Make sure you have sufficient permissions ("roles/datastore.owner", "roles/logging.logWriter", "roles/logging.viewer", "roles/storage.admin", "roles/run.developer", "roles/iam.serviceAccountUser") and declare your configuration:

```sh
export BUCKET_NAME=<bucket-name>
export DOCKER_IMAGE=<docker-image>
export SERVICE_ACCOUNT=<service-account-email>
export PROJECT_ID=<project-id>
export LOCATION=<location>
```