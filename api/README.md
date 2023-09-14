# dbt-server

```dbt-server``` is a Fastapi server allowing users to run ```dbt``` commands on Cloud Run jobs and following their execution in real-time.

## Deployment

To deploy this server on GCP, you can use our Terraform code.

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
