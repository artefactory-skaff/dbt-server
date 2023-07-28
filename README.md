# dbt-server

Send new Dockerfile
```gcloud builds submit --region=us-central1 --tag us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:tag3```

Launch Cloud Run dbt-server
```gcloud run deploy server-image-3 --image us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:tag3 --platform managed --region us-central1 --service-account=stc-dbt-sa@stc-dbt-test-9e19.iam.gserviceaccount.com --set-env-vars=BUCKET_NAME='dbt-stc-test' --set-env-vars=DOCKER_IMAGE='us-central1-docker.pkg.dev/stc-dbt-test-9e19/cloud-run-dbt/server-image:tag3'```

## Test dbt server with Client

```poetry run python3 run client.py```