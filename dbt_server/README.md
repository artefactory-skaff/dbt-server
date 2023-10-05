# dbt-server (for data platform engineers)

This section is dedicated to ```dbt-server``` deployment and maintenance by system administrators.

```dbt-server``` is a Fastapi server allowing users to run ```dbt``` commands on Cloud Run jobs and to follow their execution in real-time. When the server receives a dbt command request, it creates and launches a job to execute it. The complete workflow is described in the [Architecture schema](../dbt-remote-schema.png).

<center><img src="../dbt-remote-schema-simplified.png" width="100%"></center>


## Requirements

You must have the following roles: `roles/datastore.owner`, `roles/logging.logWriter`, `roles/logging.viewer`, `roles/storage.admin`, `roles/run.developer`, `roles/iam.serviceAccountUser`.

You must have [gcloud CLI](https://cloud.google.com/sdk/docs/authorizing) set up with your project. If not, run:
```sh
gcloud auth login
gcloud auth application-default login
gcloud config set project <your-project-id>
```


## Deployment

Clone this repository and go to the ```dbt-server``` folder.
```sh
git clone git@github.com:artefactory-fr/dbt-server.git
cd dbt-server
git checkout diff-pr
```

Export your env variables.
```sh
export PROJECT_ID=<your-project-id> &&
export LOCATION=europe-west9
```

Create an artifact registry
```sh
gcloud artifacts repositories create dbt-server --repository-format=docker --location=$LOCATION --description="Used to host the dbt-server docker image. https://github.com/artefactory-fr/dbt-server"
```

Create a bucket for artifacts
```sh
gcloud storage buckets create gs://$PROJECT_ID-dbt-server --project=$PROJECT_ID --location=$LOCATION
```

Create a service account that will be used for dbt runs
```sh
gcloud iam service-accounts create dbt-server --project=${PROJECT_ID};
```

Assign roles to the SA
```sh
ROLES=(
  "datastore.user"
  "storage.admin"
  "bigquery.dataEditor"
  "bigquery.jobUser"
  "bigquery.dataViewer"
  "bigquery.metadataViewer"
  "run.developer"
  "iam.serviceAccountUser"
  "logging.logWriter"
  "logging.viewer"
);

for ROLE in ${ROLES[@]}
do
  gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member=serviceAccount:dbt-server@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/${ROLE};
done
```

Enable GCP APIs
```sh
gcloud services enable \
    cloudbuild.googleapis.com \
    firestore.googleapis.com \
    run.googleapis.com \
    bigquery.googleapis.com \
    --project=$PROJECT_ID
```

Create Firestore database (default) if it does not exist
```sh
gcloud firestore databases create --location=$LOCATION;
```

Build the server image
```sh
gcloud builds submit ./dbt_server/ --region=$LOCATION --tag $LOCATION-docker.pkg.dev/$PROJECT_ID/dbt-server/dbt-server
```

Deploy the server on Cloud Run
```sh
gcloud run deploy dbt-server \
	--image ${LOCATION}-docker.pkg.dev/${PROJECT_ID}/dbt-server/dbt-server \
	--platform managed \
	--region ${LOCATION} \
	--service-account=dbt-server@${PROJECT_ID}.iam.gserviceaccount.com \
	--set-env-vars=BUCKET_NAME=${PROJECT_ID}-dbt-server \
	--set-env-vars=DOCKER_IMAGE=${LOCATION}-docker.pkg.dev/${PROJECT_ID}/dbt-server/dbt-server \
	--set-env-vars=SERVICE_ACCOUNT=dbt-server@${PROJECT_ID}.iam.gserviceaccount.com \
	--set-env-vars=PROJECT_ID=${PROJECT_ID} \
	--set-env-vars=LOCATION=${LOCATION} \
  --no-allow-unauthenticated
```

The deployment of your dbt-server is finished! 

To test it, you can [install the `dbt-remote` CLI](../README.md) and run it to execute dbt commands on your server, such as
```sh
dbt-remote debug
```
