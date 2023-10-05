# dbt-remote project

This package provides 
- `dbt-remote`, a drop-in replacement for the dbt CLI. 
- `dbt-server`, a Cloud Run API that will need to be deployed to perform the remote dbt runs.

<center><img src="./intro-README.png" width="100%"></center>


# dbt-remote

This CLI runs dbt commands remotely on a GCP. 

## Requirements

- [A deployed dbt-server](#dbt-server)
- [An initialized dbt project](https://docs.getdbt.com/quickstarts/) > "Quickstart for dbt Core from a manual install" (end of page)

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

----
# dbt-server

This section is dedicated to ```dbt-server``` deployment and maintenance by system administrators.

```dbt-server``` is a Fastapi server allowing users to run ```dbt``` commands on Cloud Run jobs and to follow their execution in real-time. When the server receives a dbt command request, it creates and launches a job to execute it. The complete workflow is described in the [Architecture schema](./dbt-remote-schema.png).

<center><img src="./dbt-remote-schema-simplified.png" width="100%"></center>


## Requirements

You must have the following roles: `roles/datastore.owner`, `roles/logging.logWriter`, `roles/logging.viewer`, `roles/storage.admin`, `roles/run.developer`, `roles/iam.serviceAccountUser`.


## Deployment

Clone this repository and go to the ```dbt-server``` folder.
```sh
git clone git@github.com:artefactory-fr/dbt-server.git
cd dbt-server
git checkout diff-pr
```

> Note: Possible change `<PROJECT_ID>` line 2 in `deploy.sh` and run
>```sh
>chmod +x deploy.sh; ./deploy.sh
>```

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
    firestore.googleapis.com \
    run.googleapis.com \
    --project=$PROJECT_ID
```

Create Firestore database (default) if it does not exist
```sh
database=$(gcloud firestore databases list | grep "projects/${PROJECT_ID}/databases/(default)")
if [ -z "$database" ]
then
   echo "(default) database does not exist, creating one...";
   gcloud firestore databases create --location=nam5;
   echo "Created";
else
   echo "(default) database already exists";
fi
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

You should now be able to run the `dbt-remote` CLI to execute dbt commands on your server
```sh
dbt-remote debug
```

### Local Run

Instead of running your dbt-server on a Cloud Run service, you can run it locally.

> :warning: you still need all the other GCP resources. You must first follow all the steps from [Deployment section](#deployment) until Firestore database creation (included)!

1. Export the environment variables
```sh
export BUCKET_NAME=<bucket-name>
export DOCKER_IMAGE=<docker-image>
export SERVICE_ACCOUNT=<service-account-email>
export PROJECT_ID=<project-id>
export LOCATION=<location>
```
> **Info**: If you used Terraform to create the resources, `<service-account-email>` should be `terraform-job-sa@<project-id>.iam.gserviceaccount.com` and `<bucket-name>` `dbt-server-test`.
2. Install the dependencies
```sh
cd dbt_server; pip install -r requirements.txt
```
3. Launch the ```dbt-server```:
```sh
python3 dbt_server.py --local
```

Your dbt-server should run on `http://0.0.0.0:8001`.
