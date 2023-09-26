# dbt-server (admin use)

This section is dedicated to ```dbt-server``` deployment and maintenance by system administrators.

```dbt-server``` is a Fastapi server allowing users to run ```dbt``` commands on Cloud Run jobs and following their execution in real-time. When the server receives a dbt command request, it creates and launches a job to execute it. The complete workflow is described in the [Architecture schema](./dbt-remote-schema.png).

<center><img src="./dbt-remote-schema.png" width="80%"></center>

## Before deployment: Submit Docker image

To deploy this server on GCP, you need to first build and deploy the ```Dockerfile``` on your Artifact registry on GCP. To this end, follow these steps:

1. Check that ```profiles.yml``` contains the right information regarding your dbt project (```dataset```, ```project```, ```location```).

2. If you do not have any artifact registry on GCP yet, [create one][create-artifact-registry].

3. Build and deploy the ```Dockerfile ``` to the registry. One way to do this is to use [```gcloud``` cli][gcloud]:
```sh
gcloud builds submit --region=<location> --tag <location>-docker.pkg.dev/<project-id>/<artifact-registry>/<image-name>
```
where:
- ```<location>``` should be your Artifact Registry's location
- ```<project-id>``` is your GCP Project ID
- ```<artifact-registry>``` is the name of your artifact registry
- ```<image-name>``` is the name you want to give to your Docker image


## Deployment on GCP

Once you image is on your Artifact registry, you can use the [Terraform code][terraform-code] from the ```terraform/``` folder.

First, check/update the content of ```variables.tf```. In particular:
- the ```project_id``` (**change required**)
- the ```docker_image``` (**change required**) (it should be ```<location>-docker.pkg.dev/<project-id>/<artifact-registry>/<image-name>```)
- the ```location``` (**change required**)

Then run ```terraform plan``` and ```terraform apply```. You should see your server url in the terminal.

To check that your server is properly deployed, you can run
```sh
curl https://<your-server>/check
```
You should receive a response similar to: ```{"response":"Running dbt-server on port 8080"}```.


## Local run / For contributors

You can also run the server locally using ```dbt_server.py```.

**Be careful, this configuration still connects to GCP and expects a environement configuration as well as some cloud resources** (e.g. a Cloud Storage bucket). This means **you must create different GCP resources beforehand**. To this end, we recommend running the ```Terraform``` code once (see [section above](#deployment-on-gcp)). Otherwise, you can manually create the resources (see [section below](#manually-create-necessary-resources)).

Make sure you have sufficient permissions (```roles/datastore.owner```, ```roles/logging.logWriter```, ```roles/logging.viewer```, ```roles/storage.admin```, ```roles/run.developer```, ```roles/iam.serviceAccountUser```) and declare your configuration in your terminal:
```sh
export BUCKET_NAME=<bucket-name>
export DOCKER_IMAGE=<docker-image>
export SERVICE_ACCOUNT=<service-account-email>
export PROJECT_ID=<project-id>
export LOCATION=<location>
```
**Info**: If you used Terraform to create the resources, ```<service-account-email>``` should be ```terraform-job-sa@<project-id>.iam.gserviceaccount.com``` and ```<bucket-name>``` ```dbt-stc-test-eu```.

Install the dependencies:
```sh
pip install -r requirements.txt
```
Then launch the ```dbt-server```:
```sh
python3 api/dbt_server.py --local
```

Your dbt-server should run on ```http://0.0.0.0:8001```.


### Manually create necessary resources

You need to create the following resources:
- a bucket on Cloud Storage
- enable Firestore API
- enable Cloud Run API
- create a service account with the following permissions: (```roles/datastore.user```, ```roles/logging.logWriter```, ```roles/logging.viewer```, ```roles/storage.admin```, ```roles/bigquery.dataEditor```, ```roles/bigquery.jobUser```, ```roles/bigquery.dataViewer```, ```roles/bigquery.metadataViewer```)

## Add a new use case

If you want to use your dbt-server for a new dbt project, you need to modify the ```profiles.yml``` file in the Docker image, so you have to re-submit it and re-deploy a ```dbt-server```.

[//]: #

   [create-artifact-registry]: <https://cloud.google.com/artifact-registry/docs/repositories/create-repos>
   [terraform-code]: ../terraform/
   [gcloud]: <https://cloud.google.com/sdk/docs/install>
