# dbt-server

```dbt-server``` is a Fastapi server allowing users to run ```dbt``` commands on Cloud Run jobs and following their execution in real-time.

## Deployment

### Submit Docker image

To deploy this server on GCP, you need to first upload the ```Dockerfile``` on GCP. To this end, follow these steps:

1. Check that ```profiles.yml``` contains the right information regarding your dbt project (```dataset```, ```project```, ```location```).

2. If you do not have an artifact registry on GCP yet, create one.

3. Submit the ```Dockerfile ``` to the registry. Ex using ```gcloud```:
```sh
gcloud builds submit --region=<location> --tag <location>-docker.pkg.dev/<project-id>/<artifact-registry>/<image-name>
```
(```<location>``` should be your Artifact Registry's location)


### Deployment on GCP

Once you image is on your Artifact registry, you can use our Terraform code.

In the ```terraform/``` folder, check the content of ```variables.tf```. In particular, the ```project_id``` and the ```docker_image``` (it should be ```<location>-docker.pkg.dev/<project-id>/<artifact-registry>/<image-name>```).

Finally run ```terraform plan``` and ```terraform apply```. You should see your server url.

To check that your server is properly deployed, you can run
```sh
curl https://<your-server>/check
```
You should receive a response similar to: ```{"response":"Running dbt-server on port 8080"}```


## Local run

You can also run the server locally using ```dbt_server.py```.

**Be careful**, this configuration still connects to GCP and expects a environement configuration as well as some cloud resources (e.g. a Cloud Storage bucket). This means **you must create different GCP resources beforehand**. To this end, we recommend running the ```Terraform``` code once (see section above). Otherwise, you can manually create the resources (see section below).

Make sure you have sufficient permissions ("roles/datastore.owner", "roles/logging.logWriter", "roles/logging.viewer", "roles/storage.admin", "roles/run.developer", "roles/iam.serviceAccountUser") and declare your configuration in your terminal:
```sh
export BUCKET_NAME=<bucket-name>
export DOCKER_IMAGE=<docker-image>
export SERVICE_ACCOUNT=<service-account-email>
export PROJECT_ID=<project-id>
export LOCATION=<location>
```
Info: If you used Terraform to create the resources, ```service-account-email``` should be ```terraform-job-sa@<project-id>.iam.gserviceaccount.com``` and ```bucket-name``` ```dbt-stc-test-eu```.

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
- create a service account with the following permissions: ("roles/datastore.user", "roles/logging.logWriter", "roles/logging.viewer", "roles/storage.admin", "roles/bigquery.dataEditor", "roles/bigquery.jobUser", "roles/bigquery.dataViewer", "roles/bigquery.metadataViewer")

## Add a new use case

If you want to use your dbt-server for a new dbt project, you need to modify the ```profiles.yml``` file in the Docker image, so you have to re-submit it and re-deploy a ```dbt-server```.