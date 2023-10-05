# dbt-server

This section is dedicated to `dbt-server` deployment and maintenance by system administrators.

`dbt-server` is a Fastapi server allowing users to run `dbt` commands on Cloud Run jobs and to follow their execution in real-time. When the server receives a dbt command request, it creates and launches a job to execute it. The complete workflow is described in the [Architecture schema](images/dbt-remote-schema.png).


![Simplified architecture](images/dbt-remote-schema-simplified.png)


- [Requirements](#requirement-submit-docker-image)
- [Deployment](#deployment-on-gcp)
- [Requests examples using curl](#send-requests-to-dbt-server)
- [Local run](#local-run)

## Requirement and installation

See repository's [README][README].



## Local run

You can run the server locally using `dbt_server.py`.

**Be careful, this configuration still connects to GCP and expects an environement configuration as well as some cloud resources** (e.g. a Cloud Storage bucket). This means **you must create different GCP resources beforehand**. To this end, we recommend running the `Terraform` module or the manual resource creation (see [section above](#deployment-on-gcp)).

Make sure you have sufficient permissions (`roles/datastore.owner`, `roles/logging.logWriter`, `roles/logging.viewer`, `roles/storage.admin`, `roles/run.developer`, `roles/iam.serviceAccountUser`).


### With Poetry (recommended)

1. **Install poetry** ([installation guide](https://python-poetry.org/docs/))
2. At the root of the project, **run**:
```sh
poetry lock -n; poetry install;
```
3. Export the environment variables
```sh
export BUCKET_NAME=<bucket-name>
export DOCKER_IMAGE=<docker-image>
export SERVICE_ACCOUNT=<service-account-email>
export PROJECT_ID=<project-id>
export LOCATION=<location>
```
> **Info**: If you used Terraform to create the resources, `<service-account-email>` should be `terraform-job-sa@<project-id>.iam.gserviceaccount.com` and `<bucket-name>` `dbt-server-test`.
4. Launch the server.
```sh
cd dbt_server
poetry run python3 dbt_server.py --local
```

### Without Poetry

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


[//]: #


   [gcloud]: <https://cloud.google.com/sdk/docs/install>
   [create-artifact-registry]: <https://cloud.google.com/artifact-registry/docs/repositories/create-repos>
   [terraform]: <https://www.terraform.io/>

   [dbt-server-terraform-module-repo]: <https://github.com/artefactory-fr/terraform-gcp-dbt-server/tree/add-dbt-server-terraform-code>

   [README]: <https://github.com/artefactory-fr/dbt-server/blob/main/README.md>
