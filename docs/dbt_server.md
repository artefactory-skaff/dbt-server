# dbt-server

This section is dedicated to `dbt-server` deployment and maintenance by system administrators.

`dbt-server` is a Fastapi server allowing users to run `dbt` commands on Cloud Run jobs and to follow their execution in real-time. When the server receives a dbt command request, it creates and launches a job to execute it. The complete workflow is described in the [Architecture schema](images/dbt-remote-schema.png).


![Simplified architecture](images/dbt-remote-schema-simplified.png)


- [Requirements](#requirement-submit-docker-image)
- [Deployment](#deployment-on-gcp)
- [Requests examples using curl](#send-requests-to-dbt-server)
- [Local run](#local-run--for-contributors)

## Requirement (submit the Docker image)

To deploy this server on GCP, you need to first build and deploy the `Dockerfile` on your Artifact registry on GCP. To this end, follow these steps:

### 1. Clone repository
Clone this repository and go to the `dbt-server` folder.
```sh
git clone git@github.com:artefactory-fr/dbt-server.git
cd dbt-server
```

### 2. Profiles.yml content
Check that `profiles.yml` contains the right information regarding your dbt project (`dataset`, `project`, `location`). You can also replace this file by your dbt project's `profiles.yml`.

### 3. Artifact registry creation

If you do not have any artifact registry on GCP yet, [create one][create-artifact-registry]. One way to do this is to use [gcloud cli][gcloud]:
```sh
gcloud artifacts repositories create <REPOSITORY> --repository-format=docker --location=<LOCATION> --description="<DESCRIPTION>"
```
with `REPOSITORY` your repository name, `LOCATION` the location you want and `DESCRIPTION` its description.

Ex:
```sh
gcloud artifacts repositories create dbt-server-repository --repository-format=docker --location=europe-west9 --description="The repository dedicated to dbt-server Docker images."
```

### 4. Build/deploy the Docker image

Build and deploy the `Dockerfile` to the registry. Using [gcloud cli][gcloud]:
```sh
gcloud builds submit --region=<LOCATION> --tag <LOCATION>-docker.pkg.dev/<PROJECT-ID>/<REPOSITORY>/<IMAGE>
```
where:
- `<LOCATION>` should be your Artifact Registry's location
- `<PROJECT-ID>` is your GCP Project ID
- `<REPOSITORY>` is the name of your artifact registry
- `<IMAGE>` is the name you want to give to your Docker image

Ex:
```sh
gcloud builds submit --region=europe-west9 --tag europe-west9-docker.pkg.dev/my-project-id/dbt-server-repository/dbt-server
```


## Deployment on GCP

> :warning: **Recommendation**
The dbt-server deployment is easier and quicker using [Terraform][terraform]. We highly recommend to use the [following module][dbt-server-terraform-module-repo].

### Resources creation

To run your dbt-server, you need to create the following resources.

- a bucket on Cloud Storage
```sh
gcloud storage buckets create gs://<BUCKET_NAME> --project=<PROJECT_ID> --location=<LOCATION>
```

- a service account with the necessary permissions. Change the value of `<PROJECT-ID>` then execute the following code in your terminal:
```sh
PROJECT="<PROJECT-ID>";
ACCOUNT="dbt-server-sa";

EMAIL="${ACCOUNT}@${PROJECT}.iam.gserviceaccount.com";

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

gcloud iam service-accounts create ${ACCOUNT} \
--project=${PROJECT};

for ROLE in ${ROLES[@]}
do
  echo $ROLE;
  gcloud projects add-iam-policy-binding ${PROJECT} \
  --member=serviceAccount:${EMAIL} \
  --role=roles/${ROLE};
done
```

You also need to enable the following APIs:

- Firestore API
```sh
gcloud services enable firestore.googleapis.com
```
- Cloud Run API
```sh
gcloud services enable run.googleapis.com
```


### dbt-server deployment

To deploy your dbt-server, complete the following code with the resources you created and run it:
```sh
gcloud run deploy <SERVER-NAME> \
	--image <IMAGE> \
	--platform managed \
	--region <LOCATION> \
	--service-account=<SERVICE-ACCOUNT> \
	--set-env-vars=BUCKET_NAME=<BUCKET-NAME> \
	--set-env-vars=DOCKER_IMAGE=<IMAGE> \
	--set-env-vars=SERVICE_ACCOUNT=<SERVICE-ACCOUNT> \
	--set-env-vars=PROJECT_ID=<PROJECT-ID> \
	--set-env-vars=LOCATION=<LOCATION>
```

Ex:
```sh
gcloud run deploy dbt-server-test \
	--image europe-west9-docker.pkg.dev/my-project-id/test-repository/server-image \
	--platform managed \
	--region europe-west9 \
	--service-account=dbt-server-sa@my-project-id.iam.gserviceaccount.com \
	--set-env-vars=BUCKET_NAME='dbt-server-bucket' \
	--set-env-vars=DOCKER_IMAGE='europe-west9-docker.pkg.dev/my-project-id/test-repository/server-image' \
	--set-env-vars=SERVICE_ACCOUNT='dbt-server-sa@my-project-id.iam.gserviceaccount.com' \
	--set-env-vars=PROJECT_ID='my-project-id' \
	--set-env-vars=LOCATION='europe-west9'
```
When asked "Allow unauthenticated invocations to [dbt-server-test] (y/N)?"
- type "y" to disable the authentication
- type "N" to enforce the authentication. (**recommended**)


### Verify the deployment

To check that your server is properly deployed, you can run:

- if authentication
```sh
my_server=https://<SERVER-URL>
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

curl -H "$header" $my_server/check
```

- if no authentication
```sh
curl https://<your-server>/check
```
You should receive a response similar to: `{"response":"Running dbt-server on port 8080"}`.


## Send requests to dbt-server

The following requests allow you to send commands to your server using `curl` rather than the cli.

- [Check your dbt-server](#check-dbt-server)
- [Send dbt commands](#post-dbt-command)
- [Follow your job execution](#follow-the-job-execution)

> **Authentication.**
> To comply with the server authentication, these requests include 2 lines to fetch an id_token using `gcloud` and to add it to the request's headers:
>```sh
>my_token=$(gcloud auth print-identity-token)
>header="Authorization: Bearer $my_token"
>```
>If your server does not require authentication, you can remove this part (as well as the `-H "$header"` from the curl commands).

### Check dbt-server

This command just check if your dbt-server is up and running. The response should be similar to `{"response":"Running dbt-server on port 8001"}`.

```sh
my_server=https://<SERVER-URL>
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

curl -H "$header" $my_server/check
```

### Post dbt command

To `POST` a dbt command to the server, we need to send files (`manifest.json`, `dbt_project.yml` and possibly more). Since some files can be quite large (`manifest.json` in particular), we encode them using base64 and we temporarily store the request's body in a `data.json` file. Then we use the `--data-binary` curl option to send our request.

Before sending these requests, replace `<SERVER-URL>` by your server url and make sure the files' paths are the right ones (`dbt_project.yml`, `manifest.json`, etc.).


**`dbt list` command:**
```sh
my_server="https://<SERVER-URL>"

my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

dbt_project=$(base64 -i dbt_project.yml); # <-- should be your path to your dbt_project.yml file
manifest=$(base64 -i target/manifest.json); # <-- same for your manifest.json file

echo '{"server_url":"'$my_server'", "user_command":"list", "manifest": "'$manifest'", "dbt_project":"'$dbt_project'"}' > data.json;

curl --data-binary @data.json -H "$header" -H "Content-Type: application/json" -X POST $my_server/dbt
```

**`dbt run` a specific model with Elementary package and report:**

Replace `<MODEL>` by one of your models.

```sh
my_server="https://<SERVER-URL>"

my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

dbt_project=$(base64 -i dbt_project.yml); # <-- should be your path to your dbt_project.yml file
manifest=$(base64 -i target/manifest.json); # <-- same for your manifest.json file
packages=$(base64 -i packages.yml); # <-- same for your packages.yml file

echo '{"server_url":"'$my_server'", "user_command":"run --select <MODEL>", "manifest": "'$manifest'", "dbt_project":"'$dbt_project'", "packages":"'$packages'", "elementary":"True"}' > data.json;

curl --data-binary @data.json -H "$header" -H "Content-Type: application/json" -X POST $my_server/dbt
```

**`dbt seed` with one particular seed file (country_code.csv):**
```sh
my_server="https://<SERVER-URL>"

my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

dbt_project=$(base64 -i dbt_project.yml); # <-- should be your path to your dbt_project.yml file
manifest=$(base64 -i target/manifest.json); # <-- same for your manifest.json file
packages=$(base64 -i packages.yml); # <-- same for your packages.yml file
seed_file=$(base64 -i seeds/country_codes.csv); # <-- same for your seed file
seeds='{"seeds/country_codes.csv":"'$seed_file'"}'; # <-- don't forget to change the file name

echo '{"server_url":"'$my_server'", "user_command":"seed", "manifest": "'$manifest'", "dbt_project":"'$dbt_project'", "packages":"'$packages'", "seeds":'$seeds'}' > data.json;

curl --data-binary @data.json -H "$header" -H "Content-Type: application/json" -X POST $my_server/dbt
```

### Follow the job execution

Replace `<UUID>` by your job's UUID. ex: `d710dc13-6175-4735-8649-31c39a4a0e90`.

**Get job run status:**

```sh
my_server="https://<SERVER-URL>"
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"
my_job_uuid=<UUID>

curl -H "$header" $my_server/job/$my_job_uuid
```

**Get job logs:**
```sh
my_server="https://<SERVER-URL>"
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"
my_job_uuid=<UUID>

curl -H "$header" $my_server/job/$my_job_uuid/logs
```

**Get elementary report:** (at the end of the execution)
```sh
my_server="https://<SERVER-URL>"
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"
my_job_uuid=<UUID>

curl -H "$header" -L $my_server/job/$my_job_uuid/report
```
> Note: the `-L` is necessary because the `/report` endpoint is a redirection to the GCS report url.


## Local run / For contributors

You can also run the server locally using `dbt_server.py`.

**Be careful, this configuration still connects to GCP and expects an environement configuration as well as some cloud resources** (e.g. a Cloud Storage bucket). This means **you must create different GCP resources beforehand**. To this end, we recommend running the `Terraform` module or the manual resource creation (see [section above](#deployment-on-gcp)).

Make sure you have sufficient permissions (`roles/datastore.owner`, `roles/logging.logWriter`, `roles/logging.viewer`, `roles/storage.admin`, `roles/run.developer`, `roles/iam.serviceAccountUser`) and declare your configuration in your terminal:
```sh
export BUCKET_NAME=<bucket-name>
export DOCKER_IMAGE=<docker-image>
export SERVICE_ACCOUNT=<service-account-email>
export PROJECT_ID=<project-id>
export LOCATION=<location>
```
> **Info**: If you used Terraform to create the resources, `<service-account-email>` should be `terraform-job-sa@<project-id>.iam.gserviceaccount.com` and `<bucket-name>` `dbt-server-test`.

Install the dependencies:
```sh
pip install -r requirements.txt
```
Then launch the ```dbt-server```:
```sh
cd dbt_server; python3 dbt_server.py --local
```

Your dbt-server should run on `http://0.0.0.0:8001`.


[//]: #


   [gcloud]: <https://cloud.google.com/sdk/docs/install>
   [create-artifact-registry]: <https://cloud.google.com/artifact-registry/docs/repositories/create-repos>
   [terraform]: <https://www.terraform.io/>

   [dbt-server-terraform-module-repo]: <https://github.com/artefactory-fr/terraform-gcp-dbt-server/tree/add-dbt-server-terraform-code>
