# dbt-remote project

This package aims to run [dbt][dbt-url] commands remotely on GCP using Cloud Run jobs. To this end, you need to set up a ```dbt-server``` on Cloud Run and install the ```dbt-remote``` cli to run your ```dbt``` commands.

- **Run** ```dbt-remote``` cli (for dbt users): [here](#dbt-remote-cli).
- **Deploy** the ```dbt-server``` (for admins): [here](#dbt-server-admin-use).
- Learn how it works (for developers/curious): [here](#how-it-works)


# dbt-remote cli

This cli aims to run [dbt][dbt-url] commands remotely on GCP. To function, it requires to host a ```dbt-server``` (which will create the Cloud Run jobs).

## Installation

```sh
python3 -m pip install --extra-index-url https://test.pypi.org/simple/ dbt-remote --no-cache-dir
```

## Requirements

**dbt-server.**
Before running ```dbt-remote```, make sure you have at least one running ```dbt-server``` on your GCP project Cloud Run. If no ```dbt-server``` is set up yet, see [dbt-server section][dbt-server-section].

**Where to run the cli?**
To run ```dbt-remote```, you should be in a ```dbt``` project.
This means:
- ```dbt_project.yml``` should be in your current directory 
- ```manifest.json``` should be in ```target/``` (if you renamed this folder, please see [```--manifest``` option](#manifest-and-dbt_project-files)).

Otherwise, you can specify the path to you dbt project using the option ```--project-dir path/to/project```.

Finally you can use specific ```manifest.json``` or ```dbt_project.yml``` files (see how [here](#manifest-and-dbt_project-files)).


## Run dbt-remote cli

```dbt-remote``` cli aims to function almost like ```dbt``` cli. You can run regular ```dbt``` commands such as ```'dbt run'```.
```sh 
dbt-remote run
```
The previous command will use the [automatic server detection](#dbt-server-detection). If you prefer, you can also precise you server url:
```sh 
dbt-remote run --server-url https://<dbt-server-url>
```

## Other options:

You can see all available options using ```dbt-remote --help```.
Below is a quick insight on:
- [dbt-server automatic detection](#dbt-server-detection)
- [--manifest and --dbt-project options](#manifest-and-dbt_project-files)
- [adding dbt packages](#extra-packages)
- [elementary report](#elementary-report)
- [seeds](#seeds-path)
- [profile/target management](#profiletarget-management)
- [configuration](#dbt-remote-configuration)

### **dbt-server automatic detection**

By default, the cli automatically looks for a ```dbt-server``` on your GCP project's Cloud Run. To this end, the cli will fetch information regarding the ```project_id``` and the ```location``` in ```profiles.yml``` if it finds one in the current ```dbt project```.

If the location in ```profiles.yml``` is not the same as the ```dbt-server```'s (typically: location in ```profiles``` is ```US``` while the server runs in ```us-central1```), you need to precise the right location using ```--location```.

Ex: 
```sh
dbt-remote run --select my_model --location europe-west9
```
To save your default location or server url, you can use [dbt config](#dbt-remote-configuration).


### **Manifest and dbt_project files**

To function, the cli sends different files to the ```dbt-server```, including ```manifest.json``` and ```dbt_project.yml```. By default, the cli recompiles the ```manifest.json``` at each execution.

If you do not want the cli to re-compile it, you can use the ```--manifest``` option to indicate a specific ```manifest.json``` file to use.

Similarly, you can specify a ```dbt_project.yml``` file using ```--dbt-project``` option.

Example:
```sh
dbt-remote list --manifest test-files/manifest.json --dbt-project test-files/dbt_project.yml
```

To save your default location or server url, you can use [dbt config](#dbt-remote-configuration).

Be careful: if you already specified a ```project-dir```, the ```manifest``` and ```dbt_project``` paths should be **relative** to the ```project-dir```.

Example:
```
├── folder1
├── folder2       <-- dbt project folder
   ├── macros
   ├── models
   ├── seeds
   ├── ...
   ├── target
   │   ├── manifest.json
   │   ├── ...
   ├── dbt_project.yml
   ├── ...
```
The command is:
```sh
dbt-remote list --project-dir folder2
```
and if you want to specify the ```manifest``` and ```dbt_project``` files:
```sh
dbt-remote list --project-dir folder2 --manifest target/manifest.json --dbt-project dbt_project.yml
```

### **Extra packages:**

By default, the cli do not send the ```packages.yml``` file and the job running on Cloud Run will not install any additional dependency.

If you need to import specific packages, you can use ```--extra-packages``` to specify the ```packages.yml``` file to send. The Cloud Run Job will run ```dbt deps``` at the beginning of its execution to install the packages.

Ex: 
```sh
dbt-remote run --select my_model --extra-packages packages.yml
```

For example if your project uses ```elementary```, you should add this option.


### **Elementary report**

If you want to produce an [elementary][elementary-url] report at the end of the job execution, you can add the ```--elementary``` flag. You may also need to specify ```--extra-packages``` if elementary is not installed on your Cloud Run job by default.

To systematically create report, you can set elementary to True in the config file using [dbt config](#dbt-remote-configuration).


### **Seeds path**

If you run ```seeds``` command, you can specify the path to seeds directory using ```--seeds-path```. By default: ```./seeds/```

Ex: ```dbt-remote seeds --seeds-path test/seeds```*

To save your default seed path, you can use [dbt config](#dbt-remote-configuration).


### **Profile/target management**

Your ```profiles.yml``` may contain several profiles or targets. In the same way as for regular ```dbt``` commands, you can specify ```--profile``` and ```--target```.

> :warning: For ```--profile``` and ```--target```, the job executing the command will **not take into account your local ```profiles.yml```**: it already has a copy of ```profiles.yml``` in its Docker image. If you modify your ```profiles.yml```, you need to update the Docker image (see [dbt-server README][dbt-server-section]).


### **dbt-remote configuration**

TO DO


## More dbt-remote command examples

- run my_model model with Elementary report: 

```sh
dbt-remote --log-level info run --manifest project/manifest.json --select my_model --dbt_project project/dbt_project.yml --extra-packages project/packages.yml --elementary
```


- list with specific profile and target: 

```sh
dbt-remote --log-level debug list --project-dir test/ --profile test_cloud_run --target dev
```


- build with local server url: 

```sh
dbt-remote build --server-url http://0.0.0.0:8001
```


# dbt-server (admin use)

This section is dedicated to ```dbt-server``` deployment and maintenance by system administrators.

```dbt-server``` is a Fastapi server allowing users to run ```dbt``` commands on Cloud Run jobs and to follow their execution in real-time. When the server receives a dbt command request, it creates and launches a job to execute it. The complete workflow is described in the [Architecture schema](./dbt-remote-schema.png).

<center><img src="./dbt-remote-schema-simplified.png" width="80%"></center>

## Requirement (Submit Docker image)

To deploy this server on GCP, you need to first build and deploy the ```Dockerfile``` on your Artifact registry on GCP. To this end, follow these steps:

### 1. Clone repository
Clone this repository and go to the ```dbt-server``` folder.
```sh
git clone git@github.com:artefactory-fr/dbt-server.git
cd dbt-server
```

### 2. Profiles.yml content
Check that ```profiles.yml``` contains the right information regarding your dbt project (```dataset```, ```project```, ```location```). You can also replace this file by your dbt project's ```profiles.yml```.

### 3. Artifact registry creation

If you do not have any artifact registry on GCP yet, [create one][create-artifact-registry]. One way to do this is to use [gcloud cli][gcloud]:
```sh
gcloud artifacts repositories create <REPOSITORY> --repository-format=docker --location=<LOCATION> --description="<DESCRIPTION>"
```
with ```REPOSITORY``` your repository name, ```LOCATION``` the location you want and ```DESCRIPTION``` its description.

Ex:
```sh
gcloud artifacts repositories create dbt-server-repository --repository-format=docker --location=europe-west9 --description="The repository dedicated to dbt-server Docker images."
```

### 4. Build/deploy the Docker image

Build and deploy the ```Dockerfile ``` to the registry. Using [gcloud cli][gcloud]:
```sh
gcloud builds submit --region=<LOCATION> --tag <LOCATION>-docker.pkg.dev/<PROJECT-ID>/<REPOSITORY>/<IMAGE>
```
where:
- ```<LOCATION>``` should be your Artifact Registry's location
- ```<PROJECT-ID>``` is your GCP Project ID
- ```<REPOSITORY>``` is the name of your artifact registry
- ```<IMAGE>``` is the name you want to give to your Docker image

Ex:
```sh
gcloud builds submit --region=europe-west9 --tag europe-west9-docker.pkg.dev/my-project-id/dbt-server-repository/dbt-server
```


## Deployment on GCP

> :warning: **Recommendation**
The dbt-server deployment is easier and quicker using [Terraform][terraform]. We highly recommend to use the following module [dbt-server-terraform-module-repo].

### Resources creation

To run your dbt-server, you need to create the following resources.

- a bucket on Cloud Storage
```sh
gcloud storage buckets create gs://<BUCKET_NAME> --project=<PROJECT_ID> --location=<LOCATION>
```

- a service account with the necessary permissions. Change the value of ```<PROJECT-ID>``` then execute the following code in your terminal:
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
When asked "Allow unauthenticated invocations to [dbt-server-test] (y/N)?", type "y".


### Verify the deployment

To check that your server is properly deployed, you can run
```sh
curl https://<your-server>/check
```
You should receive a response similar to: ```{"response":"Running dbt-server on port 8080"}```.


## Local run / For contributors

You can also run the server locally using ```dbt_server.py```.

**Be careful, this configuration still connects to GCP and expects a environement configuration as well as some cloud resources** (e.g. a Cloud Storage bucket). This means **you must create different GCP resources beforehand**. To this end, we recommend running the ```Terraform``` module or the manual resource creation (see [section above](#deployment-on-gcp)).

Make sure you have sufficient permissions (```roles/datastore.owner```, ```roles/logging.logWriter```, ```roles/logging.viewer```, ```roles/storage.admin```, ```roles/run.developer```, ```roles/iam.serviceAccountUser```) and declare your configuration in your terminal:
```sh
export BUCKET_NAME=<bucket-name>
export DOCKER_IMAGE=<docker-image>
export SERVICE_ACCOUNT=<service-account-email>
export PROJECT_ID=<project-id>
export LOCATION=<location>
```
**Info**: If you used Terraform to create the resources, ```<service-account-email>``` should be ```terraform-job-sa@<project-id>.iam.gserviceaccount.com``` and ```<bucket-name>``` ```dbt-server-test```.

Install the dependencies:
```sh
pip install -r requirements.txt
```
Then launch the ```dbt-server```:
```sh
python3 dbt_server/dbt_server.py --local
```

Your dbt-server should run on ```http://0.0.0.0:8001```.


## Send requests to dbt-server

TO DO


# How it works


<center><img src="./dbt-remote-schema.png" width="80%"></center>

TO DO


[//]: #

   [dbt-url]: <https://www.getdbt.com/>
   [elementary-url]: <https://www.elementary-data.com/>
   [terraform]: <https://www.terraform.io/>

   [gcloud]: <https://cloud.google.com/sdk/docs/install>
   [create-artifact-registry]: <https://cloud.google.com/artifact-registry/docs/repositories/create-repos>
   

   [dbt-server-repo-url]: <https://github.com/artefactory-fr/dbt-server>
   [dbt-server-section]: #dbt-server-admin-use
   
   [dbt-server-terraform-module-repo]: <https://github.com/artefactory-fr/terraform-gcp-dbt-server/tree/add-dbt-server-terraform-code>
   
