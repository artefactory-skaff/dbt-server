echo Export the project id and location
export PROJECT_ID=<your-project-id> &&
export LOCATION=europe-west9

echo Create artifact registry
gcloud artifacts repositories create dbt-server-repository --repository-format=docker --location=$LOCATION --description="Used to host the dbt-server docker image. https://github.com/artefactory-fr/dbt-server"

echo Create storage bucket
gcloud storage buckets create gs://$PROJECT_ID-dbt-server --project=$PROJECT_ID --location=$LOCATION

echo Create a service account that will be used for dbt runs
gcloud iam service-accounts create dbt-server-service-account --project=${PROJECT_ID};

echo Assign roles to the SA
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
  --member=serviceAccount:dbt-server-service-account@${PROJECT_ID}.iam.gserviceaccount.com \
  --role=roles/${ROLE};
done

echo Enable GCP APIs
gcloud services enable \
    firestore.googleapis.com \
    run.googleapis.com \
    --project=$PROJECT_ID

echo 'Create Firestore database (default) if it does not exist'
database=$(gcloud firestore databases list | grep "projects/${PROJECT_ID}/databases/(default)")
if [ -z "$database" ]
then
   echo "(default) database does not exist, creating one...";
   gcloud firestore databases create --location=nam5;
   echo "Created";
else
   echo "(default) database already exists";
fi

echo Build the server image
gcloud builds submit ./dbt_server/ --region=$LOCATION --tag $LOCATION-docker.pkg.dev/$PROJECT_ID/dbt-server-repository/dbt-server-image

echo Deploy the server
gcloud run deploy dbt-server \
	--image ${LOCATION}-docker.pkg.dev/${PROJECT_ID}/dbt-server-repository/dbt-server-image \
	--platform managed \
	--region ${LOCATION} \
	--service-account=dbt-server-service-account@${PROJECT_ID}.iam.gserviceaccount.com \
	--set-env-vars=BUCKET_NAME=${PROJECT_ID}-dbt-server \
	--set-env-vars=DOCKER_IMAGE=${LOCATION}-docker.pkg.dev/${PROJECT_ID}/dbt-server/dbt-server-image \
	--set-env-vars=SERVICE_ACCOUNT=dbt-server-service-account@${PROJECT_ID}.iam.gserviceaccount.com \
	--set-env-vars=PROJECT_ID=${PROJECT_ID} \
	--set-env-vars=LOCATION=${LOCATION} \
    --no-allow-unauthenticated