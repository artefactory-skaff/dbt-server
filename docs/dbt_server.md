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



[//]: #


   [gcloud]: <https://cloud.google.com/sdk/docs/install>
   [create-artifact-registry]: <https://cloud.google.com/artifact-registry/docs/repositories/create-repos>
   [terraform]: <https://www.terraform.io/>

   [dbt-server-terraform-module-repo]: <https://github.com/artefactory-fr/terraform-gcp-dbt-server/tree/add-dbt-server-terraform-code>

   [README]: <https://github.com/artefactory-fr/dbt-server/blob/main/README.md>
