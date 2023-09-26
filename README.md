# dbt-remote project

This package aims to run [dbt][dbt-url] commands remotely using jobs (e.g. on Cloud Run jobs on GCP). To this end, you need to set up a ```dbt-server``` and install the ```dbt-remote``` cli to run your ```dbt``` commands.

- **Deploy** the ```dbt-server``` (for admins): ```README.md``` in ```api/``` folder.
- **Run** ```dbt-remote``` cli (for dbt users): ```README.md``` in ```package/``` folder.
