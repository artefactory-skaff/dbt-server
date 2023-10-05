# dbt-remote

This cli aims to be a drop-in replacement for the [dbt][dbt-url] CLI, but running on GCP. 

## Requirements and installation

See repository's [README][README].



## Some cli options:

You can see all available options using `dbt-remote --help`.
We detail some subtleties of 2 options:

- [dbt-server automatic detection](#dbt-server-detection)
- [--manifest and --dbt-project options](#manifest-and-dbt_project-files)

### **dbt-server automatic detection**

By default, the cli automatically looks for a `dbt-server` on your GCP project's Cloud Run. To this end, the cli uses the `location` given in the user command or set in the configuration. If none is given, it looks at all EU and US locations.

Ex: 
```sh
dbt-remote run --select my_model --location europe-west9
```
To save your default location or server url, you can use `dbt-remote config`.


### **manifest.json and dbt_project.yml files**

To function, the cli sends different files to the `dbt-server`, including `manifest.json` and `dbt_project.yml`. By default, the cli recompiles the `manifest.json` at each execution.

If you do not want the cli to re-compile it, you can use the `--manifest` option to indicate a specific `manifest.json` file to use.

Similarly, you can specify a `dbt_project.yml` file using `--dbt-project` option.

Example:
```sh
dbt-remote list --manifest test-files/manifest.json --dbt-project test-files/dbt_project.yml
```

To save your default manifest and dbt_project files, you can use `dbt-remote config`.

> :warning: if you already specified a `project-dir`, the `manifest` and `dbt_project` paths should be **relative** to the `project-dir`.



## dbt-remote command examples

- run my_model model with Elementary report: 

```sh
dbt-remote --log-level info run --manifest project/manifest.json --select my_model --dbt_project project/dbt_project.yml --extra-packages project/packages.yml --elementary
```

- list with specific profile and target: 

```sh
dbt-remote --log-level debug list --project-dir test/ --profile my_profile --target dev
```

- build with local server url: 

```sh
dbt-remote build --server-url http://0.0.0.0:8001
```

## Troubleshooting

Please contact me (emma.galliere@artefact.com)

## How does dbt-remote cli work ?

> To learn about the complete process, take a look at the [Explanation](explanation.md) page.

The cli does 3 things:

- it can detect your dbt-server. As explained in [Explanation](explanation.md):

> The cli detects the dbt-server. To this end, it invokes the automatic server detection (see `dbt_remote/src/dbt_remote/dbt_server_detector.py`). Using the given location, the cli sends a request to Cloud Run to list all available services, then tries to ping each service on the `/check` endpoint. If a dbt-server is running on this location, the cli should receive a message similar to `{"response":"Running dbt-server on port 8001"}`.

- it sends POST requests to the dbt-server with all necessary information to execute the dbt command.

- it streams the execution logs to allow the user to follow this execution in real-time. To this end, the cli asks the server for the job status and logs, and display the latter. As long as the job is not finished, it starts again.

A simplified version of dbt-remote cli interactions with the dbt-server is presented on [this image](images/dbt-remote-cli-workflow-simplified.png).

![dbt-remote-cli-workflow](images/dbt-remote-cli-workflow-simplified.png)



[//]: #

   [dbt-url]: <https://www.getdbt.com/>
   [elementary-url]: <https://www.elementary-data.com/>
   [bigquery-api]: <https://console.cloud.google.com/marketplace/product/google/bigquery.googleapis.com>

   [dbt-server-section]: dbt_server.md
   [dbt_server_detector]: ../dbt_remote/src/dbt_remote/dbt_server_detector.py

   [README]: <https://github.com/artefactory-fr/dbt-server/blob/main/README.md>
