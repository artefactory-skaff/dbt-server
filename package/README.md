This package aims to run [dbt][dbt-url] commands on a remote server hosted on GCP. To function, it requires to host a dbt-server and to install the cli

# dbt-remote cli

## Installation

```python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps dbt-remote ```

## Run dbt-remote

### Before running

Before running ```dbt-remote```, make sure you have at least one running ```dbt-server``` of your GCP project Cloud Run (it should be in the same project as the BigQuery data you want to process). If not, see set up [here](../api/README.md#dbt-server).

Make sure you are in a dbt project (```dbt_project.yml``` should be in your current directory and ```manifest.json``` should be in ```./target```). Otherwise, you can specify the path to a dbt project using ```--project-dir path/to/project```. Finally, if needed, you can specify path to ```manifest.json``` and ```dbt_project.yml``` : 

```sh
dbt-remote run --manifest test-files/manifest.json --select my_model --dbt_project test-files/dbt_project.yml
```

Be careful: if you already specified a ```project-dir```, the ```manifest``` and ```dbt_project``` paths should be **relative** to the ```project-dir```)



Then run ```dbt``` commands such as:
```sh 
dbt-remote list --location europe-west9
```

```--location``` corresponds to the ```dbt-server``` location.


### Other options:

**Server url:**

You can specify your ```dbt-server``` url using ```--server-url```.

Ex: ```dbt-remote run --select vbak --server-url https://my-server.com```

If not given, the cli automatically looks for a ```dbt-server``` on your GCP project's Cloud Run.
To this end, the cli will fetch information (regarding the ```project_id``` and the ```location```) in ```profiles.yml``` if it finds one in the ```dbt project```.


**Extra packages:**

If you need to import specific packages, you can specify the file to load using ```--extra-packages```. The Cloud Run Jobs will run ```dbt deps``` at the beginning of its execution.

Ex: ```dbt-remote run --select vbak --extra-packages packages.yml```

For example if your project uses ```elementary```, you should add this option.


**Elementary**

If you want to produce an elementary report at the end of the run, you can add the ```--elementary``` flag. You may also need to specify ```--extra-packages``` if elementary is not install on your Cloud Run job by default (check ```requirements.txt```).


**Seeds path:**

If you run ```seeds``` command, you can specify the path to seeds directory using ```--seeds-path```. By default: ```./seeds/```

Ex: ```dbt-remote seeds --seeds-path test/seeds```



## More dbt-remote command examples

- run --select with elementary: 

```sh
dbt-remote --log-level info run --manifest project/manifest.json --select vbak_dbt --dbt_project project/dbt_project.yml --extra-packages project/packages.yml --elementary
```


- list with specific profile and target: 

```sh
dbt-remote --log-level debug list --project-dir test/ --profile test_cloud_run --target dev
```


- build with local server url: 

```sh
dbt-remote build --server-url http://0.0.0.0:8001
```

[//]: #

   [git-repo-url]: <https://github.com/artefactory-fr/dbt-server>
   [dbt-url]: <https://www.getdbt.com/>