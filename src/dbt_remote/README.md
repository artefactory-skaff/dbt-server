This package aims to run [dbt][dbt-url] commands remotely (on Cloud Run jobs on GCP). To function, it requires to host a ```dbt-server``` (which will create the Cloud Run jobs) and to install the cli (which will send the user commands to the ```dbt-server```).

# dbt-remote cli

## Installation

```python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps dbt-remote ```

## Run dbt-remote

### Before running

**dbt-server**
Before running ```dbt-remote```, make sure you have at least one running ```dbt-server``` on your GCP project Cloud Run. If no ```dbt-server``` is set up yet, see #dbt-server section.

**dbt project**
To run ```dbt-remote```, you should be in a dbt project: ```dbt_project.yml``` should be in your current directory and ```manifest.json``` should be in ```./target```. Otherwise, you can specify the path to a dbt project using ```--project-dir path/to/project```. Finally, if needed, you can specify path to ```manifest.json``` and ```dbt_project.yml``` :

```sh
dbt-remote run --manifest test-files/manifest.json --select my_model --dbt_project test-files/dbt_project.yml
```

Be careful: if you already specified a ```project-dir```, the ```manifest``` and ```dbt_project``` paths should be **relative** to the ```project-dir```.

### Run the cli

Then run regular ```dbt``` commands such as ```dbt list``` and precise you ```dbt-server``` url.
```sh
dbt-remote list --server-url <dbt-server-url>
```

## Other options:

You can see all available options using ```dbt-remote --help```.

**dbt-server detection**

You can omit your ```dbt-server``` url. The cli automatically looks for a ```dbt-server``` on your GCP project's Cloud Run.
To this end, the cli will fetch information regarding the ```project_id``` and the ```location``` in ```profiles.yml``` if it finds one in the current ```dbt project```.

If the location in ```profiles.yml``` is not the same as the ```dbt-server```'s (typically: location in ```profiles``` is US while the server runs in ```us-central1```), you need to precise the right location using ```--location```.

Ex: ```dbt-remote run --select vbak --location europe-west9```


**Extra packages:**

If you need to import specific packages, you can specify the ```packagse.yml``` file to load using ```--extra-packages```. The Cloud Run Job will run ```dbt deps``` at the beginning of its execution to install the packages.

Ex: ```dbt-remote run --select vbak --extra-packages packages.yml```

For example if your project uses ```elementary```, you should add this option.


**Elementary**

If you want to produce an [elementary][elementary-url] report at the end of the run, you can add the ```--elementary``` flag. You may also need to specify ```--extra-packages``` if elementary is not install on your Cloud Run job by default.


**Seeds path:**

If you run ```seeds``` command, you can specify the path to seeds directory using ```--seeds-path```. By default: ```./seeds/```

Ex: ```dbt-remote seeds --seeds-path test/seeds```


**Profile/target management**

Your ```profiles.yml``` may contain several profiles or target. In the same way as for ```dbt```, you can specify ```--profile``` and ```--target```.

> :warning: For ```--profile``` and ```--target```, the job executing the command will not take into account your local ```profiles.yml``` (it already has a copy of ```profiles.yml``` in its Docker image). If you modify your ```profiles.yml```, you need to update the Docker image (see #dbt-server deployment).


## More dbt-remote command examples

- run vbak_dbt model with Elementary report:

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
   [elementary-url]: <https://www.elementary-data.com/>
