This package aims to run [dbt][dbt-url] commands remotely (e.g. on Cloud Run jobs on GCP). To function, it requires to host a ```dbt-server``` (which will create the Cloud Run jobs) and to install the cli (which will send the user commands to the ```dbt-server```).

# dbt-remote cli

## Installation

```python3 -m pip install --index-url https://test.pypi.org/simple/ dbt-remote ```

## Before running

**dbt-server.**
Before running ```dbt-remote```, make sure you have at least one running ```dbt-server``` on your GCP project Cloud Run. If no ```dbt-server``` is set up yet, see [dbt-server README][dbt-server-section].

**dbt project.**
To run ```dbt-remote```, you should be in a ```dbt``` project.

```
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
This means:
- ```dbt_project.yml``` should be in your current directory 
- ```manifest.json``` should be in ```target/``` (if you renamed this folder, please see [```--manifest``` option](#manifest-and-dbt_project-files)).

Otherwise, you can specify the path to you dbt project using the option ```--project-dir path/to/project```.

Finally you can use specific ```manifest.json``` or ```dbt_project.yml``` files (see how [here](#manifest-and-dbt_project-files)).


## Run dbt-remote cli

You can run regular ```dbt``` commands such as ```'dbt list'```.
```sh 
dbt-remote list
```
The previous command will use the [automatic server detection](#dbt-server-detection). If you prefer, you can also precise you server url:
```sh 
dbt-remote list --server-url https://<dbt-server-url>
```

## Other options:

You can see all available options using ```dbt-remote --help```.
Below is a quick deep-dive in:
- [dbt-server automatic detection](#dbt-server-detection)
- [--manifest and --dbt-project options](#manifest-and-dbt_project-files)
- [adding dbt packages](#extra-packages)
- [elementary report](#elementary-report)
- [seeds](#seeds-path)
- [profile/target management](#profiletarget-management)

### **dbt-server detection**

You can omit your ```dbt-server``` url. The cli automatically looks for a ```dbt-server``` on your GCP project's Cloud Run.
To this end, the cli will fetch information regarding the ```project_id``` and the ```location``` in ```profiles.yml``` if it finds one in the current ```dbt project```.

If the location in ```profiles.yml``` is not the same as the ```dbt-server```'s (typically: location in ```profiles``` is ```US``` while the server runs in ```us-central1```), you need to precise the right location using ```--location```.

Ex: ```dbt-remote run --select my_model --location europe-west9```

### **Manifest and dbt_project files**

If you do not want the cli to re-compile the ```manifest``` before sending the request to the ```dbt server```, you can use the ```--manifest``` option to indicate a specific ```manifest.json``` file to use.

Similarly, you can specify a ```dbt_project.yml``` file using ```--dbt-project``` option.

Example:
```sh
dbt-remote list --manifest test-files/manifest.json --dbt-project test-files/dbt_project.yml
```

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
The command is
```sh
dbt-remote list --project-dir folder2
```
and if you want to specify the ```manifest``` and ```dbt_project``` files:
```sh
dbt-remote list --project-dir folder2 --manifest target/manifest.json --dbt-project dbt_project.yml
```

### **Extra packages:**

If you need to import specific packages, you can specify the ```packages.yml``` file to load using ```--extra-packages```. The Cloud Run Job will run ```dbt deps``` at the beginning of its execution to install the packages.

Ex: ```dbt-remote run --select my_model --extra-packages packages.yml```

For example if your project uses ```elementary```, you should add this option.


### **Elementary report**

If you want to produce an [elementary][elementary-url] report at the end of the run, you can add the ```--elementary``` flag. You may also need to specify ```--extra-packages``` if elementary is not installed on your Cloud Run job by default.


### **Seeds path**

If you run ```seeds``` command, you can specify the path to seeds directory using ```--seeds-path```. By default: ```./seeds/```

Ex: ```dbt-remote seeds --seeds-path test/seeds```


### **Profile/target management**

Your ```profiles.yml``` may contain several profiles or targets. In the same way as for regular ```dbt``` commands, you can specify ```--profile``` and ```--target```.

> :warning: For ```--profile``` and ```--target```, the job executing the command will **not take into account your local ```profiles.yml```**: it already has a copy of ```profiles.yml``` in its Docker image. If you modify your ```profiles.yml```, you need to update the Docker image (see [dbt-server README][dbt-server-section]).


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

[//]: #

   [git-repo-url]: <https://github.com/artefactory-fr/dbt-server>
   [dbt-url]: <https://www.getdbt.com/>
   [elementary-url]: <https://www.elementary-data.com/>
   [dbt-server-section]: <https://github.com/artefactory-fr/dbt-server/blob/diff-pr/api/README.md>
