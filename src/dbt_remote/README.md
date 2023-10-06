This package aims to run [dbt][dbt-url] commands remotely. To function, it requires to host a ```dbt-server``` and to install the cli (which will send the user commands to the ```dbt-server```).

# dbt-remote cli

## Installation

```sh
pip install dbt-remote
```

## Run dbt-remote

### Pre-requisites

**An existing dbt-server deployment**

Before running ```dbt-remote```, make sure you have at least one running ```dbt-server```. If no ```dbt-server``` is set up yet, see #dbt-server section.

**Any dbt project**

To run ```dbt-remote```, you should be in a dbt project: ```dbt_project.yml``` should be in your current directory and ```manifest.json``` should be in ```./target```. Otherwise, you can specify the path to a dbt project using ```--project-dir path/to/project```. Finally, if needed, you can specify path to ```manifest.json``` and ```dbt_project.yml``` :

```sh
dbt-remote run --manifest test-files/manifest.json --select my_model --dbt_project test-files/dbt_project.yml
```

Be careful: if you already specified a ```project-dir```, the ```manifest``` and ```dbt_project``` paths should be **relative** to the ```project-dir```.

### Run the cli

Then run regular ```dbt``` commands such as ```dbt list``` without forgetting to add your ```dbt-server``` url.
```sh
dbt-remote list --server-url <dbt-server-url>
```

## Other options:

You can see all available options using ```dbt-remote --help```.


**Extra packages:**

If you need to import specific packages, you can specify the ```package.yml``` file to load using ```--extra-packages```. The job will run ```dbt deps``` at the beginning of its execution to install the packages.

Ex: ```dbt-remote run --select orders --extra-packages packages.yml```

For example if your project uses ```elementary```, you should add this option.


**Elementary**

If you want to produce an [elementary][elementary-url] report at the end of the run, you can add the ```--elementary``` flag. You will also need to specify ```--extra-packages```.


**Seeds path:**

If you run ```seeds``` command, you can specify the path to seeds directory using ```--seeds-path```. By default: ```./seeds/```

Ex: ```dbt-remote seeds --seeds-path ./other_seeds/```


**Profile/target management**

Your ```profiles.yml``` may contain several profiles or target. In the same way as for dbt, you can specify ```--profile``` and ```--target```.

## More dbt-remote command examples

- run the jaffle_shop's ```orders``` model with Elementary report:

```sh
dbt-remote --log-level info run --manifest project/manifest.json --select orders --dbt_project project/dbt_project.yml --extra-packages project/packages.yml --elementary
```

- list with specific profile and target:

```sh
dbt-remote --log-level debug list --project-dir my_dbt_project/ --profile my_profile --target dev
```

- build with a local server url:

```sh
dbt-remote build --server-url http://0.0.0.0:8001
```

[//]: #

   [git-repo-url]: <https://github.com/artefactory-fr/dbt-server>
   [dbt-url]: <https://www.getdbt.com/>
   [elementary-url]: <https://www.elementary-data.com/>
