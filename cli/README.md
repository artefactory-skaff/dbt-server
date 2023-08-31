# dbt-remote cli

## Installation

In ```cli``` directory, execute:
```poetry run python3 -m pip install --editable .```

## Run dbt-remote

Make sure you are in a dbt project (```dbt_project.yml``` should be in your current directory and ```manifest.json``` should be in ```./target```)
Then run commands like ```poetry run dbt-remote run --select vbak_dbt```.

Otherwise, you can specify the path to a dbt project using ```--project-dir path/to/project```. Finally, if needed, you can specify path to ```manifest.json``` and ```dbt_project.yml``` : ```poetry run dbt-remote run --manifest ../test-files/manifest.json --select vbak_dbt --dbt_project ../test-files/dbt_project.yml```. (be careful: if you already  specified a project-dir, manifest and dbt_project paths should be relative to the project-dir)


### Other options:

Server url:
You can specify you dbt-server url using ```--server-url```. If not given, the cli automatically look for a dbt-server on your GCP project's Cloud Run.
To this end, the cli will fetch information in ```profiles.yml``` if it finds one in the dbt project.
Ex: ```poetry run dbt-remote run --select vbak --server-url https://my-server.com```

Extra packages:
If you need to import specific packages, you can specify the file to load using ```--extra-packages```. The Cloud Run Jobs will run ```dbt deps``` at the beginning of its execution.
Ex: ```poetry run dbt-remote run --select vbak --extra-packages packages.yml```

Elementary
If you want to produce an elementary report at the end of the run, you can add the ```--elementary``` flag. You may also need to specify ```--extra-packages``` if elementary is not install on your Cloud Run job by default (check ```requirements.txt```).

Seeds path:
You can specify the path to seeds directory using ```--seeds-path```. By default: ```./seeds/```
Ex: ```poetry run dbt-remote seeds --seeds-path test/seeds```



## Examples

- run --select with elementary: ```poetry run dbt-remote --log-level info run --manifest ../test-files/elementary/manifest.json --select vbak_dbt --dbt_project ../test-files/elementary/dbt_project.yml --extra-packages ../test-files/elementary/packages.yml --elementary```

- list with specific profile and target: ```poetry run dbt-remote --log-level debug list --project-dir test/ --profile test_cloud_run --target dev```

- build with local server url: ```poetry run dbt-remote build --project-url http://0.0.0.0:8001```