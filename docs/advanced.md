# Advanced usage

## Send requests to dbt-server

The following requests allow you to send commands to your server using `curl` rather than the cli.

- [Check your dbt-server](#check-dbt-server)
- [Send dbt commands](#post-dbt-command)
- [Follow your job execution](#follow-the-job-execution)

> **Authentication.**
> To comply with the server authentication, these requests include 2 lines to fetch an id_token using `gcloud` and to add it to the request's headers:
>```sh
>my_token=$(gcloud auth print-identity-token)
>header="Authorization: Bearer $my_token"
>```
>If your server does not require authentication, you can remove this part (as well as the `-H "$header"` from the curl commands).

### Check dbt-server

This command just check if your dbt-server is up and running. The response should be similar to `{"response":"Running dbt-server on port 8001"}`.

```sh
my_server=https://<SERVER-URL>
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

curl -H "$header" $my_server/check
```

### Post dbt command

To `POST` a dbt command to the server, we need to send files (`manifest.json`, `dbt_project.yml`, `profiles.yml` and possibly more). Since some files can be quite large (`manifest.json` in particular), we encode them using base64 and we temporarily store the request's body in a `data.json` file. Then we use the `--data-binary` curl option to send our request.

Before sending these requests, replace `<SERVER-URL>` by your server url and make sure the files' paths are the right ones (`dbt_project.yml`, `manifest.json`, etc.).


**`dbt list` command:**
```sh
my_server="https://<SERVER-URL>"

my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

dbt_project=$(base64 -i dbt_project.yml); # <-- should be your path to your dbt_project.yml file
manifest=$(base64 -i target/manifest.json); # <-- same for your manifest.json file
profiles=$(base64 -i profiles.yml);  # <-- same for your profiles.yml file

echo '{"server_url":"'$my_server'", "user_command":"list", "manifest": "'$manifest'", "dbt_project":"'$dbt_project'", "profiles":"'$profiles'"}' > data.json;

curl --data-binary @data.json -H "$header" -H "Content-Type: application/json" -X POST $my_server/dbt
```

**`dbt run` a specific model with Elementary package and report:**

Replace `<MODEL>` by one of your models.

```sh
my_server="https://<SERVER-URL>"

my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

dbt_project=$(base64 -i dbt_project.yml); # <-- should be your path to your dbt_project.yml file
manifest=$(base64 -i target/manifest.json); # <-- same for your manifest.json file
profiles=$(base64 -i profiles.yml);  # <-- same for your profiles.yml file
packages=$(base64 -i packages.yml); # <-- same for your packages.yml file

echo '{"server_url":"'$my_server'", "user_command":"run --select <MODEL>", "manifest": "'$manifest'", "dbt_project":"'$dbt_project'", "profiles":"'$profiles'", "packages":"'$packages'", "elementary":"True"}' > data.json;

curl --data-binary @data.json -H "$header" -H "Content-Type: application/json" -X POST $my_server/dbt
```

**`dbt seed` with one particular seed file (country_code.csv):**
```sh
my_server="https://<SERVER-URL>"

my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"

dbt_project=$(base64 -i dbt_project.yml); # <-- should be your path to your dbt_project.yml file
manifest=$(base64 -i target/manifest.json); # <-- same for your manifest.json file
profiles=$(base64 -i profiles.yml);  # <-- same for your profiles.yml file
packages=$(base64 -i packages.yml); # <-- same for your packages.yml file
seed_file=$(base64 -i seeds/country_codes.csv); # <-- same for your seed file
seeds='{"seeds/country_codes.csv":"'$seed_file'"}'; # <-- don't forget to change the file name

echo '{"server_url":"'$my_server'", "user_command":"seed", "manifest": "'$manifest'", "dbt_project":"'$dbt_project'", "profiles":"'$profiles'", "packages":"'$packages'", "seeds":'$seeds'}' > data.json;

curl --data-binary @data.json -H "$header" -H "Content-Type: application/json" -X POST $my_server/dbt
```

### Follow the job execution

Replace `<UUID>` by your job's UUID. ex: `d710dc13-6175-4735-8649-31c39a4a0e90`.

**Get job run status:**

```sh
my_server="https://<SERVER-URL>"
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"
my_job_uuid=<UUID>

curl -H "$header" $my_server/job/$my_job_uuid
```

**Get job logs:**
```sh
my_server="https://<SERVER-URL>"
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"
my_job_uuid=<UUID>

curl -H "$header" $my_server/job/$my_job_uuid/logs
```

**Get elementary report:** (at the end of the execution)
```sh
my_server="https://<SERVER-URL>"
my_token=$(gcloud auth print-identity-token)
header="Authorization: Bearer $my_token"
my_job_uuid=<UUID>

curl -H "$header" -L $my_server/job/$my_job_uuid/report
```
> Note: the `-L` is necessary because the `/report` endpoint is a redirection to the GCS report url.

