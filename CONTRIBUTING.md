# Contributing to dbt-remote

First off, thanks for taking the time to contribute!

## Setup

### Environment
To set up the project, we recommend using Poetry since it will be required for changes submission and publication. The steps are the following:

1. **Clone the repository**
2. **Install poetry** ([installation guide](https://python-poetry.org/docs/))
3. At the root of the project, **run**:
```shell
poetry lock -n; poetry install;
```
Poetry should install the right packages (listed in ```pyproject.toml```).

Export the project ID and location of your dbt-server
```shell
export PROJECT_ID=<PROJECT_ID>
export LOCATION=<LOCATION>
```

### Deploy the dbt-server
Follow the dbt-server deployment instructions here: [dbt-server deployment guide](../dbt_server/README.md)

### Run end-to-end tests to make sure everyting is properly setup
Go to the testing directory that contains a dbt project.
```shell
cd tests/dbt_project
```

From there, run the tests. This should take a few minutes.
```shell
poetry run pytest .. -log_cli=true -log_cli_level=info -vvv --maxfail=1
```

Once everything here is green, your are good to go.
## **Workflow for developing on the dbt-server**

To run your dbt-server in local, **you must first create all the required resources on GCP** (see the README's section 'dbt-server'). Then export your environment variables using your GCP project and newly-created resources:
```sh
export BUCKET_NAME=<bucket-name>
export DOCKER_IMAGE=<docker-image>
export SERVICE_ACCOUNT=<service-account-email>
export PROJECT_ID=<project-id>
export LOCATION=<location>
```
Finally you can run your server:
```sh
cd dbt_server
poetry run python3 dbt_server.py
```
> Note: This server is a Fastapi server running on 8001, you can change this set up at the end of the ```dbt_server.py``` file.

## **Workflow for working on the dbt-remote**

To build and install your own version of the package, you can run (at the root of the project):

```sh
poetry build
poetry run pip install dist/gcp_dbt_remote-X.Y.Z.tar.gz # <-- change X.Y.Z by your version
dbt-remote --help
```

## Test the project

### **Test the dbt-server**

```sh
cd dbt_server
poetry run pytest tests/unit
```

All tests should pass by your will get a TypeError due to ```google.cloud.logging.Worker```. Bug to fix.

> Note: Since some Google libraries raise many DeprecationWarning errors regarding namespace packages, you may want to add ```-W ignore::DeprecationWarning``` to your command

### **Test dbt-remote**

```sh
poetry run pytest dbt_remote/tests/unit
```

The ```dbt_remote/tests/integration``` contains one file with integration tests but it is not finished and it needs to be reviewed.

## Report a bug

**Before submitting a bug report**

Check the troubleshooting section in the README.

**Submit a bug report**

Explain the problem as clearly as possible and include additional details to help maintainers reproduce the problem. Ex:

- Use a clear and descriptive title for the issue to identify the problem.
- Describe the exact steps which reproduce the problem in as many details as possible.
- Provide specific examples to demonstrate the steps. If possible, include links to files or GitHub projects, or copy/pasteable snippets, which you use in those examples. If you're providing snippets in the issue, use Markdown code blocks.
- Describe the behavior you observed after following the steps and point out what exactly is the problem with that behavior.
- Explain which behavior you expected to see instead and why.
- Specify your environment (which version of the tool you are using, which OS, packages versions)
- If the problem wasn't triggered by a specific action, describe what you were doing before the problem happened.


## Submit changes

This section describes the procedure for submitting any type of change, be it a bug fix or a new feature.

### Before submitting a change

Take a look at the project guidelines in [dbt-remote project page](index.md) to make sure your change is aligned with the project.

### Submitting a change

- Explain your change: Give a precise description of what you would change in the project behavior and why it would be useful

- Validate the change with the owners

- Code your change following the [coding convention](#coding-convention)

- Create a PR and wait for validation

### Publishing a change

>**First contribution**
> Create an account on Pypi (or TestPypi). Get your Pypi token. Ask Emma Gallière to add you as collaborator on the project

- **If not done yet**, add Test Pypi as repository
```sh
poetry config repositories.testpypi https://test.pypi.org/legacy/
```

- Check the tests pass:
```sh
poetry run pytest dbt_remote/tests/unit/
```
and
```sh
cd dbt_server; poetry run pytest tests/unit
```

- Increment the package version in `pyproject.toml`

- Build the package
```sh
poetry build
```

- Publish the package
```sh
token=$(cat <your-pypi-token.txt>)
poetry publish --repository testpypi -u __token__ -p $token --skip-existing
```

### Coding convention

- Follow Clean Code good practices as much as possible
- Be vigilant with your naming convention (variable, functions, etc.) so that it is as clear as possible
- Add type hinting for every function
- ...

**Dependencies:** you can't change dbt-bigquery version without discussing it with the team for retro compatibility reasons. For other dependencies, make sure all the tests pass before submitting your change.


## I have a question/need help with the project

Contact: emma.galliere@artefact.com


## Contributors
