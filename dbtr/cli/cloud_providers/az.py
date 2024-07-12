import json
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from dbtr.common.exceptions import AzureDeploymentFailed, MissingExtraPackage

try:
    from azure.identity import AzureCliCredential, DefaultAzureCredential
    from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
    from azure.mgmt.resource.resources.models import DeploymentMode
except ImportError:
    raise MissingExtraPackage("dbtr is not installed with Azure support. Please install with `pip install dbtr[azure]`.")


def deploy(image: str, service_name: str, location: str, adpater: str, resource_group: str, log_level: str = "INFO", auto_approve: bool = False):
    console = Console()
    table = Table(title="Deploying dbt server on Azure with the following configuration")

    table.add_column("Configuration", justify="right", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    table.add_row("Resource Group", resource_group)
    table.add_row("Service Name", service_name)
    table.add_row("Location", location)
    table.add_row("Adapter", adpater)
    table.add_row("Image", image)
    table.add_row("Log Level", log_level)

    console.print(table)
    if not auto_approve:
        click.confirm("Confirm deployment?", abort=True)

    credential = AzureCliCredential()
    subscription_client = SubscriptionClient(credential)
    subscription = next(subscription_client.subscriptions.list())
    subscription_id = subscription.subscription_id
    resource_client = ResourceManagementClient(credential, subscription_id)

    with open(Path(__file__).parent / "arm_templates" / "azuredeploy.json", "r") as template_file:
        template_body = json.load(template_file)

    sanitized_name = "".join(filter(str.isalpha, service_name)).lower()

    deployment_poller = resource_client.deployments.begin_create_or_update(
        resource_group,
        "dbtServerDeployment",
        {
            "properties": {
                "template": template_body,
                "parameters": {
                    "name": {
                        "value": service_name
                    },
                    "nameSanitized": {
                        "value": sanitized_name
                    },
                    "location": {
                        "value": location
                    },
                    "image": {
                        "value": image
                    },
                    "envVars": {
                        "value": [
                            {
                                "name": "LOG_LEVEL",
                                "value": log_level
                            },
                            {
                                "name": "ADAPTER",
                                "value": adpater
                            },
                            {
                                "name": "PROVIDER",
                                "value": "azure"
                            }
                        ]
                    },
                    "secrets": {  # TODO
                        "value": {
                            "arrayValue": []
                        }
                    }
                },
                "mode": DeploymentMode.incremental
            }
        }
    )


    try:
        deployment_poller.result()
        print("Azure deployment succeeded.")
    except Exception as e:
        raise AzureDeploymentFailed(f"Deployment failed:\n{e}")


def get_auth_token(*args, **kwargs):
    return DefaultAzureCredential().get_token("https://management.azure.com/.default").token
