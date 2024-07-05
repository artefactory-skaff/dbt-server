import json
from pathlib import Path

import click

from dbtr.common.exceptions import AzureDeploymentFailed, MissingExtraPackage

try:
    from azure.identity import AzureCliCredential, DefaultAzureCredential
    from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
    from azure.mgmt.resource.resources.models import DeploymentMode
except ImportError:
    raise MissingExtraPackage("dbtr is not installed with Azure support. Please install with `pip install dbtr[azure]`.")


def deploy(image: str, service_name: str, location: str, adpater: str, resource_group: str, log_level: str = "INFO"):
    credential = AzureCliCredential()
    subscription_client = SubscriptionClient(credential)
    subscription = next(subscription_client.subscriptions.list())
    subscription_id = subscription.subscription_id
    resource_client = ResourceManagementClient(credential, subscription_id)

    with open(Path(__file__).parent / "arm_templates" / "azuredeploy.json", "r") as template_file:
        template_body = json.load(template_file)

    sanitized_name = "".join(filter(str.isalpha, service_name)).lower()

    click.echo(f"Deploying dbt server '{service_name}' to Azure in resource group '{resource_group}'. This may take a few minutes.")
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
