import typing
from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
    aws_iam
)
import aws_cdk as cdk
import aws_cdk.aws_ecs as ecs
import aws_cdk.aws_ecs_patterns as ecsp
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_efs as efs
import aws_cdk.aws_iam as iam
import aws_cdk.aws_elasticloadbalancingv2 as elbv2
from aws_cdk.aws_autoscaling import AutoScalingGroup
from aws_cdk import aws_elasticloadbalancingv2_targets as elasticloadbalancingv2_targets
import aws_cdk.aws_apigateway as apigateway
from constructs import Construct

class EcsStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        subnet_configuration = ec2.SubnetConfiguration(
                name="dbtr-subnets",
                subnet_type=ec2.SubnetType.PUBLIC,
                reserved=False
            )
        vpc = ec2.Vpc(self, "dbtr-vpc", vpc_name="dbtr-vpc", subnet_configuration=[subnet_configuration]) #vpc-0f763d4efd233d795
        subnets = ec2.SubnetSelection(subnets=vpc.public_subnets)

        sg = ec2.SecurityGroup(self, "dbtr-sg", vpc=vpc, security_group_name="dbtr-sg", allow_all_outbound=True)
        sg_fs = ec2.SecurityGroup(self, "dbtr-sg-fs", vpc=vpc, security_group_name="dbtr-sg-fs", allow_all_outbound=True)
        sg_nlb = ec2.SecurityGroup(self, "dbtr-sg-nlb", vpc=vpc, security_group_name="dbtr-sg-nlb", allow_all_outbound=True)
        for subnet in vpc.public_subnets:
            sg.add_ingress_rule(peer=ec2.Peer.ipv4(typing.cast(str, subnet.ipv4_cidr_block)),
                                connection=ec2.Port.tcp(port=8080))
            sg_fs.add_ingress_rule(peer=ec2.Peer.ipv4(typing.cast(str, subnet.ipv4_cidr_block)),
                                    connection=ec2.Port.NFS)            
            sg_nlb.add_ingress_rule(peer=ec2.Peer.ipv4(typing.cast(str, subnet.ipv4_cidr_block)),
                                    connection=ec2.Port.all_traffic())      

        dbt_server_cluster = ecs.Cluster(
            self,
            "dbtServerCluster",
            enable_fargate_capacity_providers=True,
            vpc=vpc,
            cluster_name="dbtServerCluster"
        )

        ap_acl=efs.Acl(
                    owner_uid="12",
                    owner_gid="12",
                    permissions="777"
                )
        file_system = efs.FileSystem(self, "dbtr-fs", file_system_name="dbtr-fs", vpc=vpc, security_group=sg_fs)
        statement = iam.PolicyStatement(
            actions=["elasticfilesystem:ClientRootAccess", "elasticfilesystem:ClientWrite", "elasticfilesystem:ClientMount"
            ],
            principals=[iam.AnyPrincipal()],
            resources=["*"],
            conditions={
                "Bool": {
                    "elasticfilesystem:AccessedViaMountTarget": "true"
                }
            },
            effect=iam.Effect.ALLOW
        )
        file_system.add_to_resource_policy(statement)
        ap = file_system.add_access_point("dbtr-ap", path="/home", create_acl=ap_acl)
        authorization_config = ecs.AuthorizationConfig(
                access_point_id=ap.access_point_id
            )
        efs_volume_configuration = ecs.EfsVolumeConfiguration(
            file_system_id=file_system.file_system_id,
            root_directory="/",
            authorization_config=authorization_config,
            transit_encryption="ENABLED"
        )
        volume = ecs.Volume(name="dbtr-volume", efs_volume_configuration=efs_volume_configuration)
        fargate_task_definition = ecs.FargateTaskDefinition(self, "dbtServer",
            memory_limit_mib=3072,
            cpu=1024,
            volumes=[volume],
        )

        container = fargate_task_definition.add_container('dbtServer',
                                      image=ecs.ContainerImage.from_registry("europe-docker.pkg.dev/skaff-dbtr/dbt-server/prod:latest"),
                                      port_mappings=[ecs.PortMapping(container_port=8080)],
                                      environment={'PROVIDER': "local", "LOG_LEVEL": "info", "LOCATION": "eu-west-3"},
                                      logging=ecs.LogDrivers.aws_logs(
                                            stream_prefix="dbtServerlogs"
                                        )
                                      )
        container.add_mount_points(ecs.MountPoint(container_path="/home/dbt_user/dbt-server-volume", \
                                                  read_only=False, source_volume=volume.name))

        service = ecs.FargateService(self, "Service",
            cluster=dbt_server_cluster,
            task_definition=fargate_task_definition,
            desired_count=1,
            assign_public_ip=True,
            security_groups=[sg],
            vpc_subnets=subnets,
            capacity_provider_strategies=[ecs.CapacityProviderStrategy(
                capacity_provider="FARGATE",
                weight=1
            )],
            service_name="dbtr-service"
        )

        # NLB and API Gateway for autheuntication
        nlb = elbv2.NetworkLoadBalancer(self, "dbtr-nlb", vpc=vpc,
                                        enforce_security_group_inbound_rules_on_private_link_traffic=False, internet_facing=False,
                                        security_groups=[sg_nlb], load_balancer_name="dbtr-nlb")
        listener = nlb.add_listener("dbtr-listener", port=80)
        listener.add_targets("dbtr-target",
                                port=80,
                                protocol=elbv2.Protocol.TCP,
                                targets=[service],
                                target_group_name="dbtr-target"
                            )

        link = apigateway.VpcLink(self, "dbtr-link",
            targets=[nlb]
        )
        gateway_timeout = 60
        integration_options = apigateway.IntegrationOptions(
                connection_type=apigateway.ConnectionType.VPC_LINK,
                vpc_link=link,
                timeout=cdk.Duration.seconds(gateway_timeout)
            )
        integration = apigateway.Integration(
            type=apigateway.IntegrationType.HTTP_PROXY,
            integration_http_method="GET",
            options=integration_options
        )
        rest_api = apigateway.RestApi(self, "dbtserver-api", default_integration=integration)
        rest_api.root.add_method("GET", integration,
            authorization_type=apigateway.AuthorizationType.IAM
        )
        api_ressource = rest_api.root.add_resource("api")
        proxy_integration_options = apigateway.IntegrationOptions(
                connection_type=apigateway.ConnectionType.VPC_LINK,
                vpc_link=link,
                timeout=cdk.Duration.seconds(gateway_timeout),
                request_parameters={"integration.request.path.proxy": "method.request.path.proxy"}
            )
        proxy_integration = lambda method: apigateway.Integration(
                    type=apigateway.IntegrationType.HTTP_PROXY,
                    integration_http_method=method,
                    uri="http://"+ nlb.load_balancer_dns_name + "/api/{proxy}",
                    options=proxy_integration_options
                )
        proxy_ressource = api_ressource.add_proxy(
                default_integration=proxy_integration("GET"),
                any_method=False
            )
        proxy_ressource.add_method("GET", proxy_integration("GET"),
            authorization_type=apigateway.AuthorizationType.IAM,
            request_parameters={"method.request.path.proxy": True}
        )
        proxy_ressource.add_method("POST", proxy_integration("POST"),
            authorization_type=apigateway.AuthorizationType.IAM,
            request_parameters={"method.request.path.proxy": True}
        )
        proxy_ressource.add_method("DELETE", proxy_integration("DELETE"),
            authorization_type=apigateway.AuthorizationType.IAM,
            request_parameters={"method.request.path.proxy": True}
        )


class AwspyStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # The code that defines your stack goes here

        # example resource
        # queue = sqs.Queue(
        #     self, "AwspyQueue",
        #     visibility_timeout=Duration.seconds(300),
        # )
