import typing
from aws_cdk import (
    # Duration,
    Stack,
    # aws_sqs as sqs,
    aws_iam,
    aws_lambda as _lambda,
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

class DBTStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ## Lambda function setup (responsible for authentication) ##
        auth_function = _lambda.Function(
            self,
            "DBTrAuthFunction",
            function_name="dbtr-auth",
            runtime = _lambda.Runtime.PYTHON_3_12, 
            code = _lambda.Code.from_asset("lambda"), # Points to the lambda directory
            handler = "dbtauth.lambda_handler", # Points to the 'dbtauth' file in the lambda directory
        )
        fn_url = auth_function.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.AWS_IAM
        )

        ## Network and firewall configuration ##
        subnet_configuration = ec2.SubnetConfiguration(
                name="dbtr-subnets",
                subnet_type=ec2.SubnetType.PUBLIC,
                reserved=False
            )
        vpc = ec2.Vpc(self, "dbtr-vpc", vpc_name="dbtr-vpc", subnet_configuration=[subnet_configuration]) #vpc-0f763d4efd233d795
        subnets = ec2.SubnetSelection(subnets=vpc.public_subnets)

        sg = ec2.SecurityGroup(self, "dbtr-sg", vpc=vpc, security_group_name="dbtr-sg", allow_all_outbound=True)
        sg_fs = ec2.SecurityGroup(self, "dbtr-sg-fs", vpc=vpc, security_group_name="dbtr-sg-fs", allow_all_outbound=True)
        
        sg.add_ingress_rule(peer=ec2.Peer.any_ipv4(),
                        connection=ec2.Port.tcp(port=80))
        for subnet in vpc.public_subnets:
            # needed to be able to access the file system
            sg_fs.add_ingress_rule(peer=ec2.Peer.ipv4(typing.cast(str, subnet.ipv4_cidr_block)),
                                    connection=ec2.Port.NFS)
            # to make dbt-server accessible internally (to other services in same vpc)
            sg.add_ingress_rule(peer=ec2.Peer.ipv4(typing.cast(str, subnet.ipv4_cidr_block)),
                                connection=ec2.Port.tcp(port=8080))      

        ## Cluster setup ##
        dbt_server_cluster = ecs.Cluster(
            self,
            "dbtServersCluster",
            enable_fargate_capacity_providers=True,
            vpc=vpc,
            cluster_name="dbtServersCluster"
        )

        ## EFS file system setup ##
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

        ## DBT server and reverse proxy setup ##
        volume = ecs.Volume(name="dbtr-volume", efs_volume_configuration=efs_volume_configuration)
        fargate_task_definition = ecs.FargateTaskDefinition(self, "dbtrServers",
            memory_limit_mib=3072,
            cpu=1024,
            volumes=[volume],
        )

        container = fargate_task_definition.add_container('dbtrServers',
                                        image=ecs.ContainerImage.from_registry("europe-docker.pkg.dev/skaff-dbtr/dbt-server/prod:latest"), # to test with an image that includes the postgress adapter use: ghcr.io/maryam21/dbt-server:latest
                                        environment={'PROVIDER': "local", "LOG_LEVEL": "info", "LOCATION": "eu-west-3"},
                                        port_mappings=[ecs.PortMapping(container_port=8080)],
                                        logging=ecs.LogDrivers.aws_logs(
                                            stream_prefix="dbtrServerslogs"
                                        ),
                                        essential=True
                                      )
        proxy_container = fargate_task_definition.add_container('reverseProxyServers',
                                        image=ecs.ContainerImage.from_registry("ghcr.io/maryam21/nginx:latest"),
                                        environment={'LAMBDA_URL': fn_url.url},
                                        port_mappings=[ecs.PortMapping(container_port=80)],
                                        logging=ecs.LogDrivers.aws_logs(
                                            stream_prefix="reverseProxyServerslogs"
                                        ),
                                        essential=True
                                      )
        container.add_mount_points(ecs.MountPoint(container_path="/home/dbt_user/dbt-server-volume", \
                                                  read_only=False, source_volume=volume.name))

        service = ecs.FargateService(self, "dbtrService",
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
            service_name="dbtr-services"
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
