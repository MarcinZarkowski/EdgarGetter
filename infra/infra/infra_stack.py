from aws_cdk import (
    Stack,
    Duration,
    Size,
    CfnOutput,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_batch as batch,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_secretsmanager as secretsmanager,
    aws_ecr as ecr,
    RemovalPolicy,
)
from constructs import Construct
import os
import json

class EdgarBatchStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. VPC
        vpc = ec2.Vpc(self, "EdgarBatchVpc", max_azs=2)

        # 2. Create ECR Repository
        # The user requested a repository with a fixed name to push images to manually
        repo = ecr.Repository(self, "EdgarBatchRepo",
            repository_name="edgar-batch-repo",
            removal_policy=RemovalPolicy.DESTROY,
            image_tag_mutability=ecr.TagMutability.MUTABLE
        )

        # 3. Import the existing secret containing sensitive values
        # The user has already created this secret in AWS Secrets Manager
        secret = secretsmanager.Secret.from_secret_name_v2(self, "EdgarBatchSecret", "EdgarPackageSecrets")

        # 4. Batch Infrastructure
        
        # Compute Environment (Fargate)
        compute_env = batch.FargateComputeEnvironment(self, "EdgarComputeEnv",
            vpc=vpc,
            maxv_cpus=256,
        )

        # Job Queue
        queue = batch.JobQueue(self, "EdgarJobQueue",
            compute_environments=[batch.OrderedComputeEnvironment(
                compute_environment=compute_env,
                order=1
            )],
            priority=1
        )

        # 5. Roles
        
        # Execution Role: Used by the ECS Agent to pull images and fetch secrets
        execution_role = iam.Role(self, "BatchExecutionRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ]
        )
        secret.grant_read(execution_role)

        # Job Role: Used by the application code itself
        job_role = iam.Role(self, "BatchJobRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com")
        )
        # Grant S3 Permissions to the job role
        job_role.add_to_policy(iam.PolicyStatement(
            actions=["s3:PutObject", "s3:PutObjectAcl"],
            resources=["arn:aws:s3:::icons-tickers-politicians/*", "arn:aws:s3:::icons-tickers-politicians"]
        ))

        # 6. Job Definition
        job_def = batch.EcsJobDefinition(self, "EdgarJobDef",
            job_definition_name="EdgarBatchJob",
            container=batch.EcsFargateContainerDefinition(self, "EdgarContainer",
                image=ecs.ContainerImage.from_ecr_repository(repo, "latest"),
                cpu=0.5, # 0.5 vCPU
                memory=Size.mebibytes(2048),
                execution_role=execution_role,
                job_role=job_role,
                environment={
                    "GET_8K": "False",
                    "GET_10K": "False",
                    "GET_10Q": "False",

                    "GET_4": "True",
                    "GET_COMPANY_INFO": "True",

                    "GET_COMPANY_CASHFLOW": "False",
                    "GET_COMPANY_BALANCE_SHEET": "False",
                    "GET_COMPANY_INCOME_STATEMENT": "False",

                    "GET_LAST_DAYS" : "31",
                    "PROCESS_SINCE_LATEST_IN_DB": "True",
                    "NUM_OF_THREADS": "20",
                    "DELETE_8K_BEFORE_DAYS_AGO": "0",
                    "DELETE_4_BEFORE_DAYS_AGO": "93",
                },
                secrets={
                    "DB_URL": batch.Secret.from_secrets_manager(secret, "DB_URL"),
                    "AWS_ACCESS_KEY": batch.Secret.from_secrets_manager(secret, "AWS_ACCESS_KEY"),
                    "AWS_SECRET_ACCESS_KEY": batch.Secret.from_secrets_manager(secret, "AWS_SECRET_ACCESS_KEY"),
                    "S3_URL": batch.Secret.from_secrets_manager(secret, "S3_URL"),
                    "REGION": batch.Secret.from_secrets_manager(secret, "REGION"),
                }
            )
        )

        # 7. Schedule
        rule = events.Rule(self, "EdgarSchedule",
            schedule=events.Schedule.expression("cron(0 0,6,12,18 * * ? *)"), # Every 6 hours
            description="Trigger Edgar Batch Job"
        )
        
        rule.add_target(targets.BatchJob(
            queue.job_queue_arn,
            queue,
            job_def.job_definition_arn,
            job_def
        ))

        # Output the secret ARN for convenience
        CfnOutput(self, "SecretArn",
            value=secret.secret_arn,
            description="The ARN of the secret containing DB_URL and AWS Keys"
        )
        
        # Output the ECR Repo URI
        CfnOutput(self, "RepoUri",
            value=repo.repository_uri,
            description="The URI of the ECR Repository to push images to"
        )
