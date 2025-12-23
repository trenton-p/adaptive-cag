import os
import pathlib
import constants
import aws_cdk as cdk

from aws_cdk import (
    aws_secretsmanager as _secrets,
    aws_lambda as _lambda,
    aws_iam as _iam,
    custom_resources as _cr,
    aws_logs as _logs
)
from constructs import Construct

class VectorDB(Construct):

    def __init__(self, scope: Construct, id: str, db_name: str) -> None:
        super().__init__(scope, id)

        # Create the Lambda Function to manage the Pinecone Index as part of the infrastructure
        index_handler = _lambda.Function(
            self,
            "IndexHandler",
            code=_lambda.Code.from_asset(
                path=str(pathlib.Path(__file__).parent.joinpath("index_handler").resolve()),
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c", "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.lambda_handler",
            memory_size=128,
            tracing=_lambda.Tracing.ACTIVE,
            timeout=cdk.Duration.minutes(5),
            log_retention_role=_iam.Role(
                self,
                "IndexHandlerLoggingRole",
                assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
                inline_policies={
                    "LambdaLogging": _iam.PolicyDocument(
                        statements=[
                            _iam.PolicyStatement(
                                actions=[
                                    "logs:DeleteRetentionPolicy",
                                    "logs:PutRetentionPolicy",
                                    "logs:CreateLogGroup",
                                    "logs:CreateLogStream",
                                    "logs:PutLogEvents"
                                ],
                                effect=_iam.Effect.ALLOW,
                                resources=["*"]
                            )
                        ]
                    )
                }
            )
        )
        self._create_log_group(scope=scope, log_name="IndexHandlerLogGroup")

        # Store the API in secrets manager for other stack components to use
        self.pinecone_secret = _secrets.Secret(
            self,
            "SecretProperties",
            secret_object_value={
                "PINECONE_INDEX_NAME": cdk.SecretValue.unsafe_plain_text(
                    secret=db_name.lower() # Can only contain lowercase letters, numbers, and hyphens.
                ),
                "PINECONE_API_KEY": cdk.SecretValue.unsafe_plain_text(
                    secret=constants.PINECONE_API_KEY
                ),
                "PINECONE_REGION": cdk.SecretValue.unsafe_plain_text(
                    secret=constants.PINECONE_REGION
                )
            }
        )
        self.pinecone_secret.grant_read(index_handler)

        # Create a CloudFormation Custom Resource to invoke the `index_handler`
        self.invoke_resource = cdk.CustomResource(
            self,
            "InvokeIndexHandler",
            resource_type="Custom::IndexHandler",
            service_token=_cr.Provider(
                self,
                "IndexHandlerProvider",
                on_event_handler=index_handler
            ).service_token,
            properties={
                "SECRET": self.pinecone_secret.secret_arn,
                "IMPORT_URI": constants.PINECONE_IMPORT_URI,
                "INTEGRATION_ID": constants.PINECONE_INTEGRATION_ID
            }
        )


    @classmethod
    def _create_log_group(self, scope: Construct, log_name: str):
        _logs.LogGroup(
            scope=scope,
            id=log_name,
            log_group_name=f"/aws/lambda/{log_name}",
            retention=_logs.RetentionDays.ONE_DAY,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )


    @property
    def secret_arn(self):
        return self.pinecone_secret.secret_arn