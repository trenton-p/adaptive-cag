import os
import pathlib
import constants
import aws_cdk as cdk

from aws_cdk import (
    aws_s3 as _s3,
    aws_iam as _iam,
    aws_lambda as _lambda,
    aws_logs as _logs
)
from constructs import Construct

class Agent(Construct):

    def __init__(self, scope: Construct, id: str, secret_arn: str) -> None:
        super().__init__(scope, id)

        # Create the API Logging Role
        logging_role = _iam.Role(
            self,
            "LambdaLoggingRole",
            assumed_by=_iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "LambdaLoggingPolicy": _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=[
                                'logs:CreateLogGroup',
                                'logs:CreateLogStream',
                                'logs:PutLogEvents',
                                'logs:DeleteRetentionPolicy',
                                'logs:PutRetentionPolicy'
                            ],
                            effect=_iam.Effect.ALLOW,
                            resources=["*"]
                        )
                    ]
                )
            }
        )

        # Create Agent API Handler
        handler = _lambda.DockerImageFunction(
            self,
            "AgentHandler",
            code=_lambda.DockerImageCode.from_image_asset(
                directory=str(pathlib.Path(__file__).parent.joinpath("runtime").resolve())
            ),
            log_retention_role=logging_role,
            memory_size=512,
            timeout=cdk.Duration.seconds(120),
            environment={
                "PINECONE_SECRET": secret_arn,
                "BEDROCK_TEXT_MODEL": constants.BEDROCK_TEXT_MODEL,
                "PINECONE_EMBEDDING_MODEL": constants.PINECONE_EMBEDDING_MODEL,
                # NOTE: Hard-code index name to the "PROD" index for demo purposes
                "PINECONE_INDEX_NAME": constants.PINECONE_PROD_INDEX
            }
        )
        handler.add_to_role_policy(
            statement=_iam.PolicyStatement(
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream"
                ],
                effect=_iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:bedrock:*::foundation-model/{os.environ['BEDROCK_TEXT_MODEL']}"
                ]
            )
        )
        handler.add_to_role_policy(
            statement=_iam.PolicyStatement(
                actions=["secretsmanager:GetSecretValue"],
                effect=_iam.Effect.ALLOW,
                resources=[secret_arn]
            )
        )
        self._create_log_group(scope=scope, log_name="AgentHandlerLogGroup")

        # Convert API Handler into a Function URL
        self.fn_url = handler.add_function_url(
            invoke_mode=_lambda.InvokeMode.RESPONSE_STREAM,
            auth_type=_lambda.FunctionUrlAuthType.AWS_IAM,
            cors=_lambda.FunctionUrlCorsOptions(
                allowed_origins=["*"],
                allowed_headers=["*"],
                allowed_methods=[_lambda.HttpMethod.POST],
                allow_credentials=True,
                max_age=cdk.Duration.seconds(0)
            )
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