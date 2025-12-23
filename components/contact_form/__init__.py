import pathlib
import aws_cdk as cdk

from aws_cdk import (
    aws_sns as _sns,
    aws_sns_subscriptions as _sns_sub,
    aws_lambda as _lambda,
    aws_apigatewayv2 as _httpgw,
    aws_apigatewayv2_integrations as _integration,
    aws_iam as _iam,
    aws_logs as _logs
)
from constructs import Construct

class ContactForm(Construct):

    def __init__(self, scope: Construct, id: str, email_address: str) -> None:
        super().__init__(scope, id)

        # Create the SNS topic to forward contact emails to provided address
        self.sns_topic = _sns.Topic(self, "SnsTopic", display_name="Contact Form Topic")
        self.sns_topic.add_subscription(
            topic_subscription=_sns_sub.EmailSubscription(
                email_address=email_address
            )
        )
        
        # Create the Lambda Function to forward the message to the SNS topic
        form_handler = _lambda.Function(
            self,
            "ContactFormHandler",
            code=_lambda.Code.from_asset(
                path=str(pathlib.Path(__file__).parent.joinpath("runtime").resolve())
            ),
            environment={
                "TOPIC_ARN": self.sns_topic.topic_arn
            },
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.lambda_handler",
            memory_size=512,
            timeout=cdk.Duration.seconds(60),
            log_retention=_logs.RetentionDays.ONE_MONTH,
            log_retention_role=_iam.Role(
                self,
                "RecordHandlerLoggingRole",
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
        self.sns_topic.grant_publish(form_handler)

        # Create the contact form API
        self.api = _httpgw.HttpApi(
            self,
            "ContactFormAPI",
            description="Website Contact Form API",
            cors_preflight={
                "allow_origins": ["*"],
                "allow_methods": [_httpgw.CorsHttpMethod.POST],
                "allow_headers": ["*"]
            }
        )
        self.api.add_routes(
            path="/api/contact",
            methods=[_httpgw.HttpMethod.POST],
            integration=_integration.HttpLambdaIntegration(
                "FormIntegration",
                handler=form_handler
            )
        )


    @property
    def topic_arn(self):
        return self.sns_topic.topic_arn
    

    @property
    def api_id(self):
        return self.api.http_api_id