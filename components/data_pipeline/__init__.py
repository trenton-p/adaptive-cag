import os
import constants
import pathlib
import aws_cdk as cdk

from aws_cdk import (
    aws_s3 as _s3,
    aws_lambda as _lambda,
    aws_glue as _glue,
    aws_kinesis as _kinesis,
    aws_s3_deployment as _deployment,
    aws_iam as _iam,
    custom_resources as _cr,
    aws_logs as _logs,
    aws_sqs as _sqs,
    aws_lambda_event_sources as _sources
)
from constructs import Construct

class DataPipeline(Construct):

    def __init__(self, scope: Construct, id: str, bucket_name: str, secret_arn: str) -> None:
        super().__init__(scope, id)

        # Register the the data lake S3 bucket
        data_bucket = _s3.Bucket.from_bucket_name(
            self,
            "DataBucket",
            bucket_name=bucket_name
        )

        # Deploy ETL scripts to the `data_bucket`
        deploy_scripts = _deployment.BucketDeployment(
            self,
            "ScriptDeployment",
            sources=[
                _deployment.Source.asset(
                    path=str(pathlib.Path(__file__).parent.joinpath("etl-scripts").resolve())
                )
            ],
            destination_bucket=data_bucket,
            destination_key_prefix="production-data/etl-scripts",
            retain_on_delete=False
        )

        # Deploy Java SDK assets
        deploy_assets = _deployment.BucketDeployment(
            self,
            "AssetsDeployment",
            sources=[
                _deployment.Source.asset(
                    path=str(pathlib.Path(__file__).parent.joinpath("assets").resolve())
                )
            ],
            destination_bucket=data_bucket,
            destination_key_prefix="production-data/assets",
            retain_on_delete=False
        )

        # Create the Kinesis stream for data ingest
        self.ingest_stream = _kinesis.Stream(
            self,
            "EventIngestStream",
            stream_name="EventIngestStream",
            stream_mode=_kinesis.StreamMode.ON_DEMAND,
            retention_period=cdk.Duration.hours(24)
        )
        self.ingest_stream.apply_removal_policy(policy=cdk.RemovalPolicy.DESTROY)

        # Create the Glue Role for the Streaming ETL Job
        glue_role = _iam.Role(
            self,
            "GlueJobRole",
            assumed_by=_iam.ServicePrincipal(service="glue.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="service-role/AWSGlueServiceRole"
                ),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="AmazonSSMReadOnlyAccess"
                ),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="AmazonEC2ContainerRegistryReadOnly"
                ),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="AWSGlueConsoleFullAccess"
                ),
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name="AmazonKinesisReadOnlyAccess"
                )
            ],
            inline_policies={
                "S3Access": _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=[
                                "s3:GetBucketLocation",
                                "s3:ListBucket", 
                                "s3:GetBucketAcl",
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:DeleteObject"
                            ],
                            effect=_iam.Effect.ALLOW,
                            resources=[
                                data_bucket.bucket_arn,
                                f"{data_bucket.bucket_arn}/*"
                            ]
                        )
                    ]
                ),
                "DynamoDBAccess": _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=[
                                "dynamodb:BatchGetItem",
                                "dynamodb:DescribeStream",
                                "dynamodb:DescribeTable",
                                "dynamodb:GetItem",
                                "dynamodb:Query",
                                "dynamodb:Scan",
                                "dynamodb:BatchWriteItem",
                                "dynamodb:CreateTable",
                                "dynamodb:DeleteTable",
                                "dynamodb:UpdateTable",
                                "dynamodb:PutItem",
                                "dynamodb:UpdateItem",
                                "dynamodb:DeleteItem"
                            ],
                            effect=_iam.Effect.ALLOW,
                            resources=["*"]
                        )
                    ]
                ),
                "PassRole": _iam.PolicyDocument(
                    statements=[
                        _iam.PolicyStatement(
                            actions=["iam:PassRole"],
                            effect=_iam.Effect.ALLOW,
                            resources=[
                                f"arn:{cdk.Aws.PARTITION}:iam::{cdk.Aws.ACCOUNT_ID}:role/StreamingEtlGlueRole"
                            ],
                            conditions={
                                "StringLike": {
                                    "iam:PassedToService": "glue.amazonaws.com"
                                }
                            }
                        )
                    ]
                )
            }
        )

        # Create the Glue Catalog for the streaming data table
        # NOTE: Ensure the DB Name is unique across the AWS Region.
        events_db = _glue.CfnDatabase(
            self,
            "NewsEventsDatabase",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=_glue.CfnDatabase.DatabaseInputProperty(
                name="events_db", # Hard-coded
                description="Kinesis Stream Events Database"
            )
        )
        events_db.apply_removal_policy(policy=cdk.RemovalPolicy.DESTROY)

        # Apply "encryption at rest" policy to the events database
        _glue.CfnDataCatalogEncryptionSettings(
            self,
            "EncryptNewsEventsDatabase",
            catalog_id=events_db.catalog_id,
            data_catalog_encryption_settings=_glue.CfnDataCatalogEncryptionSettings.DataCatalogEncryptionSettingsProperty(
                encryption_at_rest=_glue.CfnDataCatalogEncryptionSettings.EncryptionAtRestProperty(
                    catalog_encryption_mode="SSE-KMS"
                )
            )
        )

        # Create the Glue streaming data table
        # TODO: Update to Open Table formatting
        stream_table = _glue.CfnTable(
            self,
            "IngestStreamTable",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_name="events_db", # Hard-coded
            table_input=_glue.CfnTable.TableInputProperty(
                name="stream_table", # Hard-coded
                description="Kinesis Stream Table",
                parameters={
                    "classification": "json"
                },
                table_type="EXTERNAL_TABLE",
                storage_descriptor=_glue.CfnTable.StorageDescriptorProperty(
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    columns=[
                        _glue.CfnTable.ColumnProperty(
                            name="event_id",
                            type="string",
                            comment="News Event Article ID"
                        ),
                        _glue.CfnTable.ColumnProperty(
                            name="updated_at",
                            type="string",
                            comment="News Event Creation Time"
                        ),
                        _glue.CfnTable.ColumnProperty(
                            name="summary",
                            type="string",
                            comment="News Article Highlights"
                        ),
                        _glue.CfnTable.ColumnProperty(
                            name="event",
                            type="string",
                            comment="News Event Article"
                        )
                    ],
                    location=self.ingest_stream.stream_name,
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    parameters={
                        "streamArn": self.ingest_stream.stream_arn,
                        "typeOfData": "kinesis"
                    },
                    serde_info=_glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe"
                    )
                )
            )
        )
        stream_table.add_dependency(events_db)
        stream_table.apply_removal_policy(cdk.RemovalPolicy.DESTROY)

        # Create the Apache Iceberg connection for the Glue Job
        # NOTE: Ensure the "Apache Iceberg Connector for AWS Glue" subscription
        #       has been configured in the AWS Marketplace. This should not be
        #       needed when upgrading to Glue 4.0, and Open Table format.
        glue_connection = _glue.CfnConnection(
            self,
            "GlueIcebergConnection",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            connection_input=_glue.CfnConnection.ConnectionInputProperty(
                name="events-iceberg-connection", # Hard-coded
                description="Apache Iceberg Connector for Glue 3.0",
                connection_type="MARKETPLACE",
                connection_properties={
                    "CONNECTOR_TYPE": "MARKETPLACE",
                    "CONNECTOR_URL": "https://709825985650.dkr.ecr.us-east-1.amazonaws.com/amazon-web-services/glue/iceberg:0.14.0-glue3.0-2",
                    "CONNECTOR_CLASS_NAME": "iceberg"
                }
            )
        )

        # Create the Glue Stream ETL job
        glue_etl_job = _glue.CfnJob(
            self,
            "GlueStreamingEtlJob",
            name="EventEtlJob", # Hard-coded
            description="AWS Streaming ETL Job to load the news event from Kinesis Data Streams, and store in S3",
            role=glue_role.role_arn,
            command=_glue.CfnJob.JobCommandProperty(
                name="gluestreaming",
                python_version="3",
                script_location=f"{data_bucket.s3_url_for_object(key='production-data/etl-scripts')}/s3_iceberg_writes.py"
            ),
            # Hard-coded link to `_glue.CfnConnection()`
            connections=_glue.CfnJob.ConnectionsListProperty(connections=["events-iceberg-connection"]),
            default_arguments={
                "--catalog": "job_catalog",
                "--database_name": "events_db", # Hard-coded
                "--table_name": "events_table", # Hard-coded
                "--primary_key": "event_id",
                "--partition_key": "updated_at", # Hard-coded
                "--kinesis_table_name": "stream_table",
                "--kinesis_stream_arn": self.ingest_stream.stream_arn,
                "--starting_position_of_kinesis_iterator": "LATEST",
                "--iceberg_s3_path": data_bucket.s3_url_for_object(
                    key="production-data/event-data" # Hard-coded
                ),
                "--aws_region": cdk.Aws.REGION,
                "--lock_table_name": "events_lock", # Hard-coded
                "--window_size": "100 seconds",
                "--extra-jars": data_bucket.s3_url_for_object(key="production-data/assets/aws-sdk-java-2.17.224.jar"),
                "--extras-jars-first": "true",
                "--enable-metrics": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": data_bucket.s3_url_for_object(key="production-data/event-data/spark_history_logs/"), # Hard-coded
                "--enable-job-insights": "true",
                "--enable-glue-datacatalog": "true",
                "--job-bookmarks-option": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-bookmark-option": "job-bookmark-disable",
                "--job-language": "python",
                "--TempDir": data_bucket.s3_url_for_object(key="production-data/event-data/temp") # Hard-coded
            },
            execution_property=_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            glue_version="3.0",
            max_retries=0,
            # NOTE: Update the following to scale compute resources for large ingest
            #       data/multiple put records
            worker_type="G.1X",
            number_of_workers=2
        )
        glue_etl_job.node.add_dependency(deploy_assets)
        glue_etl_job.node.add_dependency(deploy_scripts)
        glue_etl_job.node.add_dependency(glue_connection)

        # Create custom resource to automatically start the glue job
        start_glue_job = _cr.AwsCustomResource(
            self,
            "GlueJobID",
            resource_type="Custom::StartGlueJobRun",
            on_create=_cr.AwsSdkCall(
                action="startJobRun",
                service="Glue",
                parameters={
                    "JobName": glue_etl_job.name
                },
                physical_resource_id=_cr.PhysicalResourceId.of("glue_job_id")
            ),
            policy=_cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=_cr.AwsCustomResourcePolicy.ANY_RESOURCE
            )
        )
        start_glue_job.node.add_dependency(glue_etl_job)
        self.glue_job_id = start_glue_job.get_response_field("JobRunId")

        # Create the Kinesis event handler function
        event_handler = _lambda.Function(
            self,
            "NewsEventRecordHandler",
            code=_lambda.Code.from_asset(
                path=str(pathlib.Path(__file__).parent.joinpath("event_handler").resolve()),
                bundling=cdk.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash", "-c", "pip install -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ]
                )
            ),
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.lambda_handler",
            memory_size=8192,
            tracing=_lambda.Tracing.ACTIVE,
            timeout=cdk.Duration.minutes(15),
            log_retention_role=_iam.Role(
                self,
                "EventHandlerLoggingRole",
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
            ),
            environment={
                "PINECONE_SECRET": secret_arn,
                "BEDROCK_TEXT_MODEL": constants.BEDROCK_TEXT_MODEL,
                "PINECONE_EMBEDDING_MODEL": constants.PINECONE_EMBEDDING_MODEL,
                "NAMESPACE_NAME": "router"
            }
        )
        event_handler.add_to_role_policy(
            statement=_iam.PolicyStatement(
                sid="SecretAccess",
                actions=["secretsmanager:GetSecretValue"],
                effect=_iam.Effect.ALLOW,
                resources=[secret_arn]
            )
        )
        event_handler.add_to_role_policy(
            statement=_iam.PolicyStatement(
                sid="BedrockAccess",
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream"
                ],
                effect=_iam.Effect.ALLOW,
                resources=[
                    f"arn:{cdk.Aws.PARTITION}:bedrock:{cdk.Aws.REGION}::foundation-model/{constants.BEDROCK_EMBEDDING_MODEL}",
                    f"arn:{cdk.Aws.PARTITION}:bedrock:{cdk.Aws.REGION}::foundation-model/{constants.BEDROCK_TEXT_MODEL}"
                ]
            )
        )
        self.ingest_stream.grant_read(event_handler)
        self._create_log_group(scope=scope, log_name="EventRecordHandlerLogGroup")

        # Add a Dead Letter Queue in case of a Lambda Failure
        failure_notifier = _sqs.Queue(
            self,
            "DeadLetterQueue",
            visibility_timeout=cdk.Duration.seconds(300),
            queue_name="lambda_kinesis_dlq"
        )
        failure_notifier.apply_removal_policy(cdk.RemovalPolicy.DESTROY)

        # Create the event source mapping to connect Lambda to Kinesis
        event_handler.add_event_source(
            _sources.KinesisEventSource(
                stream=self.ingest_stream,
                batch_size=25,
                starting_position=_lambda.StartingPosition.TRIM_HORIZON,
                retry_attempts=0,
                parallelization_factor=5, # Multiple batches in parallel
                on_failure=_sources.SqsDlq(failure_notifier)
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


    @property
    def job_id(self):
        return str(self.glue_job_id)
    

    @property
    def stream_name(self):
        return self.ingest_stream.stream_name