#!/usr/bin/env python3
import os
import boto3
import constants
import aws_cdk as cdk

from dotenv import load_dotenv
from components.vector_db import VectorDB
from components.data_pipeline import DataPipeline
from components.contact_form import ContactForm
from components.agent import Agent
from components.website import StaticWebsite
from constructs import Construct


class NewsAgentStack(cdk.Stack):

    def __init__(self, scope: Construct, id: str, *, data_lake_bucket: str, index_name: str, contact_email: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Initialize the Pinecone vector database
        vector_db = VectorDB(self, "VectorDatabase", db_name=index_name)

        # Create the Data ingestion pipeline
        data_pipeline = DataPipeline(
            self,
            "DataPipeline",
            bucket_name=data_lake_bucket,
            secret_arn=vector_db.secret_arn
        )
        cdk.CfnOutput(self, "GlueJobID", value=data_pipeline.glue_job_id)
        cdk.CfnOutput(self, "KinesisStreamName", value=data_pipeline.stream_name)
        cdk.CfnOutput(self, "DataBucketName", value=data_lake_bucket)

        # Website contact form api
        contact_form = ContactForm(self, "ContactForm", email_address=contact_email)
        cdk.CfnOutput(self, "TopicArn", value=contact_form.topic_arn)

        # News agent api
        news_agent = Agent(self, "NewsAgent", secret_arn=vector_db.secret_arn)

        # ACME News Website
        website = StaticWebsite(
            self,
            "Website",
            fn_url=news_agent.fn_url,
            form_api=contact_form.api_id
        )
        cdk.CfnOutput(self, "CloudFrontUrl", value=website.domain_name)


app = cdk.App()
NewsAgentStack(
    app,
    "ACME-NewsAgentStack",
    env=cdk.Environment(
        account=boto3.client("sts").get_caller_identity()["Account"],
        region=constants.AWS_REGION

    ),
    contact_email=constants.CONTACT_EMAIL,
    data_lake_bucket=constants.DATA_LAKE_BUCKET,
    index_name=constants.PINECONE_PROD_INDEX
)

if __name__ == "__main__":
    app.synth()