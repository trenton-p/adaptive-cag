import os
import json
import boto3
import time

from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from pinecone import Pinecone, ServerlessSpec

# Global parameters
LOGGER = Logger()
TRACER = Tracer()
REGION = os.environ["AWS_REGION"]

class ImportException(Exception):
    def __init__(self, message):
        super().__init__(message)


@TRACER.capture_lambda_handler
def lambda_handler(event, context):
    LOGGER.info(f"Received event:\n{event}")
    props = event["ResourceProperties"]
    pinecone_props = get_secret(secret_arn=props["SECRET"])
    api_key = pinecone_props["PINECONE_API_KEY"]
    index_name = pinecone_props["PINECONE_INDEX_NAME"]
    index_region = pinecone_props["PINECONE_REGION"]
    root_uri = props["IMPORT_URI"]
    integration_id = props["INTEGRATION_ID"]

    if event["RequestType"] == "Create":
        # Initialize the Pinecone client
        # pc = Pinecone(api_key=api_key)
        # NOTE: Internal Pinecone Indexes are using "slabs", therefore force v4 "clustering" index type
        pc = Pinecone(
            api_key=api_key,
            additional_headers={
                "x-pinecone-index-mode": "clustering"
            }
        )

        # Check if the Pinecone index already exists
        if index_name not in pc.list_indexes().names():
            LOGGER.info(f"Non-existent Pinecone Index: {index_name}; Creating ...")
            pc.create_index(
                name=index_name,
                dimension=1024, # `cohere.embed-english-v3`, and `multilingual-e5-large`
                metric="cosine",
                # metric="dotproduct",
                spec=ServerlessSpec(
                    cloud="aws",
                    region=index_region
                )
            )

            # Wait for the Pinecone index to be created
            while not pc.describe_index(index_name).status["ready"]:
                time.sleep(1)

            # Start the Pinecone Import for semantic route data
            LOGGER.info("Initializing Pinecone Data Import ...")
            index = pc.Index(index_name)
            import_job = index.start_import(
                uri=root_uri,
                error_mode="ABORT",
                integration_id=integration_id
            )
            job_id = import_job.id
            job_status = index.describe_import(id=job_id)["status"]
            LOGGER.info(f"Pinecone Index Import Status: {job_status}")

            return {
                "PhysicalResourceId": index_name,
                "Data": {
                    "JobId": job_id
                }
            }
    
    if event["RequestType"] == "Delete":
        # Delete the Pinecone Index
        LOGGER.info("Deleting Pinecone Index ...")
        pc = Pinecone(api_key=api_key)
        pc.delete_index(index_name)
        
        return {
            "PhysicalResourceId": index_name,
            "Data": {
                "JobId": "None"
            }
        }


def get_secret(secret_arn: str):
    # Get the Pinecone secret values
    LOGGER.info("Retrieving Pinecone Secret Values ...")
    try:
        response = boto3.client("secretsmanager", region_name=REGION).get_secret_value(
            SecretId=secret_arn
        )

    except ClientError as e:
        message = e.response["Error"]["Message"]
        LOGGER.error(message)
        raise e

    return json.loads(response["SecretString"])
