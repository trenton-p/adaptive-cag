import os
import json
import boto3
import base64
import time

from aws_lambda_powertools import Tracer
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from pinecone_plugins.inference.core.client.exceptions import PineconeApiException
from langchain.text_splitter import CharacterTextSplitter
from pinecone import Pinecone
from adapter import BedrockStreamAdapter

# Global variables
CHUNK_SIZE = 512
OVERLAP = 100
REGION = os.environ["AWS_REGION"]
EMBEDDING_MODEL = os.environ["PINECONE_EMBEDDING_MODEL"]
TEXT_MODEL = os.environ["BEDROCK_TEXT_MODEL"]
NAMESPACE_NAME = os.environ["NAMESPACE_NAME"]
SECRET_ARN = os.environ["PINECONE_SECRET"]
TRACER = Tracer()
LOGGER = Logger()
INFERENCE_ADAPTER= BedrockStreamAdapter(text_model=TEXT_MODEL, region=REGION)

# Context Chunking Prompt
contextual_prompt = """
    <document>
    {doc_content}
    </document>


    Here is the chunk we want to situate within the whole document
    <chunk>
    {chunk_content}
    </chunk>


    Please give a short, succinct context to situate this chunk within the overall document, for the purposes of improving search retrieval of the chunk.
    Answer only with the succinct context and nothing else.
"""


def get_secret(secret_arn: str):
    client = boto3.client("secretsmanager", region_name=REGION)
    # Get the Pinecone secret values
    LOGGER.info("Retrieving Pinecone Secret Values ...")
    try:
        response = client.get_secret_value(
            SecretId=secret_arn
        )

    except ClientError as e:
        message = e.response["Error"]["Message"]
        LOGGER.error(message)
        raise e

    return json.loads(response["SecretString"])


def get_chunks(text: str):
    LOGGER.info("Chunking Record Event ...")

    # Split the event into chunks
    text_splitter = CharacterTextSplitter(
        separator=".",
        chunk_size=CHUNK_SIZE,
        chunk_overlap=OVERLAP
    )

    return text_splitter.split_text(text)


# create embeddings (exponential backoff to avoid RateLimitError)
def get_embeddings(pc, texts: list, input_type: str):
    # TODO: Update to incorporate Pinecone integrated embeddings.
    for i in range(5):  # max 5 retries
        try:
            response = pc.inference.embed(
                model=EMBEDDING_MODEL,
                inputs=texts,
                parameters={"input_type": input_type, "truncate": "END"}
            )
            passed = True
        except PineconeApiException:
            time.sleep(2**i)  # wait 2^j seconds before retrying
            print("Retrying Pinecone Embedding Request ...")
            passed = False
    if not passed:
        raise RuntimeError("Failed to create embeddings!")

    return response


def get_namespace(pc, index, summary: str):
    # Get embedding representation of the summary
    embedding = get_embeddings(
        pc=pc,
        texts=[summary],
        input_type="query"
    )

    # Use the embedding representation to get the top 5 vectors
    query_results = index.query(
        namespace="router", # Hard-coded to match semantic routing namespace in the Pinecone Index
        vector=embedding.data[-1]["values"],
        top_k=5,
        include_values=False,
        include_metadata=True
    )
    
    # Query the namespace of the top ranked document id
    # NOTE: Removed re-ranked documents, as there is a rate limit of 60 requests per minute.
    # TODO: Check with support to see if the limit can be lifted.
    metadata = index.query(
        namespace="router",
        id=query_results["matches"][0]["id"],
        top_k=1,
        include_values=False,
        include_metadata=True
    )
    
    return metadata.matches[0]["metadata"]["namespace"]


@TRACER.capture_lambda_handler
def lambda_handler(event, context):
    LOGGER.info(event)

    # Get Pinecone Index properties
    pinecone_props = get_secret(
        secret_arn=SECRET_ARN
    )
    api_key = pinecone_props["PINECONE_API_KEY"]
    index_name = pinecone_props["PINECONE_INDEX_NAME"]

    # Connect to the Pinecone Index
    LOGGER.info("Connecting to the Pinecone Index ...")
    pc = Pinecone(api_key=api_key)
    index = pc.Index(index_name)
    
    # Get the news event from the Kinesis Stream
    for record in event["Records"]:
        try:
            LOGGER.info(f"Processing Kinesis Record: {record['eventID']}")
            record_data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            LOGGER.info(f"Record Data: {record_data}")
            record_body = json.loads(record_data)

            # Create chunks for the record `event`. Use "fixed size chunking" strategy to
            # convert the `event` data into a list of chunks.
            chunks = get_chunks(text=record_body["event"])

            # Create a list of contextual text, and chunk id, by applying the context to each chunk, using Contextual Retrieval
            # SEE: https://www.anthropic.com/news/contextual-retrieval
            data = []
            for idx, chunk in enumerate(chunks):
                # Create the contextual prompt format
                prompt = contextual_prompt.format(
                    doc_content=record_body["event"],
                    chunk_content=chunk
                )

                # Get the chuck context from the LLM
                response_stream = INFERENCE_ADAPTER.invoke_model(prompt=prompt)

                # Add context to each chunk
                contextual_chunk = "".join(response for response in response_stream if response)

                # Update the dataset
                data.append(
                    {
                        "id": f"{record_body['event_id']}-{idx}",
                        "text": contextual_chunk + "\n\n" + chunk,
                    }
                )
            
            # Generate embeddings from the contextual chunks
            # NOTE: This is a single call to Pinecone Inference for the entire list of contextual chunks,
            #       as opposed to a single chunk, which can lead to timeouts.
            embeddings = get_embeddings(
                pc=pc,
                texts=[d["text"] for d in data],
                input_type="passage"
            )
            
            # Get the name of the namespace to use, by supplying the event summary, as a vector,
            # and retrieving the semantic classification for similar headlines.
            namespace = get_namespace(pc=pc, index=index, summary=record_body["summary"])

            # `upsert()` the vector into the Pinecone Index, using the classification namespace.
            LOGGER.info(f"Adding Record Vectors to Pinecone Namespace: {namespace}")
            records = []
            for d, e in zip(data, embeddings):
                records.append(
                    {
                        "id": d["id"],
                        "values": e["values"],
                        "metadata": {
                            "event_id": record_body["event_id"],
                            "text": d["text"],
                            "summary": record_body["summary"],
                            "updated_at": record_body["updated_at"]
                        }
                    }
                )
            index.upsert(vectors=records, namespace=namespace)
            
        except Exception as e:
            LOGGER.error(f"Error Message: {e}")
            raise e
    
    LOGGER.info(f"Successfully processed {len(event['Records'])} records.")