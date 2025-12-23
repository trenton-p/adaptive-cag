import os
import json
import time
import boto3

from pinecone import Pinecone
from botocore.exceptions import ClientError
from pinecone_plugins.inference.core.client.exceptions import PineconeApiException

REGION = os.environ["AWS_REGION"]
PINECONE_SECRET = os.environ["PINECONE_SECRET"]
PINECONE_EMBEDDING_MODEL = os.environ["PINECONE_EMBEDDING_MODEL"]

def get_secret():
    """
    Function to return the Pinecone secret values.

    Returns:
        dict: A dictionary of the pinecone environment variables.
    """
    # Get the Pinecone secret values
    try:
        response = boto3.client(
            "secretsmanager",
            region_name=REGION
        ).get_secret_value(
            SecretId=PINECONE_SECRET
        )

    except ClientError as e:
        raise e.response["Error"]["Message"]

    return json.loads(response["SecretString"])


def get_embeddings(pc: Pinecone, text: str, input_type: str):
    # TODO: Update to leverage Pinecone integrated inference.
    """
    Function to return the embeddings for a given str, using Pinecone Inference.

    Args:
        pc (Pinecone): Pinecone client SDK.
        text (str): The text to create an embeddings representation of.
        input_type (str): The embedding model input type "query" | "passage".
    
    Returns:
        List of vectors.
    """
    # Create embeddings (exponential backoff to avoid RateLimitError)
    for j in range(5):  # Max 5 retries
        try:
            embedding = pc.inference.embed(
                model=PINECONE_EMBEDDING_MODEL,
                inputs=[text],
                parameters={"input_type": input_type, "truncate": "END"}
            )
            passed = True
        except PineconeApiException:
            time.sleep(2**j)  # Wait 2^j seconds before retrying
            print("Retrying Pinecone Embedding Request ...")
            passed = False
    if not passed:
        raise RuntimeError("Failed to create embeddings!")
    
    return embedding[0].values


def get_namespace(text: str):
    """
    Function to retrieve the Pinecone Namespace for retrieve contextual data.
    NOTE: This function matches the similar mechanism for initial contextual data ingest.

    Args:
        text (str): The user text for which to find similar data.
    
    Returns:
        namespace (str): The metadata value corresponding to the the namespace in which
                         to retrieve the context.
    """

    # Configure the Pinecone client
    pinecone_props = get_secret()
    api_key = pinecone_props["PINECONE_API_KEY"]
    # index_name = pinecone_props["PINECONE_INDEX_NAME"]
    # NOTE: Hard-coded to the PROD index for demo purposes
    index_name = os.environ["PINECONE_INDEX_NAME"].lower()
    pc = Pinecone(api_key=api_key)
    index = pc.Index(name=index_name)

    # Get embedding representation of the summary
    embedding = get_embeddings(
        pc=pc,
        text=text,
        input_type="query"
    )

    # Use the embedding representation to get the top 5 vectors
    query_results = index.query(
        namespace="router",
        vector=embedding,
        top_k=5,
        include_values=False,
        include_metadata=True
    )

    # Re-rank the top 3 documents 
    ranked_results = pc.inference.rerank(
        model="pinecone-rerank-v0",
        query=text,
        documents=[
            {
                "id": x["id"],
                "text": x["metadata"]["text"]
            } for x in query_results["matches"]
        ],
        top_n=3,
        return_documents=True,
    )

    # Query the namespace of the top ranked document id
    metadata = index.query(
        namespace="router",
        id=ranked_results.data[0]["document"]["id"],
        top_k=1,
        include_values=False,
        include_metadata=True
    )

    return metadata.matches[0]["metadata"]["namespace"]


def get_context(text: str, namespace: str):
    """
    Function to retrieve the semantic contextual data.

    Args:
        text (str): The user text for which to find similar data.
        namespace (str): The Pinecone namespace to query.
    
    Returns:
        context (str): The metadata value corresponding to the "best" vector in which
        to retrieve the context.
    """

    # Configure the Pinecone client
    pinecone_props = get_secret()
    api_key = pinecone_props["PINECONE_API_KEY"]
    # index_name = pinecone_props["PINECONE_INDEX_NAME"]
    # NOTE: Hard-coded to the PROD index for demo purposes
    index_name = os.environ["PINECONE_INDEX_NAME"].lower()
    pc = Pinecone(api_key=api_key)
    index = pc.Index(name=index_name)

    # Get embedding representation of the summary
    embedding = get_embeddings(
        pc=pc,
        text=text,
        input_type="query"
    )

    # Use the embedding representation to get the top 10 vectors
    query_results = index.query(
        namespace=namespace,
        vector=embedding,
        top_k=10,
        include_values=False,
        include_metadata=True
    )

    # Re-rank the top 3 documents 
    ranked_results = pc.inference.rerank(
        model="pinecone-rerank-v0",
        query=text,
        documents=[
            {
                "id": x["id"],
                "text": x["metadata"]["text"]
            } for x in query_results["matches"]
        ],
        top_n=3,
        return_documents=True,
    )

    # Query the text metadata of the top ranked document id
    metadata = index.query(
        namespace=namespace,
        id=ranked_results.data[0]["document"]["id"],
        top_k=1,
        include_values=False,
        include_metadata=True
    )
    context= metadata.matches[0]["metadata"]["text"]

    return context