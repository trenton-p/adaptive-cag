# NOTE: This is based on the official guide frm AWS.
# SEE: https://github.com/anthropics/anthropic-cookbook/blob/main/skills/contextual-embeddings/contextual-rag-lambda-function/inference_adapter.py

import boto3
import json

from botocore.config import Config
from botocore.exceptions import ClientError

class BedrockStreamAdapter:

    def __init__(self, text_model, region):
        self.region = region
        self.text_model = text_model
        # self.embedding_model = embedding_model

    def invoke_model(self, prompt, max_tokens=1000):
        bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name=self.region,
            config=Config(connect_timeout=5, read_timeout=60, retries={"total_max_attempts": 20, "mode": "adaptive"})
        )

        # Request format for `anthropic.claude-3-haiku-20240307-v1:0`
        request_body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "temperature": 0.0,
            }
        )

        # Invoke the model
        try:
            response = bedrock_runtime.invoke_model_with_response_stream(
                modelId=self.text_model,
                contentType="application/json",
                accept="application/json",
                body=request_body
            )

            for event in response.get("body"):
                chunk = json.loads(event["chunk"]["bytes"].decode())
                if chunk["type"] == "content_block_delta":
                    yield chunk["delta"]["text"]
                elif chunk["type"] == "message_delta":
                    if "stop_reason" in chunk["delta"]:
                        break

        except ClientError as e:
            raise e