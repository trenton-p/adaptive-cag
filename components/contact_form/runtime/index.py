import os
import logging
import json
import boto3
from botocore.exceptions import ClientError
from http import HTTPStatus

sns = boto3.client("sns")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(request, context):
    logger.info(f"Processing HTTP API Request: {json.dumps(request, indent=2)}")
    if request["requestContext"]["http"]["method"] == "POST":
        response_code, response_body = handle_request(request)
        return generate_response(request, response_body, response_code)
    else:
        logger.info("Request is not using POST method")
        return generate_response(request, json.dumps({"message:" "Unsupported method."}), HTTPStatus.BAD_REQUEST)


def generate_response(request, response_body, response_code):
    logger.info("Generating HTTP response:")
    response = {
        "body": response_body,
        "isBase64Encoded": request["isBase64Encoded"],
        "headers": request["headers"],
        "statusCode": response_code
    }
    logger.info(json.dumps(response, indent=2))
    return response


def handle_request(request):
    if request["rawPath"] == "/api/contact":
        logger.info("Processing Contact Form request.")
        return send_message(request)
    else:
        logger.info("Request outside of scope.")
        return HTTPStatus.BAD_REQUEST, json.dumps({"message": "Unsupported path."})


def send_message(request):
    request_body = json.loads(request["body"])
    notification = f"From: {request_body['email']}\n\nMessage: {request_body['question']}"
    logger.info(f"SNS Topic Submission: {notification}")
    try:
        response = sns.publish(
            TopicArn=os.environ["TOPIC_ARN"],
            Subject="New Comment from the ACME News Website",
            Message=notification
        )
        logger.info(f"SNS Send Response Code: {response['ResponseMetadata']['HTTPStatusCode']}")
        return HTTPStatus.OK, json.dumps(
            {
                "message": f"<b>Thank you!</b> We\'ve received your message, and we will be responding shortly."
            }
        )
    
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(f"SNS Send Response Error: {error_message}")
        return HTTPStatus.OK, json.dumps(
            {
                "message": "<b>Message Send Failure!</b> Please try again later."
            }
        )