
# ACME News Demo - Pinecone Adaptive-RAG Agent

## Overview

This repository contains the source code for a sample website for a fictitious company called **ACME News**. The website is based on the use case example from the book, [Automated Machine Learning on AWS](https://www.packtpub.com/product/automated-machine-learning-on-aws/9781801811828?utm_source=github&utm_medium=repository&utm_campaign=9781801811828), published by Packt, and will form the foundation on which to further build an [Adaptive-RAG](https://arxiv.org/pdf/2403.14403) Generative AI Agent, using [Pinecone](https://pinecone.io) semantic classification on AWS.

![architecture](./docs/architecture.jpg)

## Pre-requisites

**1. A Pinecone Account**

To create a Pinecone Account, navigate to https://pinecone.io, and click on the ***Sign Up*** button. Follow the simple, and intuitive flow to create an account. After logging in, folow the instructions to [Create and API key](https://docs.pinecone.io/guides/projects/manage-api-keys#create-an-api-key). Update the `constants.py` file, by adding the newly created API key as the value for the `PINECONE_API_KEY` variable.

**2. An AWS Account** 

Use and existing AWS Account, or create a new one, to ensure the following:
- Use the AWS Management Console to [enable access to Bedrock foundation models](https://docs.aws.amazon.com/bedrock/latest/userguide/model-access-modify.html). At a minimum, ensure the `anthropic.claude-3-haiku-20240307-v1:0` is enabled.
- Use the AWS Management to [create and S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html). Update the `constants.py` file with the name of the newly created bucket, as the value for the `DATA_LAKE_BUCKET` variable.
- Create a parquet formatted representation for the [ag_news](https://huggingface.co/datasets/sh0416/ag_news) dataset. This dataset will be used to semantically classify the correct Pinecone Namespace to ***"route"*** the retrieval request to. For more infomration on how to create the parquet data, see the [example](https://huggingface.co/datasets/sh0416/ag_news) notebook.
- Upload the parquet data to the `vector-exports/router` folder of the previously created S3 bucket. Update the `constants.py` file with the full S3 URI to the `vector-exports` folder, as the value for the `PINECONE_IMPORT_URI` variable. e.g. `s3://acme-news-datalake-012345678910/vector-exports`.
- Configure the [S3 Integration](https://docs.pinecone.io/guides/operations/integrations/integrate-with-amazon-s3), and update the `PINECONE_INTEGRATION_ID` variable in the `constants.py` file.
- Subscribe to the [Apache Iceberg Connector for AWS Glue](https://aws.amazon.com/marketplace/pp/prodview-iicxofvpqvsio) in the AWS Marketplace.

**3. Additional Variables**

Update the `constants.py` file with the following additional variables:
- `CONTACT_EMAIL` - The email address for the recipient of any contact requests from the deployed website.
- `PINECONE_PROD_INDEX` - The name of the Pinecone Index, that will be created.

**4. Third-party Tools**

Before deploying the solution, ensure that the following required tools have been installed:

- AWS Cloud Development Kit (CDK) >= 2.179.0
- Python >= 3.10
- NodeJS >= 18

>__NOTE:__ The solution has been tested using AWS CDK version 2.179.0. If you wish to update the CDK application to later version, make sure to update the `requirements.txt` file, in the root of the repository, with the updated version of the AWS CDK.

## Deployment

Run the following steps synthesize the code, in order to verify functionality before deployment.

>__NOTE:__ Before the solution can be successfully deployed, the AWS environment my be prepared for usage with the AWS CDK. See the [Bootstrap your environment for use with the AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/bootstrapping-env.html) for more information.

1. Manually create a virtualenv on MacOS and Linux:

    ```
    $ python3 -m venv .venv
    ```

2. After the init process completes and the virtualenv is created, use the following
step to activate the virtualenv.

    ```
    $ source .venv/bin/activate
    ```

3. Once the virtualenv is activated, install the required dependencies.

    ```
    $ pip install -r requirements.txt
    ```

4. At this point the solution can be synthesized, to create the CloudFormation template for this code.

    ```
    $ cdk synth
    ```

5. Deploy the solution.

    ```
    $ cdk deploy
    ```
