FROM public.ecr.aws/docker/library/python:3.12.0-slim-bullseye

ENV AWS_LWA_INVOKE_MODE=RESPONSE_STREAM

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

COPY --from=public.ecr.aws/awsguru/aws-lambda-adapter:0.8.4 /lambda-adapter /opt/extensions/lambda-adapter

COPY requirements.txt /tmp/pip-tmp/

RUN pip --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/requirements.txt \
   && rm -rf /tmp/pip-tmp

WORKDIR /app

RUN touch __init__.py

ADD ./app/ .

CMD ["python", "main.py"]