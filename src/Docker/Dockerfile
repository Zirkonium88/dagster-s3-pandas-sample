FROM python:3.8-slim

ARG iam_role_arn
ARG s3_bucket_name

ENV IAM_ROLE_ARN=$iam_role_arn
ENV S3_BUCKET=$s3_bucket_name

COPY docker-client-config.json /root/.docker/config.json

WORKDIR /opt/dagster/app

RUN apt-get update && apt-get install -y git

RUN apt install -y default-jre

ADD src/s3-sample/ .

COPY setup.py .

RUN pip install -e .
