FROM python:3.8-slim

ARG path

WORKDIR /opt/dagster/app

RUN apt-get update && apt-get install -y git

RUN apt install -y default-jre

RUN mkdir -p sample_s3/

COPY $path/src/sample_s3 sample_s3
COPY $path/setup.py .
COPY $path/dagster.yaml .
COPY $path/dagster_cloud.yaml .
COPY $path/workspace.yaml .

RUN pip install -e .