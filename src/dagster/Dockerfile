FROM python:3.8-slim

WORKDIR /opt/dagster/app

RUN apt-get update && apt-get install -y git

RUN apt install -y default-jre

COPY src/sample_s3 sample_s3
COPY setup.py .
COPY dagster.yaml .
COPY dagster_cloud.yaml .
COPY workspace.yaml .

RUN pip install -e .