FROM python:3.8-slim

WORKDIR /opt/dagster/app

RUN apt-get update && apt-get install -y git

RUN apt install -y default-jre

ADD src/s3-sample/ .

COPY setup.py .

RUN pip install -e .
