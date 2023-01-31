FROM hdi-registry.we.decodeinsurance.de/mdp-hd/amazonlinux:2

ARG iam_role_arn
ARG s3_bucket_name
ARG USER="dagster-user"
ARG UID="1000"
ARG GID="100"

# Change version for Python updates
ENV PYTHON_VERSION=python3.8
ENV IAM_ROLE_ARN=${iam_role_arn}
ENV S3_BUCKET=${s3_bucket_name}
ENV PATH=$PATH:/home/$USER/.local/bin

RUN amazon-linux-extras enable $PYTHON_VERSION &&\
    yum install -y $PYTHON_VERSION \
    python-pip3 \
    default-jre \
    git && \
    yum update -y && \
    yum upgrade -y && \
    useradd --create-home --shell /bin/bash --gid "${GID}" --uid ${UID} ${USER} && \
    yum clean all

RUN rm /usr/bin/python
RUN ln -s /usr/bin/$PYTHON_VERSION /usr/bin/python3
RUN ln -s /usr/bin/$PYTHON_VERSION /usr/bin/python

COPY src/Docker/docker-client-config.json /root/.docker/config.json
RUN chown -R $UID:$GID  /root/.docker/*
RUN chmod ug+rwx "$HOME/.docker" -R

RUN mkdir -p /opt/dagster/dagster_home/storage &&\
    chown -R $UID:$GID /opt/dagster/dagster_home/storage

WORKDIR /opt/dagster/app

ADD src/s3_sample/ .
COPY setup.py .
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel
RUN python3 -m pip install -e .

USER $USER