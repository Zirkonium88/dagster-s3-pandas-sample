FROM hdi-registry.we.decodeinsurance.de/mdp-hd/amazonlinux:2

ARG USER="dagster-user"
ARG UID="1000"
ARG GID="100"

# Change version for Python updates
ENV PYTHON_VERSION=python3.8
ENV PATH=$PATH:/home/$USER/.local/bin

RUN amazon-linux-extras enable $PYTHON_VERSION &&\
    yum install -y $PYTHON_VERSION  \
    python3-pip \
    shadow-utils && \
    yum update -y && \
    yum upgrade -y && \
    yum clean all

RUN useradd --create-home --shell /bin/bash --gid "${GID}" --uid ${UID} ${USER}

RUN rm /usr/bin/python
RUN ln -s /usr/bin/$PYTHON_VERSION /usr/bin/python

COPY docker-client-config.json /root/.docker/config.json
RUN chown -R $UID:$GID  /root/.docker/*
RUN chmod ug+rwx "$HOME/.docker" -R

RUN mkdir -p /opt/dagster/dagster_home/storage &&\
    chown -R $UID:$GID /opt/dagster/dagster_home/storage

WORKDIR /opt/dagster/app

ADD s3_pandas_sample/ .
COPY setup.py .
RUN python3 -m pip install -e .
RUN python3 -m pip install --no-cache-dir --upgrade pip setuptools wheel

USER $USER