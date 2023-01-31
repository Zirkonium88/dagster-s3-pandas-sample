FROM public.ecr.aws/amazonlinux/amazonlinux:2

# Change version for Python updates
ENV PYTHON_VERSION=python3.8
ENV CDK_VERSION=aws-cdk@2.50.0
ENV USER=cicd
ENV USERGROUP=docker
ENV PATH=$PATH:/home/$USER/.local/bin


RUN amazon-linux-extras enable $PYTHON_VERSION &&\
    yum install -y $PYTHON_VERSION \
    python-pip3 && \
    yum update -y && \
    yum upgrade -y && \
    yum clean all

RUN ln -s /usr/bin/$PYTHON_VERSION /usr/bin/python3
