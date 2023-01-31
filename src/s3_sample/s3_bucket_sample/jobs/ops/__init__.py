import numpy as np
import boto3
from io import StringIO
import pandas as pd
from dagster import op, Field, Int, String, Out, In
import logging
import time
from botocore.exceptions import ClientError

sts = boto3.client("sts")


def build_s3_client(sts_client: boto3.client, role_arn: str) -> boto3.client:
    """Create a S3 boto3 resource object.
    Args:
        sts_client: sts boto3 client
        role_arn: str, IAM role ARN

    Returns: s3_client, boto3.client object for S3

    """
    try:
        response = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName="dagster-s3-sample",
        )
        access_key = response["Credentials"]["AccessKeyId"]
        secret_access_key = response["Credentials"]["SecretAccessKey"]
        session_token = response["Credentials"]["SessionToken"]
    except ClientError as e:
        logging.exception(e)
        raise

    s3_client = boto3.client(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token
    )

    return s3_client


@op(
    description="Dieser Op-Schritt erzeugt anhand von Parameter ein pd.Dataframe.",
    config_schema={
        "random_min_size": Field(Int, is_required=True),
        "random_max_size": Field(Int, is_required=True),
        "n_rows": Field(Int, is_required=True),
        "n_cols": Field(Int, is_required=True),
    },
    out={"created_dataframe": Out()}
)
def create_dataframe(context) -> pd.DataFrame:
    """Create a pd.DataFrame with random values.

    Args:
        context: dict, Dagster context object

    Return: df, pd.DataFrame holding the random values

    """
    context.log.info(f"""
        Creating random pd.Dataframe with parameters: random_min_size {context.op_config["random_min_size"]}, 
        random_max_size {context.op_config["random_max_size"]}, n_rows {context.op_config["n_rows"]}, 
        n_cols {context.op_config["n_cols"]}
    """)
    df = pd.DataFrame(
        np.random.randint(
            context.op_config["random_min_size"],
            context.op_config["random_max_size"],
            size=(context.op_config["n_rows"], context.op_config["n_cols"])
        ),
    )
    context.log.info("Created random pd.Dataframe")
    return df


@op(
    description="Dieser Op-Schritt lädt das erzeugte pd.DataFrame mittels einer Rolle nach S3 (build_s3_client)",
    config_schema={
        "bucket_name": Field(String, is_required=True),
        "role_arn": Field(String, is_required=True),
    },
    ins={"created_dataframe": In()}
)
def upload_dataframe(context, created_dataframe: pd.DataFrame) -> bool:
    """Upload a pd.DataFrame as CSV to S3.

    Args:
        context: dict, Dagster context object
        created_dataframe: pd.DataFrame to upload, pd.DataFrame created by create_dataframe(),

    Returns: True, if succeeded
    """

    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    file_name = f"{current_time}-df.csv"
    csv_buffer = StringIO()

    created_dataframe.to_csv(csv_buffer)
    bucket_name = context.op_config["bucket_name"]
    role_arn = context.op_config["role_arn"]
    s3_client = build_s3_client(sts_client=sts, role_arn=role_arn)
    try:
        context.log.info(f"Uploading file {file_name} to S3 bucket {bucket_name}")
        s3_client.put_object(Bucket=bucket_name, Body=csv_buffer.getvalue(), Key=file_name)
    except ClientError as e:
        context.log.error(e)
        raise

    return True
