import numpy as np
import os
import boto3
from io import StringIO
import pandas as pd
from dagster import op, Field, Int
import logging
import time
from botocore.exceptions import ClientError

sts = boto3.client("sts")


def build_s3_client(sts) -> boto3.client:
    """Create a S3 boto3 resource object.
    Args:
        sts: sts boto3 client
    Returns: s3_client, boto3.client object for S3

    """
    try:
        role_arn = os.environ["IAM_ROLE_ARN"]
    except KeyError as e:
        logging.exception(e)
        raise
    try:
        response = sts.assume_role(
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
    config_schema={
        "random_min_size": Field(Int, is_required=True),
        "random_max_size": Field(Int, is_required=True),
        "n_rows": Field(Int, is_required=True),
        "n_cols": Field(Int, is_required=True),
        "col_names": Field([str], is_required=False)
    }
)
def create_dataframe(context) -> pd.DataFrame:
    """Create a pd.DataFrame with random values.

    Args:
        context:

    Return: df, pd.DataFrame holding the random values

    """
    assert context.op_config["n_cols"] == len(context.op_config["col_names"]), f"Error: number of columns {context.op_config['n_cols']} is not equal to length of list of columns {context.op_config['col_names']}"
    df = pd.DataFrame(
        np.random.randint(
            context.op_config["random_min_size"],
            context.op_config["random_max_size"],
            size=(context.op_config["n_rows"], context.op_config["n_cols"])
        ),
        columns=context.op_config["col_names"]
    )
    return df


@op
def upload_dataframe(created_dataframe, s3_client) -> bool:
    """Upload a pd.DataFrame as CSV to S3.

    Args:
        created_dataframe: function, to create a pd.DataFrame,
        s3_client: s3 boto client
    Returns: True, if succeeded
    """
    try:
        bucket_name = os.environ["S3_BUCKET"]
    except KeyError as e:
        logging.exception(e)
        raise

    t = time.localtime()
    current_time = time.strftime("%H:%M:%S", t)
    file_name = f"./{current_time}-df.csv"
    csv_buffer = StringIO()
    created_dataframe.to_csv(csv_buffer)

    try:
        logging.info(f"Uploading file {file_name} to S3 bucket {bucket_name}")
        s3_client.put_object(Bucket=bucket_name, Body=csv_buffer.getvalue(), Key=f"s3://{bucket_name}/{file_name}")
    except ClientError as e:
        logging.exception(e)
        raise

    return True

