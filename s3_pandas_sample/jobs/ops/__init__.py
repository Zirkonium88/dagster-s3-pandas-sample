import numpy as np
import boto3
from io import StringIO
import pandas as pd
from dagster import op, Field, Int, String, Out, In
import time
from botocore.exceptions import ClientError


def build_s3_client() -> boto3.client:
    """Create a S3 boto3 resource object.
    Args:
        no args

    Returns: s3_client, boto3.client object for S3

    """
    s3_client = boto3.client("s3")

    return s3_client


@op(
    description="This Op creates a pd.Dataframe based on parameters.",
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
    description="This Op step uploads the created pd.DataFrame via the Code Location role to a S3 Bucket.",
    config_schema={
        "bucket_name": Field(String, is_required=True),
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
    s3_client = build_s3_client()

    try:
        context.log.info(f"Uploading file {file_name} to S3 bucket {bucket_name}")
        s3_client.put_object(Bucket=bucket_name, Body=csv_buffer.getvalue(), Key=file_name)
    except ClientError as e:
        context.log.error(e)
        raise

    return True
