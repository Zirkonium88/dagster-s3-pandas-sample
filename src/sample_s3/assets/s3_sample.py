import numpy as np
import os
import boto3
from io import StringIO
import pandas as pd
from dagster import asset
import logging

log_level = os.environ.get("LOG_LEVEL", os.environ["LOG_LEVEL"])
logging.root.setLevel(logging.getLevelName(log_level))
logger = logging.getLogger(__name__)


def build_s3_resource() -> boto3.resource:
    """Create a S3 boto3 resource object.

    Returns: s3_resource, boto3.resource object for S3

    """

    sts = boto3.client("sts")

    try:
        role_arn = os.environ["IAM_ROLE_ARN"]
    except KeyError as e:
        logger.exception(e)
        raise
    try:
        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="dagster-s3-sample",
        )
        access_key = response["Credentials"]["AccessKeyId"]
        secret_access_key = response["Credentials"]["SecretAccessKey"]
        session_token = response["Credentials"]["SessionToken"]
    except Exception as e:
        logger.exception(e)
        raise

    s3_resource = boto3.resource(
        service_name="s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token
    )

    return s3_resource

@asset
def create_df() -> pd.DataFrame:
    """Create a pd.DataFrame with random values.

    Returns: df, pd.DataFrame holding the random values

    """
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    return df

@asset
def upload_df(df=create_df) -> None:
    """Upload a pd.DataFrame as CSV to S3.

    Args:
        df: function, to create a pd.DataFrame

    Returns:

    """
    try:
        bucket_name = os.environ["S3_BUCKET"]
    except KeyError as e:
        logger.exception(e)
        raise

    csv_buffer = StringIO()
    df.to_csv(csv_buffer)

    s3=build_s3_resource()

    try:
        s3.Object(bucket_name, 'df.csv').put(Body=csv_buffer.getvalue())
    except Exception as e:
        logger.exception(e)
        raise