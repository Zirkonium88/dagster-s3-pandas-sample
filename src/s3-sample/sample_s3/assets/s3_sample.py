import numpy as np
import os
import boto3
import pandas as pd
from dagster import asset
import logging

log_level = os.environ.get("LOG_LEVEL", os.environ["LOG_LEVEL"])
logging.root.setLevel(logging.getLevelName(log_level))
logger = logging.getLogger(__name__)


def assume_role() -> dict:
    """Assume am IAM role.

    Returns: dict with AWS tokens

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
    except Exception as e:
        logger.exception(e)
        raise

    return response

@asset
def create_df() -> pd.DataFrame:
    """Create a pd.DataFrame with random values.

    Returns: df, pd.DataFrame holding the random values

    """
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    return df

@asset
def upload_df(create_df) -> None:
    """Upload a pd.DataFrame as CSV to S3.

    Args:
        create_df: function, to create a pd.DataFrame

    Returns:

    """
    try:
        bucket_name = os.environ["S3_BUCKET"]
    except KeyError as e:
        logger.exception(e)
        raise

    df = create_df()
    response=assume_role()
    access_key = response["Credentials"]["AccessKeyId"]
    secret_access_key = response["Credentials"]["SecretAccessKey"]
    session_token = response["Credentials"]["SessionToken"]

    try:
        df.to_csv(
            f"s3://{bucket_name}/df.csv",
            index=False,
            storage_options={
                "key": access_key,
                "secret": secret_access_key,
                "token": session_token,
            },
        )
    except Exception as e:
        logger.exception(e)
        raise