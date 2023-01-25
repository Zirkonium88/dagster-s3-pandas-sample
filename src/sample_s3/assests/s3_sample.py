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

s3_resource= boto3.resource("s3")

@asset
def create_df() -> pd.DataFrame:
    """Create a pd.DataFrame with random values.

    Returns: df, pd.DataFrame holding the random values

    """
    df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))
    return df

@asset
def upload_df(s3: boto3.resource, df=create_df) -> None:
    """Upload a pd.DataFrame as CSV to S3.

    Args:
        s3: boto3.resource, for AWS S3 API calls
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

    try:
        s3.Object(bucket_name, 'df.csv').put(Body=csv_buffer.getvalue())
    except Exception as e:
        logger.exception(e)
        raise