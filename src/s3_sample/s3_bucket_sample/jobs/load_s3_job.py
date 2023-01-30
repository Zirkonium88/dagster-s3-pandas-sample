import logging
import os

from dagster import job

from .ops import (
    build_s3_client,
    create_dataframe,
    upload_dataframe,
)

import boto3
sts = boto3.client("sts")


@job
def load_s3() -> None:
    """Job definition for ops parts.

    Returns: message: str, success code

    """
    logging.info("Starting S3 upload job ...")
    try:
        upload_dataframe(
            created_dataframe=create_dataframe(),
            s3_client=build_s3_client(sts),
            bucket_name=os.getenv("S3_BUCKET", None),
        )
    except Exception as e:
        logging.error(e)
    logging.info("Finished S3 upload job ...")
