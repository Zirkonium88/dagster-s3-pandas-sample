import logging
from dagster import job

from .ops import (
    build_s3_client,
    create_dataframe,
    upload_dataframe,
)

import boto3
sts = boto3.client("sts")


@job(
    config={
        "ops": {
            "create_dataframe": {
                "config": {
                    "random_min_size": 0,
                    "random_max_size": 100,
                    "n_rows": 0,
                    "n_cols": 4,
                    "col_names": ["ABCD"]
                }
            },
        }
    }
)
def load_s3() -> None:
    """Job definition for ops pats.

    Returns: message: str, success code

    """
    logging.info("Starting S3 upload job ...")
    try:
        upload_dataframe(
            created_dataframe=create_dataframe(),
            s3_client=build_s3_client(sts)
        )
    except Exception as e:
        logging.error(e)
    logging.info("Finished S3 upload job ...")
