import logging
import os
from dagster import job
from .ops import (
    create_dataframe,
    upload_dataframe,
)


@job(
    # input_values={
    #     "create_dataframe": {
    #         "config": {
    #             "random_min_size": 0,
    #             "random_max_size": 100,
    #             "n_rows": 0,
    #             "n_cols": 4,
    #         }
    #     },
    #     "upload_dataframe": {
    #         "config": {
    #             "bucket_name": os.getenv("S3_BUCKET", None),
    #             "role_arn": os.getenv("IAM_ROLE_ARN", None),
    #         }
    #     }
    # }
)
def load_s3() -> None:
    """Job definition for ops parts.

    Returns: message: str, success code

    """
    logging.info("Starting S3 upload job ...")
    try:
        upload_dataframe(create_dataframe())
    except Exception as e:
        logging.error(e)
    logging.info("Finished S3 upload job ...")

