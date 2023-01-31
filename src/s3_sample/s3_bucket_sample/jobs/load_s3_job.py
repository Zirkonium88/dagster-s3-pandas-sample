import os
from dagster import job
from .ops import (
    create_dataframe,
    upload_dataframe,
)


@job(
    description="""
        Dieser Job (load_s3()) besteht aus zwei Op-Schritten, zuerst wird ein pd.DataFrame erzeut und dieses dann in 
        einem S3 Bucket geladen.
    """,
    config={
        "ops": {
            "create_dataframe": {
                "config": {
                    "random_min_size": 0,
                    "random_max_size": 100,
                    "n_rows": 0,
                    "n_cols": 4,
                }
            },
            "upload_dataframe": {
                "config": {
                    "bucket_name": os.getenv("S3_BUCKET", "mybucket"),
                    "role_arn": os.getenv("IAM_ROLE_ARN", "myiamrolearn"),
                }
            }
        }
    }
)
def load_s3() -> None:
    """Job definition for ops parts.

    Returns: message: str, success code

    """
    try:
        upload_dataframe(create_dataframe())
    except Exception as e:
        raise e
