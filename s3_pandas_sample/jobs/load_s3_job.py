import os
from dagster import job
from .ops import (
    create_dataframe,
    upload_dataframe,
)


@job(
    description="""
        This Job (load_s3()) contains to Op steps. First a pd.DataFrame will be created and afterwards this one will
        be uploaded to a S3 Bucket.
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
