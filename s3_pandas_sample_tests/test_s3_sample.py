from unittest import mock
import pandas as pd
import numpy as np
from dagster import build_op_context
from unittest.mock import MagicMock, ANY
import os
import boto3


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


class TestS3Sample():

    def test_create_dataframe(self):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.load_s3_job import create_dataframe as uat
        test_context = build_op_context(
            config={
                "random_min_size": 0,
                "random_max_size": 100,
                "n_rows": 100,
                "n_cols": 4,
            }
        )

        # When
        response = uat(test_context)

        # Then
        assert type(response) == pd.DataFrame
        assert len(response) == 100
        assert len(response.columns) == 4

    def test_build_s3_client(self):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.ops import build_s3_client as uat
        sts = MagicMock()
        s3_client = boto3.client("s3")

        # When
        response = uat()

        # Then
        assert response.__class__.__name__ == s3_client.__class__.__name__

    @mockenv(
        S3_BUCKET="S3_BUCKET",
    )
    @mock.patch(
        "src.s3_sample.s3_bucket_sample.jobs.ops.build_s3_client"
    )
    def test_upload_dataframe(self, build_s3_client_mocked):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.load_s3_job import upload_dataframe as uat
        bucket_name = os.environ["S3_BUCKET"]
        build_s3_client_mocked.return_value = MagicMock()
        test_context = build_op_context(
            config={
                "bucket_name": bucket_name
            }
        )
        df = pd.DataFrame(
            np.random.randint(
                0,
                100,
                size=(100, 4)
            ),
            columns=list("ABCD")
        )

        # When
        response = uat(
            context=test_context,
            created_dataframe=df
        )

        # Then
        build_s3_client_mocked.Object(bucket_name, f"{bucket_name}/{ANY}").put(Body=ANY)
        assert response is True

    @mockenv(
        S3_BUCKET="S3_BUCKET",
    )
    @mock.patch(
        "src.s3_sample.s3_bucket_sample.jobs.ops.build_s3_client",
    )
    def test_load_s3(self, build_s3_client_mocked):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.load_s3_job import load_s3 as uat
        build_s3_client_mocked.return_value = MagicMock()

        # When
        result = uat.execute_in_process(
            run_config={
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
                            "bucket_name": os.environ["S3_BUCKET"],
                        }
                    }
                }
            }
        )

        # Then
        assert result.success is True

