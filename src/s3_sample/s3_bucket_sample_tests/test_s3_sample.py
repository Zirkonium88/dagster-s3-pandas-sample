from unittest import mock
import pandas as pd
import numpy as np
from dagster import build_op_context
from unittest.mock import MagicMock, ANY
import pytest
import os
from botocore.exceptions import ClientError

@pytest.fixture()
def environment():
    os.environ = {
        "IAM_ROLE_ARN": "rolearn",
        "S3_BUCKET": "s3bucket",
    }


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

    def test_build_s3_client(self, environment):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.ops import build_s3_client as uat
        sts = MagicMock()
        role_arn = os.environ["IAM_ROLE_ARN"]

        # When
        response = uat(
            sts_client=sts,
            role_arn=role_arn
        )

        sts.assume_role.assert_called_with(
            RoleArn=role_arn,
            RoleSessionName="dagster-s3-sample",
        )

        assert response is not None

    def test_build_s3_client_client_error(self, environment):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.ops import build_s3_client as uat
        sts = MagicMock()
        role_arn = os.environ["IAM_ROLE_ARN"]

        sts.assume_role.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "ParamValidationError",
                    "Message": "Invalid request exception",
                }
            },
            operation_name="AssumeRole",
        )

        # When
        with pytest.raises(ClientError):
            uat(
                sts_client=sts,
                role_arn=role_arn
            )

    @mock.patch(
        "src.s3_sample.s3_bucket_sample.jobs.ops.build_s3_client",
        return_value=MagicMock()
    )
    def test_upload_dataframe(self, s3_mock, environment):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.load_s3_job import upload_dataframe as uat
        bucket_name = os.environ["S3_BUCKET"]
        role_arn = os.environ["IAM_ROLE_ARN"]
        test_context = build_op_context(
            config={
                "bucket_name": bucket_name,
                "role_arn": role_arn,
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
        s3_mock.Object(bucket_name, f"{bucket_name}/{ANY}").put(Body=ANY)
        assert response is True

    @mock.patch(
        "src.s3_sample.s3_bucket_sample.jobs.ops.build_s3_client",
        return_value=MagicMock()
    )
    def test_load_s3(self, environment):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.load_s3_job import load_s3 as uat

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
                            "role_arn": os.environ["IAM_ROLE_ARN"],
                        }
                    }
                }
            }
        )

        # Then
        assert result.success is True

    @mock.patch(
        "src.s3_sample.s3_bucket_sample.jobs.ops.upload_dataframe",
        return_value=Exception
    )
    def test_load_s3_exception(self, mocked_upload_dataframe, environment):
        # Give
        from src.s3_sample.s3_bucket_sample.jobs.load_s3_job import load_s3 as uat

        # When
        with pytest.raises(Exception):
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
                                "role_arn": os.environ["IAM_ROLE_ARN"],
                            }
                        }
                    }
                }
            )

            # Then
            assert result.success is False