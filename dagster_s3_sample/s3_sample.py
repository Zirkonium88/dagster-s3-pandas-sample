from aws_cdk import (
    Stack, RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_kms as kms,
    aws_ssm as ssm,
)
from constructs import Construct


class SampleStack(Stack):
    """Defines an architecture to run Dagster s3 example and it's dependencies."""

    def define_s3_bucket(self) -> dict:
        """Create S3 bucket for dagster-s3 sample.

        Returns: dict, holding the s3 Bucket and KMS Key construct.

        """
        data_bucket_key = kms.Key(
            self,
            id="DataBucketKey",
            enabled=True,
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.DESTROY,
            alias="Keys/DagsterSample",
        )
        cfn_key = data_bucket_key.node.default_child
        cfn_key.cfn_options.metadata = {
            "cfn_lint": {
                "rules_to_suppress": [
                    {"id": "EKMSSettings"},
                    {
                        "reason": "KMS keys will be deleted only in EW stages"
                    },
                ]
            }
        }

        data_bucket = s3.Bucket(
            self,
            id="DataBucket",
            encryption_key=data_bucket_key,
            enforce_ssl=True,
            versioned=True,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        ssm.StringParameter(
            self,
            id="DataBucketSSMOutput",
            parameter_name="/dagster/s3-sample/BucketName",
            string_value=data_bucket.bucket_name,
        )

        return {
            "Bucket": data_bucket,
            "BucketKey": data_bucket_key,
        }

    def define_iam_roles(self, bucket: s3.IBucket, key: kms.IKey) -> None:
        """Create IAM Role For dagster Code location.

        Returns: dict, holding both IAM Roles as iam.IRole

        """

        code_location_role = iam.Role(
            self,
            id="dagster-code-location-role",
            assumed_by=iam.AccountRootPrincipal()
        )

        bucket.grant_read_write(code_location_role)
        key.grant_encrypt_decrypt(code_location_role)

        ssm.StringParameter(
            self,
            id="IAMRoleSSMOutput",
            parameter_name="/dagster/s3-sample/RoleArn",
            string_value=code_location_role.role_arn,
        )

        return None

    def __init__(self, scope: Construct, construct_id: str, env_config, **kwargs) -> None:
        """Defines the CloudFormation stack.

        :param scope: Construct, CDK Python class to create the stack
        :param construct_id: str, name of the stack
        :param env_config: dict, holding information about the environment (branch, account id etc.)
        :param kwargs: additional arguments to define the stack
        """
        super().__init__(scope, construct_id, **kwargs)
        data_response = self.define_s3_bucket()

        self.define_iam_roles(
            bucket=data_response["Bucket"],
            key=data_response["BucketKey"],
        )