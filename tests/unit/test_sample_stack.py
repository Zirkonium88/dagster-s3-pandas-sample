import aws_cdk as core
import aws_cdk.assertions as assertions

from dagster_s3_sample.s3_sample import SampleStack

env_config = {
    "is_main_env_in_account": "True",
    "account": "12345678901",
    "region": "eu-central-1",
    "name": "SI3",
    "team": "MDP",
}

env = core.Environment(
    account=env_config["account"],
    region=env_config["region"],
)

app = core.App()
stack = SampleStack(app, "dagster-s3-sample", env=env, env_config=env_config)
template = assertions.Template.from_stack(stack)
template = assertions.Template.from_stack(stack)


class TestSampleClass:

    def test_iam_roles_created(self, template=template):
        template.resource_count_is("AWS::IAM::Role", 1)

    def test_s3_buckets_created(self, template=template):
        template.resource_count_is("AWS::S3::Bucket", 1)
        template.resource_count_is("AWS::KMS::Key", 1)