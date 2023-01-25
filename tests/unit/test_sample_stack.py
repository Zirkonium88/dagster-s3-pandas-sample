import aws_cdk as core
import aws_cdk.assertions as assertions

from dagster_s3_sample.sample_stack import SampleStack

# example tests. To run these tests, uncomment this file along with the example
# resource in dagster_s3_sample/sample_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = SampleStack(app, "dagster-s3-sample")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
