import aws_cdk as core
import aws_cdk.assertions as assertions

from awspy.awspy_stack import AwspyStack

# example tests. To run these tests, uncomment this file along with the example
# resource in awspy/awspy_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AwspyStack(app, "awspy")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
