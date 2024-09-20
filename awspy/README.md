
# Welcome to your CDK Python project!

This is a blank project for CDK development with Python.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ python3 -m pip install -r requirements.txt
```
https://docs.aws.amazon.com/cdk/v2/guide/work-with-cdk-python.html

At this point you can now synthesize the CloudFormation template for this code.


# Deploying the infrastructure

Increase API Gateway timeout quota:
```
aws service-quotas request-service-quota-increase --service-code 'apigateway' --quota-code 'L-E5AE38E3' --desired-value 120000 --profile 533267378851_AdministratorAccess --region eu-west-3`
```
Then deploy using:
```
cdk synth
cdk bootstrap --profile <profile to use in .aws/credentials>
cdk deploy --profile <profile to use in .aws/credentials>
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation


# Documentation
## Setup API Gateway REST API

https://docs.aws.amazon.com/apigateway/latest/developerguide/getting-started-with-private-integration.html
fix privatelink
https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-nlb-for-vpclink-using-console.html

### setup routes
https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-create-api-step-by-step.html
https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-routes.html

### api gateway max timeout increase
https://aws.amazon.com/about-aws/whats-new/2024/06/amazon-api-gateway-integration-timeout-limit-29-seconds/
https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html#api-gateway-execution-service-limits-table
https://raviintodia.medium.com/aws-api-gateway-extending-timeout-limits-beyond-29-seconds-b8c947f8e84c#:~:text=If%20the%20current%20quota%20is,await%20a%20response%20from%20AWS

## Auth to API Gateway
command to test the API:
```
aws apigateway test-invoke-method --rest-api-id "<api id>" --http-method "GET" --resource-id "<api resource id>" --profile <profile name> --region eu-west-3
```

https://github.com/aws-samples/sigv4-signing-examples/blob/main/sdk/python/main.py

### additional docs
https://docs.aws.amazon.com/amazonglacier/latest/dev/amazon-glacier-signing-requests.html#SignatureCalculationTask1


# VPC setup
https://stackoverflow.com/questions/64469544/is-there-a-way-to-not-allocate-an-elastic-ip-eip-when-creating-a-vpc-using-aws

