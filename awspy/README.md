
## Deploying the infrastructure

### Prerequisites

- Install Node.js 14.15.0 or later
Then run 
```
npm install -g aws-cdk
```
Verify installation
```
cdk --version
```
The CDK CLI will use your security credentials to authenticate with AWS, to configure them you need to run the following:
```
aws configure
```
and then enter you access key ID and secret.
After this your credentials will be saved in `~/.aws/credentials` and your configuration in `~/.aws/config`.

#### Sources
https://docs.aws.amazon.com/cdk/v2/guide/prerequisites.html
https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-authentication.html

### Deployment

The `cdk.json` file tells the CDK Toolkit how to execute your app.
At this point you can now synthesize the CloudFormation template for this code:
```
cdk synth
```
Deploy using:
```
cdk bootstrap --profile <profile to use in ~/.aws/credentials>
cdk deploy --profile <profile to use in ~/.aws/credentials>
```
To update the stack just rerun the above commands after modifying the code.

#### Sources
https://docs.aws.amazon.com/cdk/v2/guide/work-with-cdk-python.html

### Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation


## Architecture
We use mainly three components:
- DBT server: deployed as a container in an instance
- Nginx reverse proxy: deployed as a container in the same instance as the DBT server
- Lambda function: used for authenticating users
<center><img src="./docs/archi.png" width="100%"></center>
As the above image suggests any request coming from the outside needs to go through the reverse proxy which only forwards requests to the server if the lambda function succeeds at authenticating the request. 


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

Increase API Gateway timeout quota:
```
aws service-quotas request-service-quota-increase --service-code 'apigateway' --quota-code 'L-E5AE38E3' --desired-value 60000 --profile <profile name> --region eu-west-3
```

### additional docs
https://docs.aws.amazon.com/amazonglacier/latest/dev/amazon-glacier-signing-requests.html#SignatureCalculationTask1


# VPC setup
https://stackoverflow.com/questions/64469544/is-there-a-way-to-not-allocate-an-elastic-ip-eip-when-creating-a-vpc-using-aws


## Use lambda functions for authentication


