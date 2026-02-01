 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation


then build and push the image to ECR

aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 390403887041.dkr.ecr.us-east-2.amazonaws.com

docker build --platform=linux/amd64 -t edgar-batch-repo .

docker tag edgar-batch-repo:latest 390403887041.dkr.ecr.us-east-2.amazonaws.com/edgar-batch-repo:latest

docker push 390403887041.dkr.ecr.us-east-2.amazonaws.com/edgar-batch-repo:latest