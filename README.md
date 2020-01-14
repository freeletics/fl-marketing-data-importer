# fl-marketing-data-importer

In this repo, we will develop a cron job that will be sceduled to import the marketing data from `facebook` and `apple`.

#Development
Developers are advised to enable virtual environment and install the dependeces using `requirements.txt`:
```shell script
virtualenv venv
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
```

We will use AWS `ECS` and `fargate` to secdule the job based on cloudwatch events. to be able to develop the job, we need to do the following steps:

```
1-create a docker image
2-upload it to ECR.
3-crteate a task difinition in ECS using fargate as launch type.
  * Task Definition is where you will provide the parameters for the task — image, environment, command etc.
  * Fargate will let you run containers without managing servers, and you will only pay for the resources (CPU and memory) that the job uses.
4- create a CloudWatch  events and choose ECS task as target instead of lambda.

```

## Deployment.
In this section, we will show how to create docker image and push it to `ECR`.

### login into docker repo, in the "Frankfurt" data center, eu-central-1:
```
$(aws ecr get-login --no-include-email --region eu-central-1)
```

### Build docker image:
```
docker build -t fl-marketing-data-importer .
```

### Tage your image
```
docker tag fl-marketing-data-importer:latest 778443482073.dkr.ecr.eu-central-1.amazonaws.com/fl-marketing-data-importer:latest
```

### Push your docker image to ECR:
```
docker push 778443482073.dkr.ecr.eu-central-1.amazonaws.com/fl-marketing-data-importer:latest
```

## create a task difinition
Use the AWS console to create a task defnition to launch the docker image as shown here:

https://eu-central-1.console.aws.amazon.com/ecs/home?region=eu-central-1#/taskDefinitions

### to list task difintions
make sure that you have created the task definition by executing the following command:
```
aws ecs list-task-definitions --region eu-central-1
```

## Create a cloudwatch event 
In this step, you should create a cloudwatch event to scedule running the task that you have defined before as shown here:

https://eu-central-1.console.aws.amazon.com/cloudwatch/home?region=eu-central-1#rules:name=marketing-api-importer
