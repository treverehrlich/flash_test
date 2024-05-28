#! /usr/bin/env bash

# Set the AWS account with the correct permissions.
AWS_ACCOUNT_ID=111111111
REGION='us-east-1'
ECR_NAME='davinci_endpoints'
# Build docker image based on Dockerfile. Tag it with required ECR information.
sudo docker build -t $AWS_ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$ECR_NAME:latest . -f Dockerfile