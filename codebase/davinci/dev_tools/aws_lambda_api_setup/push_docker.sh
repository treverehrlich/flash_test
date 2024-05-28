#! /usr/bin/env bash

source .env

# Set the AWS account with the correct permissions.
AWS_ACCOUNT_ID=111111111
REGION='us-east-1'
ECR_NAME='davinci_endpoints'

# For the below to work, install aws cli and use following: aws configure
# Put in credentials for the AWS_ACCOUNT_ID set above.
aws ecr get-login-password --region $REGION | sudo docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com
sudo docker push $AWS_ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$ECR_NAME:latest