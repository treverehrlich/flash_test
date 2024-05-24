#FROM public.ecr.aws/lambda/python:3.7
#FROM public.ecr.aws/lambda/python:3.11-arm64
FROM public.ecr.aws/lambda/python:3.10

#Allow statements and logs messages to appear
ENV PYTHONUNBUFFERED True

RUN pip3 install --upgrade pip
RUN pip3 install awscli

# ARG AWS_ACCOUNT_ID
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION

# Set environment variables for AWS credentials
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
ENV AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY requirements.txt . 
RUN pip3 install -r requirements.txt --target ${LAMBDA_TASK_ROOT} -U --no-cache-dir --default-timeout=1000

# Copy project code
COPY ./ ${LAMBDA_TASK_ROOT}

# Get the latest model files from S3
RUN aws s3 cp s3://aws-scs-prod-bucket/prod/avrl/pickle ${LAMBDA_TASK_ROOT}/.model_cache --recursive

RUN ls -a
RUN ls -a .model_cache

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]