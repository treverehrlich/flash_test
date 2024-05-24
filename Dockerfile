#FROM public.ecr.aws/lambda/python:3.7
#FROM public.ecr.aws/lambda/python:3.11-arm64
FROM public.ecr.aws/lambda/python:3.10

#Allow statements and logs messages to appear
ENV PYTHONUNBUFFERED True

# Install the function's dependencies using file requirements.txt
# from your project folder.
RUN pip3 install --upgrade pip
COPY requirements.txt . 
RUN pip3 install -r requirements.txt --target ${LAMBDA_TASK_ROOT} -U --no-cache-dir --default-timeout=1000

ARG AWS_ACCOUNT_ID
ARG AWS_ECR_ACCESS_KEY_ID
ARG AWS_ECR_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION

RUN echo "here it is..."
RUN echo ${AWS_ACCOUNT_ID}

# Install AWS CLI
RUN yum install -y python3 && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install

# Set AWS credentials (replace placeholders with your actual AWS access key ID and secret access key)
RUN aws configure set aws_access_key_id ${AWS_ECR_ACCESS_KEY_ID} && \
    aws configure set aws_secret_access_key ${AWS_ECR_SECRET_ACCESS_KEY} && \
    aws configure set region ${AWS_DEFAULT_REGION}


# Copy project code
COPY ./ ${LAMBDA_TASK_ROOT}

# Install codebase
#RUN pip3 install --upgrade pip
# RUN pip3 install -e codebase --target ${LAMBDA_TASK_ROOT} --no-cache-dir --default-timeout=1000

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]