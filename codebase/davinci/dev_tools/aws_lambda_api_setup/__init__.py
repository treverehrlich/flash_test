"""This is the template for launching an API
via AWS Lambda, API Gateway, and FastAPI.
Below are the step by step instructions.
Included in this template is a minimal FastAPI
construct; read more about FastAPI in their docs
to set up the API portion.

Steps
=====

.. contents::
  :local:
  :backlinks: none
  :depth: 1

0: FastAPI
----------

Design and test your FastAPI locally. See the FastAPI docs for more info.

1: IAM Permissions
------------------

Ensure the AWS account has the permissions: AmazonEC2ContainerServiceFullAccess, AmazonEC2ContainerRegistryFullAccess, AmazonEC2ContainerRegistryPowerUser 

2: Create ECR
-------------

Go to AWS service 'Elastic Container Registry'. Create new registry. This will be where Docker image of API is stored.

3: Modify Scripts
-----------------

Update both build_docker.sh and push_docker.sh to refer to the correct account, region, and ECR name.
Also update your Dockerfile to point to the correct requirements file and FastAPI entry point (api_endpoints.py by default).
Note that your Dockerfile may need more modifications for complex setups.
Ensure your requirements file includes:

    .. code-block:: python
        :caption: requirements.txt
        :linenos:

        fastapi==0.61.1
        mangum==0.10.0
        requests>=2.24.0
        uvicorn==0.12.2
        ...

Versions above aren't a hard-requirement, but they worked at time of writing.

4: AWS CLI
----------

Install the AWS CLI if not available on your machine. Then use:

    .. code-block:: shell
        :caption: AWS CLI
        :linenos:

        aws configure

to configure with the right credentials.

5: Create and Push Docker Image of FastAPI to ECR
-------------------------------------------------

This step packages your FastAPI setup as a Docker image.

    .. code-block:: shell
        :caption: Docker Scripts
        :linenos:

        . build_docker.sh
        . push_docker.sh

6: Create AWS Lambda Function
-----------------------------

Use the 'Container' option as a template and refer to the new Docker image in ECR.


7: API Gateway
--------------

Build new REST API (not the VPC one). Use the Actions dropdown to select 'Create Method' > 'Any'. Configure this method as a Lambda Function and Use Lambda Proxy Integration.
Use the Lambda function created above.

From the Actions dropdown, now select 'Create Resource'. Configure it as a proxy resource. Use Lambda Function Proxy, and refer to the same Lambda function again.

Now you should be able to Deploy API from the Actions dropdown. Give a stage name like 'dev' or 'prod'. Then you should be presented with a link to test your endpoint.

"""