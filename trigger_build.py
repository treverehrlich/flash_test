import boto3
from davinci.services.auth import get_secret

def get_CodeBuild_client():
    """
    Returns a boto3.client for CodeBuild interaction.

    :return: boto3.client
    """
    boto3_login = {
            "verify": False,
            "service_name": 'codebuild',
            "region_name": 'us-east-1',
            "aws_access_key_id": get_secret("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": get_secret("AWS_SECRET_ACCESS_KEY")
        }
    build = boto3.client(**boto3_login)
    return build

client = get_CodeBuild_client() 

# Start the build
response = client.start_build(
    projectName='flash_build',
)

print(response)