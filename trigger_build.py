import boto3
import time
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

def start_and_monitor_build(project_name):
    # Start the build
    response = client.start_build(projectName=project_name)
    build_id = response['build']['id']
    print(f'Started build with ID: {build_id}')

    # Poll the build status
    while True:
        build_status = get_build_status(build_id)
        print(f'Build status: {build_status}')
        
        if build_status in ['SUCCEEDED', 'FAILED', 'FAULT', 'STOPPED', 'TIMED_OUT']:
            break
        
        # Wait before polling again
        time.sleep(10)
    
    print(f'Build {build_status}')
    return build_status

def get_build_status(build_id):
    # Get the build information
    response = client.batch_get_builds(ids=[build_id])
    
    if 'builds' in response and response['builds']:
        build = response['builds'][0]
        return build['buildStatus']
    else:
        raise Exception('Build not found')


client = get_CodeBuild_client() 

### TODO: email if the build status is not SUCCEEDED!

# Example usage
project_name = 'flash_build'
start_and_monitor_build(project_name)