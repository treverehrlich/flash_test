import boto3
from davinci.services.auth import get_secret
from prefect import flow, task, get_run_logger

LIFECYCLE_POLICY_TEXT = '''
    {
        "rules": [
            {
                "rulePriority": 1,
                "description": "Expire untagged images",
                "selection": {
                    "tagStatus": "untagged",
                    "countType": "imageCountMoreThan",
                    "countNumber": 1
                },
                "action": {
                    "type": "expire"
                }
            },
            {
                "rulePriority": 2,
                "description": "Expire excessive dev images",
                "selection": {
                    "tagStatus": "tagged",
                    "tagPatternList": ["dev*"],
                    "countType": "imageCountMoreThan",
                    "countNumber": 2
                },
                "action": {
                    "type": "expire"
                }
            }
        ]
    }
'''

def assign_lifecycle_policy(ecr_client, repository_name):
    """
    Assigns a lifecycle policy to a given ECR repository.

    Parameters:
    - ecr_client: boto3 ECR client
    - repository_name: Name of the ECR repository
    """
    logger = get_run_logger()
    try:
        ecr_client.put_lifecycle_policy(
            registryId=get_secret('AWS_ACCOUNT_ID', doppler=True),
            repositoryName=repository_name,
            lifecyclePolicyText=LIFECYCLE_POLICY_TEXT
        )
        logger.info(f"Lifecycle policy assigned successfully for repository: {repository_name}")
    except Exception as e:
        logger.info(f"Error assigning lifecycle policy for repository {repository_name}: {str(e)}")

@task
def assign_lifecycle_policies(ecr_client, repos):
    """
    Assigns lifecycle policies to a list of ECR repositories.

    Parameters:
    - ecr_client: boto3 ECR client
    - repos: List of ECR repository names
    """
    for repo in repos:
        assign_lifecycle_policy(ecr_client, repo)

@task
def list_kencologistics_repos(ecr_client, filter_criteria=lambda x: True):
    """
    Lists ECR repositories based on the provided filter criteria.

    Parameters:
    - ecr_client: boto3 ECR client
    - filter_criteria: A function to filter repositories, default accepts all

    Returns:
    - List of filtered repository names
    """
    response = ecr_client.describe_repositories()
    repositories = response['repositories']
    return list(filter(
        filter_criteria,
        map(
            lambda r: r['repositoryName'],
            repositories
        )
    ))

@flow(name='Apply ECR Lifecycle Policy')
def main():
    """
    Main Prefect flow to apply ECR lifecycle policy to repositories.
    """
    ecr_client = boto3.client(
        'ecr',
        aws_access_key_id=get_secret('AWS_ECS_ACCOUNT_ACCESS_KEY', doppler=True),
        aws_secret_access_key=get_secret('AWS_ECS_ACCOUNT_SECRET_KEY', doppler=True),
        region_name='us-east-1'
    )
    repos = list_kencologistics_repos(ecr_client, filter_criteria=lambda x: x.startswith('kencologistics/'))
    assign_lifecycle_policies(ecr_client, repos)
