import requests
from base64 import b64encode
from nacl import encoding, public
from prefect import flow, task
from davinci.services.auth import get_secret

# Global headers for GitHub API requests
HEADERS = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {get_secret('GITHUB_PERSONAL_ACCESS_TOKEN')}",
    "X-GitHub-Api-Version": "2022-11-28",
}

# Secrets dictionary containing various AWS and Doppler secrets
SECRETS = {
    'AWS_ACCESS_KEY_ID': get_secret('AWS_ECS_ACCOUNT_ACCESS_KEY', doppler=True),
    'AWS_ACCOUNT_ID': get_secret('AWS_ACCOUNT_ID', doppler=True),
    'AWS_REGION': get_secret('AWS_REGION', doppler=True),
    'AWS_REGISTRY_URL': get_secret('AWS_REGISTRY_URL', doppler=True),
    'AWS_SECRET_ACCESS_KEY': get_secret('AWS_ECS_ACCOUNT_SECRET_KEY', doppler=True),
    'DOPPLER_API_KEY_DEV': get_secret('DOPPLER_API_KEY_DEV', doppler=True),
    'DOPPLER_API_KEY_PROD': get_secret('DOPPLER_API_KEY_PROD', doppler=True),
    'PREFECT_CLI_LOGIN': get_secret('PREFECT_CLI_LOGIN', doppler=True),
}

def encrypt(public_key: str, secret_value: str) -> str:
    """
    Encrypts a Unicode string using the provided public key.

    Parameters:
    - public_key (str): Public key used for encryption.
    - secret_value (str): Unicode string to be encrypted.

    Returns:
    - str: Encrypted string.
    """
    public_key = public.PublicKey(public_key.encode("utf-8"), encoding.Base64Encoder())
    sealed_box = public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return b64encode(encrypted).decode("utf-8")

def get_repo_key(repo):
    """
    Retrieves the public key for the GitHub repository's actions secrets.

    Parameters:
    - repo (str): Full name of the GitHub repository.

    Returns:
    - dict: JSON response containing repository public key information.
    """
    url = f"https://api.github.com/repos/{repo}/actions/secrets/public-key"
    response = requests.get(url, headers=HEADERS)
    return response.json()

def write_repo_secret(repo, repo_keys, secret_id, secret_val):
    """
    Writes a secret to the GitHub repository using the provided keys and values.

    Parameters:
    - repo (str): Full name of the GitHub repository.
    - repo_keys (dict): Public key information for the repository.
    - secret_id (str): Identifier for the secret.
    - secret_val (str): Value of the secret.

    Returns:
    - Response object from the GitHub API.
    """
    url = f"https://api.github.com/repos/{repo}/actions/secrets/{secret_id}"

    repo_key = repo_keys['key']
    repo_key_id = repo_keys['key_id']
    encrypted_secret = encrypt(repo_key, secret_val)
    data = {
        "encrypted_value": encrypted_secret,
        "key_id": repo_key_id,
    }
    response = requests.put(url, headers=HEADERS, json=data)
    return response

@task
def list_git_repos(org):
    """
    Lists all repositories for a given GitHub organization.

    Parameters:
    - org (str): GitHub organization name.

    Returns:
    - List of repository full names.
    """
    url = f"https://api.github.com/orgs/{org}/repos"
    response = requests.get(url, headers=HEADERS)
    response = response.json()
    response = list(map(lambda r: r['full_name'], response))
    return response

def write_secrets_to_repo(repo):
    """
    Writes secrets to a specific GitHub repository.

    Parameters:
    - repo (str): Full name of the GitHub repository.
    - secrets (dict): Dictionary containing secrets to be written.

    Returns:
    - HTTP status code (int) indicating the success of the operation.
    """
    repo_keys = get_repo_key(repo)
    for secret in SECRETS:
        write_repo_secret(repo, repo_keys, secret, SECRETS[secret])
    return 200

@task
def write_secrets_to_repos(repos):
    """
    Writes secrets to multiple GitHub repositories.

    Parameters:
    - repos (list): List of GitHub repository full names.
    """
    for repo in repos:
        write_secrets_to_repo(repo)

@flow(name='Sync GitHub Secrets')
def main():
    """
    Main Prefect flow to sync GitHub secrets to repositories.
    """
    all_git_repos = list_git_repos('kencologistics')
    write_secrets_to_repos(all_git_repos)