import hmac, hashlib, base64 

from davinci.services.auth import get_secret, get_cognito_client

def _cognito_hasher(val, app_client_id, app_client_key):
    """
    Executes the AWS hashing logic defined here:
    https://docs.aws.amazon.com/cognito/latest/developerguide/signing-up-users-in-your-app.html#cognito-user-pools-computing-secret-hash

    :param val: value to act as username
    :type val: str
    :param app_client_id: App Client ID (AWS)
    :type app_client_id: str
    :param app_client_key: App Client Key (AWS)
    :type app_client_key: str
    :return: hashed secret
    :rtype: str
    """
    message = bytes(val+app_client_id,'utf-8') 
    key = bytes(app_client_key,'utf-8') 
    secret_hash = base64.b64encode(hmac.new(key, message, digestmod=hashlib.sha256).digest()).decode() 
    return secret_hash

def get_cognito_access_token(username, password, client=None):
    """
    Attempts to get a cognito access token for the user.

    :param username: username
    :type username: str
    :param password: password
    :type password: str
    :param client: optional pre-existing Cognito client
    :type client: boto3.client
    :return: access token string
    :rtype: str
    """
    if not client:
        client = get_cognito_client()
    app_client_id = get_secret("AWS_COGNITO_APP_CLIENT_ID", doppler=True)
    key = get_secret("AWS_COGNITO_APP_SECRET_KEY", doppler=True)
    secret_hash = _cognito_hasher(username, app_client_id, key) 

    # Initiating the Authentication, 
    response = client.initiate_auth(
        ClientId=app_client_id,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": username, "PASSWORD": password, "SECRET_HASH": secret_hash},
    )
    return response['AuthenticationResult']['AccessToken']


def get_cognito_user(token=None, client=None, **kwargs):
    """
    Attempts to get a cognito user with info.

    :param token: optional pre-existing token
    :type token: str
    :param client: optional pre-existing Cognito client
    :type client: boto3.client
    :param kwargs: username and password to pass to _get_cogntio_access_token
        if no token is provided.
    :return: response JSON as dict, will be empty on fail.
    :rtype: dict
    """
    try:
        client = get_cognito_client() if not client else client
        token = get_cognito_access_token(**kwargs) if not token else token
        response = client.get_user(AccessToken=token)
        return response
    except:
        return {}

def get_cognito_user_first_name(user=None, token=None, **kwargs):
    """
    Attempts to get a user's first name attribute.
    
    :param user: optional pre-existing user
    :type user: dict
    :param token: optional pre-existing token
    :type token: str
    :param kwargs: username and password to pass to _get_cogntio_access_token
        if no token is provided.
    :return: name attribute if found, otherwise "Unknown User"
    :rtype: str
    """
    if not user:
        token = get_cognito_access_token(**kwargs) if not token else token
        user = get_cognito_user(token) if not user else user
    user_attributes = user['UserAttributes']
    for d in user_attributes:
        for k in d:
            if k == 'Name' and d[k] == 'name':
                return d['Value']
    return "Unknown User"

def list_cognito_groups_for_user(user):
    """
    List user-groups for user. Doesn't require token
    since it uses an admin account in the background.
    
    :param user: username string
    :type user: str
    :return: list of user groups
    :rtype: list[str]
    """
    try:
        client = get_cognito_client()
        response = client.admin_list_groups_for_user(
            Username=user,
            UserPoolId=get_secret('AWS_COGNITO_USER_POOL_ID', doppler=True),
            Limit=59,
        )
        groups = response['Groups']
        group_list = [d['GroupName'] for d in groups]
        return group_list
    except:
        return []
        