import os
import time
#import pyodbc
import boto3
import requests
from contextlib import contextmanager
from dotenv import load_dotenv
from functools import lru_cache

from davinci.utils.logging import log, logger
from davinci.utils.utils import _parse_doppler_list
from davinci.utils.global_config import SYSTEM, DOPPLER_KEY

load_dotenv()

def _build_doppler_http_connect(token):
    """
    Builds an https string pointed to doppler with auth in header.

    :param token: doppler token
    :type token: str
    :return: https_string, header_dict
    :rtype: str, dict
    """
    url = "https://api.doppler.com/v3/configs/config/secrets/download?format=json&include_dynamic_secrets=true&dynamic_secrets_ttl_sec=1800"
    headers = {
        "accept": "application/json",
        "authorization": f"Bearer {token}"
    }
    return url, headers

def _get_doppler_token():
    """
    Attempt to fetch doppler token from dev_tools/auth.py file.

    :return: token string
    :rtype: str
    """
    try:
        from davinci.dev_tools.auth import SECRETS
    except ImportError:
        raise ImportError("No auth.py file detected for secrets. Check the DaVinci pip package.")
    try:
        token = SECRETS[DOPPLER_KEY]
    except KeyError:
        raise KeyError("No token specified for secrets. Check the auth.py file.")
    return token


@lru_cache()
def _get_all_doppler_secrets():
    """
    Fetch and cache dictionary of all Doppler secrets.
    LRU cache is used here to minimize number of requests to Doppler.
    So if get_secret("MY_SECRET", doppler=True) is called at any point
    in a job, this function will only be called one time, then reference
    the cache for subsequent calls.

    :return: secrets dict
    :rtype: dict
    """
    # Fetch token from auth.py file
    token = _get_doppler_token()

    # Loop and exponential backoff for reliability against network issues
    response = False
    reconnect_counter = 0
    url, headers = _build_doppler_http_connect(token)
    while not response and reconnect_counter < 5:
        try:
            response = requests.get(url, headers=headers).json()
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
            logger.warning("Issue with Doppler connection... retrying.")
            logger.info(str(e))
        reconnect_counter += 1
        time.sleep(2 ** reconnect_counter)
    if not response:
        raise ValueError("Could not connect to Doppler.")
    return response

def _get_doppler_secret(key: str):
    """
    Fetch a single secret using doppler backend.
    If the secret has semicolons, those will be
    interpreted as delimiters and return a list.

    .. warning::
        All returns from this function are either a string
        or a list of strings. User must manually cast to
        int if needed.

    :return: secret
    :rtype: str or list[str]
    """
    secrets = _get_all_doppler_secrets()
    res = _parse_doppler_list(secrets[key])
    return res

@log()
def get_secret(key, prod=SYSTEM, doppler=True):
    """
    This secret-getter varies depending on prod/dev environment.
    In production, it is identical to using os.environ.get(key);
    in development, however, this function reads from 
    davinci.dev_tools.auth for credentials.

    .. warning::
        All returns from the doppler backend are either a string
        or a list of strings. User must manually cast to
        int if needed. This is different than the normal
        handling from a .env file.

    :param key: .env key to get value for.
    :type key: str
    :param prod: production boolean
    :type prod: bool
    :param doppler: use doppler boolean
    :type doppler: bool
    :return: str or list[str]
    """

    if doppler:
        return _get_doppler_secret(key)
    print(f"doppler:{doppler}")
    load_dotenv()
    if prod:
        res = os.environ.get(key)
    else:
        try:
            print("HERE")
            from davinci.dev_tools.auth import SECRETS
            res = SECRETS[key]  
        except ImportError as e:
            logger.error(f'Could not access env var: {key}. Please check the DaVinci pip package.')
            raise e
    return res

@contextmanager
@log()
def open_sql_connection(db: str='FINAL_SQL_DATABASE') -> None:
    """
    Opens a SQL connection via context-manager. This
    function automagically handles safely closing the connection.
    See usage for example.

    :param db: The database to connect to. Defaults to 'FINAL_SQL_DATABASE'.
        See .env for definitions.
    :type db: str
    :return: None

    Example usage:

        .. code-block:: python

            import davinci as dv

            with dv.services.auth.open_sql_connection() as conn:
                # Connection opened
                foo(conn, *args, **kwargs)
            # Connection closed
            
    """    
    try:
        # creating a connection string
        conn = pyodbc.connect(
            driver='{ODBC Driver 17 for SQL Server}',
            server=get_secret('SQL_SERVER'),
            database=get_secret(db),
            trusted_conn='no',
            uid=get_secret('SQL_USER'),
            pwd=get_secret('SQL_PASSWORD'),
        )
        yield conn
    except Exception as err:
        # Ensure conn variable exists if, e.g., VPN is off
        # then the error displayed is more obvious.
        conn = None
        raise err
    else:
        pass
    finally:
        conn.close() if conn else None

@log()
def get_s3_client():
    """
    Returns a boto3.client for s3 interaction.

    :return: boto3.client
    """
    boto3_login = {
            "verify": False,
            "service_name": 's3',
            "region_name": 'us-east-2',
            "aws_access_key_id": get_secret("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": get_secret("AWS_SECRET_ACCESS_KEY")
        }
    s3 = boto3.client(**boto3_login)
    return s3

@log()
def get_cognito_client():
    """
    Returns a boto3.client for cognito interaction.

    :return: boto3.client
    """
    boto3_login = {
            "verify": True,
            "service_name": 'cognito-idp',
            "region_name": 'us-east-1',
            "aws_access_key_id": get_secret("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": get_secret("AWS_SECRET_ACCESS_KEY")
        }
    cog = boto3.client(**boto3_login)
    return cog


def get_mlflow_tracking_uri():
    """
    Returns a string with the DaVinci mlflow tracking uri.

    :return: tracking uri string
    :rtype: str

    Example usage:

        .. code-block:: python

            import mlflow
            from davinci.services.auth import get_mlflow_tracking_uri

            tracking_uri = get_mlflow_tracking_uri()

            # This works...
            mlflow.set_tracking_uri(tracking_uri)

            # Or this works too.
            client = mlflow.tracking.MlflowClient(tracking_uri=tracking_uri)

    """
    return "http://{}:{}@{}/".format(
        get_secret('MLFLOW_USERNAME', doppler=True),
        get_secret('MLFLOW_PASSWORD', doppler=True),
        get_secret('MLFLOW_SERVER', doppler=True)
    )
