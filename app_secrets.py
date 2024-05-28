import boto3
import datetime
from dateutil.tz import tzlocal
import ast
client = boto3.client('secretsmanager')

#Sonar token
# SONAR_URL = 'https://api.freightwaves.com/RateCalculator/trac'
# DAT_AUTH_URL = 'https://identity.api.dat.com/access/v1/token/organization'
# DAT_ACCESS_URL = 'https://identity.api.dat.com/access/v1/token/user'
# DAT_LKP_URL = 'https://analytics.api.dat.com/linehaulrates/v1/lookups'
# EIA_URL = 'https://www.eia.gov/petroleum/gasdiesel/'
# SONAR_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6IlNhdGlzaC5WYWRsYW1hbmlAa2VuY29ncm91cC5jb20iLCJPcmdhbml6YXRpb25JRCI6IjM3MyIsIk1heFJlY29yZHMiOiIzNjUwMDAiLCJFbnRpdGxlbWVudCI6IlllcyIsIlVzZXJJZCI6IjQ0NyIsIm5iZiI6MTY5OTM4NDMxNCwiZXhwIjoxNzMwOTIwMzE0LCJpYXQiOjE2OTkzODQzMTR9.OievHRMpK95LPOXiItc4QBSrlyiXjEtczRDsI_gsTwY"
# DAT_USER = 'DATServiceAccount@kencogroup.com'
# DAT_PWD = 'V-OnEHO2X%BqpOw'
# DAT_API_USER = 'AVRLDAT'
# # secret key to make sure someone can't simply stumble upon our api and begin using it
# AVRL_API_KEY = 'o0kXae6x1kJjFtXn8T5CWlk88d7bxDuM' 
# AVRL_API_SANDBOX_KEY = 'cllyiuaYNW5arTsIW6SL7HsXX4wR322g'

try: 
    response = client.get_secret_value(
        SecretId='Flash_Secrets',
    )

    secret_str = ast.literal_eval(response['SecretString'])

    SONAR_URL = secret_str["SONAR_URL"]
    DAT_AUTH_URL = secret_str['DAT_AUTH_URL']
    DAT_ACCESS_URL = secret_str['DAT_ACCESS_URL']
    DAT_LKP_URL = secret_str['DAT_LKP_URL']
    EIA_URL = secret_str['EIA_URL']
    SONAR_TOKEN = secret_str['SONAR_TOKEN']
    DAT_USER = secret_str['DAT_USER']
    DAT_PWD = secret_str['DAT_PWD']
    DAT_API_USER = secret_str['DAT_API_USER']
    AVRL_API_KEY = secret_str['AVRL_API_KEY']
    AVRL_API_SANDBOX_KEY = secret_str['AVRL_API_SANDBOX_KEY']

except Exception as e:
    print(f"An unknown error occurred: {str(e)}.")
    raise
