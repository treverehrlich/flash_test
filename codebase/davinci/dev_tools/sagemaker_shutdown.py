import os
import sys
import re
import os
import boto3

from davinci.utils.logging import logger
from davinci.services.outlook import DavinciEmail
from davinci.services.auth import get_secret

DEV_TEAM = get_secret("DEV_TEAM", doppler=True)


def sagemaker_shutdown():

    boto3_login = {
                   "verify": False,
                   "service_name": 'sagemaker',
                   "region_name": 'us-east-2',
                   "aws_access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
                   "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY")
                   }

    sage = boto3.client(**boto3_login)
    sage.stop_notebook_instance(NotebookInstanceName=os.environ.get("AWS_SAGEMAKER_NOTEBOOK"))


if __name__ == '__main__':

    # Create email instance
    email = DavinciEmail("SageMaker Shutdown",
                         """<p style="font-family:Cambria"> Team,</p> 
                            <p style="font-family:Cambria"> Is anyone working on Sagemaker? Whoever working, 
                                                            can you respond "yes" and when you are done, 
                                                            please shutdown Sagemaker others do not respond.
                            <p style="font-family:Cambria"> Thank you for the help and support !! </p>
                                           """)

    email.send(DEV_TEAM)

    # Wait for a reply
    logger.info('waiting for team response')
    code, resp = email.await_response()

    resp = re.sub(r"[^a-zA-Z]", "", str(resp).lower())

    if resp == 'yes':
        logger.info('Someone is Working, so avoiding Shutdown')
        sys.exit()
    else:
        logger.info('Shutting the Sagemaker Instance')
        sagemaker_shutdown()

