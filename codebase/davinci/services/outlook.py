import os
import time
import multiprocessing
import pandas as pd
import random
import string
import pathlib
from typing import Tuple
from datetime import datetime, timedelta
from pretty_html_table import build_table
from bs4 import BeautifulSoup
from O365 import Account, FileSystemTokenBackend
from O365.mailbox import MailBox

from davinci.utils.logging import log, logger
from davinci.services.auth import get_secret

def _parse_html(html):
    parsed_html = BeautifulSoup(html, features="lxml")
    return parsed_html.body.find('div', attrs={'class':'WordSection1'}).text

def _periodic_matching_email_check(subject, queue, period=20):
    while True:
        resp, data = find_matching_email_contents(subject)
        if resp == 200:
            break
        time.sleep(period)
    parsed_data = _parse_html(data)
    try:
        clean_data = parsed_data.split('From: Ai, Davinci')[0].strip()
    except IndexError:
        clean_data = "Bad email read. No data."
    queue.put(clean_data)
    return clean_data

def _get_account():
    client_id = get_secret('GRAPH_CLIENT_ID')
    client_secret = get_secret('GRAPH_CLIENT_SECRET')
    tenant_id = get_secret('GRAPH_TENANT_ID')
    credentials = (client_id, client_secret)
    resource = get_secret('EMAIL_USER')
    token_backend = FileSystemTokenBackend(
        token_path=__file__.split('codebase')[0] + 'codebase/.tokens/', token_filename='my_token.txt'
    )
    account = Account(credentials, auth_flow_type='credentials',
                tenant_id=tenant_id, main_resource=resource,
                token_backend=token_backend)
    return account

def _get_authenticated_account():
    account = _get_account()
    if account.authenticate():
        return account
    else:
        raise AssertionError("Authentication step failed in _get_authenticated_account method.")

class DavinciEmail:
    """
    Construct an email, potentially with attachments,
    and send to Kenco recipients.

    :param subject: Subject Line of the email.
    :type subject: str
    :param html_body: String formatted as HTML to be used as
        the main content
    :type html_body: str
    :param uid: bool to append a unique id to the subject 
        line
    :type uid: bool
    :param logo: bool to use the DaVinci logo image
    :type logo: bool

    Example usage:

    .. code-block:: python

        # MUST INCLUDE A MAIN GUARD FOR email.await_response()!!!
        if __name__ == '__main__':
            # Create email instance
            email = DavinciEmail("Weekly Volume Analysis", 
                "<h3>Please find data attached for each's month forecasted capacity.</h3>")
            
            # Add attachments
            email.attach_df_as_excel(df, 'Analysis.xlsx')
            email.embed_df_as_html_table(df, color='blue_light',
                font_size=12, text_align='center')
            # Add more text sequentially
            email.add_to_body("<h5> More Text! </h5>")
            
            # Send
            email.send(['someone@kencogroup.com'])

            # Wait for a reply
            code, resp = email.await_response()

    """
    def __init__(self, subject: str, html_body: str, uid: bool = True, logo: bool = True):
        # Random ID
        if uid:
            letters = string.ascii_letters + string.digits
            self.uid = ' UID: ' + ''.join(random.choice(letters) for i in range(12))
        else:
            self.uid = ''

        
        # Critical Email components
        self.resource = get_secret('EMAIL_USER')
        self.account = self._authenticate()

        self.mailbox = self.account.mailbox(resource=self.resource)
        self.message = self.mailbox.new_message()
        self.subject = subject + self.uid
        self.html_body = html_body
        
        # Storage for attachments
        self.body_append = []
        self.attachments = []
        self.keep_attachments = []

        self.logo = logo
        # Logo
        if self.logo:
            logo_path = os.path.dirname(__file__) + '/images/logo.png'
            logo = open(logo_path, 'rb').read()
            self.message.attachments.add(logo_path)
            att_logo = self.message.attachments[0]
            att_logo.is_inline = True
            att_logo.content_id = 'logo.png'
            self.logo_html = """
                <html>
                    <body>
                        <p>
                            <img src="cid:logo.png">
                        </p>
                    </body>
                </html>
            """

    def _authenticate(self):
        account = _get_authenticated_account()
        return account

    def add_to_body(self, html):
        """
        Add more text to the body of the email.

        :param html: HTML contents to add.
        :type html: str
        """
        self.body_append.append(html)

    def add_attachment(self, filename):
        """
        Add an attachment from local machine.

        :param filename: the file to add.
        :type filename: str
        """
        self.keep_attachments.append(filename)

    def attach_df_as_excel(self, df: pd.DataFrame, filename: str):
        """
        Attach a DataFrame as an Excel file.

        :param df: DataFrame
        :type df: pd.DataFrame
        :param filename: Name of the excel file
        :type filename: str
        """
        if pathlib.Path(filename).suffix != '.xlsx':
            logger.error('Failed to attach dataframe as excel file. Ensure file extension is .xlsx')
            return
        df.to_excel(filename, index=False)
        self.attachments.append(filename)

    def embed_df_as_html_table(self, df: pd.DataFrame, **kwargs: dict):
        """
        Embed a DataFrame directly into the email body.

        :param df: DataFrame
        :type df: pd.DataFrame
        :param kwargs: Formatting key-word args to
            pass to pretty_html_table.build_table
        :type kwargs: dict
        """
        table = f"{build_table(df, **kwargs)}"
        self.body_append.append(table)

    def send(self, to):
        """
        Send the email to recipients.

        :param to: List of recipient email addresses.
        :type to: List[str]
        """
        for recipient in to:
            self.message.to.add(recipient)
        for body in self.body_append:
            self.html_body += body
        for attachment in self.attachments + self.keep_attachments:
            self.message.attachments.add(attachment)
        if self.logo:
            self.html_body += self.logo_html
        self.message.subject = self.subject
        self.message.body = self.html_body
        self.message.send()
        for attachment in self.attachments:
            # Removing temp files created
            os.unlink(attachment)

    def await_response(self, timeout_after=1800):
        """
        Wait for the response email and return the first line of it as string.
        This should be used with a uid, and is ideal for confirm/deny setups.

        .. warning::
            This method needs to be in a 'main' guard. See the example code above.
            If it is not within a main guard, then a duplicate email will be sent.


        :param timeout_after: how long to wait in seconds.
        :type timeout_after: int
        """
        re_subject = "RE: " + self.subject
        queue = multiprocessing.Queue()
        p = multiprocessing.Process(
            target=_periodic_matching_email_check,
            name="periodic_matching_email_check",
            args=(re_subject, queue)
        )
        p.start()
        p.join(timeout_after)

        # Clean up
        if p.is_alive():
            p.terminate()
            p.join()

        if queue.empty():
            return 404, None
        else:
            return 200, queue.get()

@log()
def collect_matching_attachments(subject: str, save_path: str='./',
    mailbox: MailBox=None, save_all: bool=False, excel_engine="openpyxl",
    equals_subject=True, mark_as_read=False, delete_after=False, **kwargs) -> pd.DataFrame:
    """

    This function will collect attachment excel files from emails
    matching on the subject line.

    :param subject: Title of the email to match against
    :type subject: string

    :param save_path: optional path to specify where to write
        collected data to. Defaults to current directory.
    :type save_path: string

    :param mailbox: O365 mailbox object
    :type mailbox: O365.mailbox.MailBox

    :param save_all: Controls whether to save all attachments locally,
        otherwise they will be deleted after reading to an
        in-memory dataframe
    :type save_all: bool

    :param excel_engine: Excel processing engine;
        'openpyxl' for .xlsx, 'xlrd' for the older .xls format
    :type excel_engine: string

    :param equals_subject: 'True' for exact subject match, 'False' when the subject
        parameter can match a substring of the email's subject
    :type equals_subject: boolean

    :param mark_as_read: the selected message will be marked with a "read" status
    :type mark_as_read: boolean

    :param delete_after: the selected message will be permanently
        deleted after the attachment has been read into the df
    :type delete_after: boolean

    :param kwargs: keywords to pass to pd.read_excel
    :type kwargs: dict

    :return: DataFrame
    :rtype:  pd.DataFrame
    
    Example usage:

    .. code-block:: python

        subject = 'some email subject'
        df = collect_matching_attachments(subject)
        
        # or save all attachments to local working dir
        collect_matching_attachments(subject, save_all=True)
    """

    if not mailbox:
        account = _get_authenticated_account()
        mailbox = account.mailbox(resource=get_secret('EMAIL_USER'))

    today = datetime.today()

    if (equals_subject):
        query = mailbox.new_query().on_attribute('subject').equals(subject)
    else:
        query = mailbox.new_query().on_attribute('subject').contains(subject)

    query.chain('and').on_attribute('receivedDateTime').greater(today - timedelta(days=30))
    messages = mailbox.get_messages(limit=1000, query=query)
    inbox = [i for i in messages]
    inbox = sorted(inbox, key=lambda x: x.received, reverse=True)
    first_match = inbox[0]
    msg_attachments = first_match.attachments
    msg_attachments.download_attachments()
    msg_attachments = first_match.attachments
    df = pd.DataFrame({})
    for attachment in msg_attachments:
        filename = attachment.name
        if pathlib.Path(filename).suffix not in ('.xls','.xlsx'):
            continue
        attachment.save(save_path)
        if not df.shape[0]:
            df = pd.read_excel(save_path + attachment.name, engine=excel_engine, **kwargs)
        if not save_all:
            os.unlink(save_path + attachment.name)
            break

    if (mark_as_read): first_match.mark_as_read()
    if (delete_after): first_match.delete()

    return df

@log()
def find_matching_email_contents(subject: str, mailbox: MailBox=None, created_after = datetime(1970,1,1), mark_as_read = False) -> Tuple:
    """

    This function finds emails in the inbox that match
    the input subject and return the written content.
    This function assumes that only one email will match
    the given subject line, so it is best used in conjunction
    with a unique identifier if possible, as well as the created_after
    parameter. Otherwise, it is not guaranteed that the match
    will be the most recent email.

    :param subject: Title of the emails to find
    :type subject: str

    :param mailbox: mailbox object from O365 package, optional and to avoid secondary logins
    :type mailbox: O365.mailbox.MailBox

    :param created_after: select only those messages created after this datetime
    :type created_after: datetime

    :param mark_as_read: the selected message will be marked with a "read" status
    :type mark_as_read: boolean

    :return: Tuple of (Response Code, Response Data)
    :rtype:  tuple
    
    Example usage:

    .. code-block:: python

        subject = 'some email subject'
        code, resp = find_matching_email_contents(subject)
    """

    if not mailbox:
        account = _get_authenticated_account()
        mailbox = account.mailbox(resource=get_secret('EMAIL_USER'))
    try:

        query = mailbox.new_query().on_attribute('subject').contains(subject).chain('and').on_attribute('created_date_time').greater(created_after)
        messages = mailbox.get_messages(query=query)
        first_match = next(messages)

        if (mark_as_read):
            first_match.mark_as_read()

        return 200, first_match.body
    except:
        return 404, None
