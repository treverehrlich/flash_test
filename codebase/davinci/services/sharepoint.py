import io
import os
import pandas as pd
from office365.sharepoint.files.file import File
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

from davinci.utils.logging import log
from davinci.services.auth import get_secret


def _get_sharepoint_auth_context(server_url: str, site_url: str):
    """
    Get Sharepoint context (with authentication step already done).

    :param server_url: Sharepoint server info.
    :type server_url: str

    :param site_url: Sharepoint site info.
    :type site_url: str

    :return: Sharepoint context
    :rtype: ClientContext
    """
    client_id = get_secret('SHAREPOINT_CLIENT_ID')
    client_secret = get_secret('SHAREPOINT_CLIENT_SECRET')
    url = server_url + site_url
    credentials = ClientCredential(client_id, client_secret)
    ctx = ClientContext(url).with_credentials(credentials)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    return ctx

def get_binary_file(server_url: str, site_url: str, file_path: str):
    """
    Get a binary file from Sharepoint site.

    :param server_url: Sharepoint server info.
    :type server_url: str

    :param site_url: Sharepoint site info.
    :type site_url: str

    :param file_path: The file path.
    :type file_path: str

    :param sheet_name: Name of the Excel sheet to read.
    :type sheet_name: str
    """

    ctx = _get_sharepoint_auth_context(server_url, site_url)
    return File.open_binary(ctx, file_path)


@log()
def read_excel_sheet_as_df(server_url: str, site_url: str, file_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Read a sheet of an Excel file on Sharepoint site into 
    pandas DataFrame.

    :param server_url: Sharepoint server info.
    :type server_url: str

    :param site_url: Sharepoint site info.
    :type site_url: str

    :param file_path: The file path.
    :type file_path: str

    :param sheet_name: Name of the Excel sheet to read.
    :type sheet_name: str

    :return: Dataframe with data from sharepoint file
    :rtype: pd.DataFrame
    """
    response = get_binary_file(server_url, site_url, file_path)
    bytes_file_obj = io.BytesIO()
    bytes_file_obj.write(response.content)
    bytes_file_obj.seek(0)
    df = pd.read_excel(bytes_file_obj, sheet_name=sheet_name, engine='openpyxl')
    return df


@log()
def get_file(server_url: str, site_url: str, file_path: str, output_path: str, save_file_name: str) -> None:
    """
    Read a sheet of an Excel file on Sharepoint site into local file. 

    :param server_url: Sharepoint server info.
    :type server_url: str

    :param site_url: Sharepoint site info.
    :type site_url: str

    :param file_path: The file path.
    :type file_path: str

    :param output_path: Location to write to. 
    :type output_path: str

    :param save_file_name: name of the file to write to locally. 
    :type save_file_name: str
    """
    response = get_binary_file(server_url, site_url, file_path)
    with open(output_path + "/" + save_file_name, "wb") as local_file:
        local_file.write(response.content)

@log()
def upload_to_sharepoint(server_url: str, site_url: str, filename: str, 
        local_folder: str, output_folder: str, delete_local: bool=False) -> None:
    """
    Upload a file to Sharepoint site. THIS FUNCTION WILL NOT WORK UNTIL WE GET NEW API PERMISSIONS THROUGH
    NETWORK TEAM. DO NOT USE IN PROD UNTIL THEN.

    :param server_url: Sharepoint server info.
    :type server_url: str

    :param site_url: Sharepoint site info.
    :type site_url: str

    :param filename: The file name.
    :type filename: str

    :param local_folder: The folder location on local system.
    :type local_folder: str

    :param output_folder: Location to write to. 
    :type output_folder: str

    :param delete_local: whether or not to delete the file locally after upload.
    :type delete_local: bool
    """

    ctx = _get_sharepoint_auth_context(server_url, site_url)

    current_folder = local_folder
    target_folder = ctx.web.get_folder_by_server_relative_url(output_folder)

    with open(current_folder + "/" + filename, 'rb') as content_file:
        file_content = content_file.read()

    target_folder.upload_file(filename, file_content).execute_query()
    if delete_local:
        os.remove(current_folder + "/" + filename)