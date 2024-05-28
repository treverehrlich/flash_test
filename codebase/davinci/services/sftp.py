from io import BytesIO

def push_file_to_sftp(server, port, username, password, fl, destination_path):

    """
    Transmit a file to secure FTP server (SFTP)

    :param server: server url
    :type server: string

    :param port: server's sftp port - often but no always 22
    :type port: int

    :param username: sftp username
    :type username: string

    :param password: sftp password
    :type server: string

    :param fl: file or "file-like" object (such as StringIO buffer)
    :type fl: file or file-like object

    :param destination_path: be sure to include the destination filename; i.e. "/" + filename
    :type destination_path: string

    :rtype: paramiko.sftp_attr.SFTPAttributes # can use the str() function on the returned object to get a unix-like result

    """
    import paramiko

    with paramiko.SSHClient() as ssh_client:

        # since authentication is done by login/password and not keys, allow_agent is disabled

        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=server,
            port=port,
            username=username,
            password=password,
            allow_agent=False
        )

        # create an SFTP client object
        with ssh_client.open_sftp() as ftp:

            # upload file-object to the remote server
            sftp_attribs = ftp.putfo(fl,destination_path,confirm=True)

    return sftp_attribs


def pull_file_from_sftp(server, port, username, password, target_path, destination_path):

    """
    Get a file from sFTP

    :param server: server url
    :type server: string

    :param port: server's sftp port - often but no always 22
    :type port: int

    :param username: sftp username
    :type username: string

    :param password: sftp password
    :type server: string

    :param target_path: the target file path on SFTP
    :type server: string

    :param destination_path: where the file will get written to.
        Be sure to include the destination filename; i.e. "/" + filename
    :type destination_path: string

    :rtype: paramiko.sftp_attr.SFTPAttributes # can use the str() function on the returned object to get a unix-like result

    """
    import paramiko

    with paramiko.SSHClient() as ssh_client:

        # since authentication is done by login/password and not keys, allow_agent is disabled

        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=server,
            port=port,
            username=username,
            password=password,
            allow_agent=False
        )

        # create an SFTP client object
        with ssh_client.open_sftp() as ftp:
            with open(destination_path, 'wb') as f:
            # get file-object from the remote server
                sftp_file = ftp.getfo(target_path, f)

    return sftp_file

def sftp_ls(server, port, username, password, dir="."):
    """
    Mimic a UNIX-like 'ls' command inside the sFTP server. Assumes dir is the directory to ls on.

    :param server: server url
    :type server: string

    :param port: server's sftp port - often but no always 22
    :type port: int

    :param username: sftp username
    :type username: string

    :param password: sftp password
    :type server: string

    :param dir: directory to 'ls' on
    :type dir: string

    :rtype: paramiko.sftp_attr.SFTPAttributes # can use the str() function on the returned object to get a unix-like result

    """
    import paramiko

    with paramiko.SSHClient() as ssh_client:

        # since authentication is done by login/password and not keys, allow_agent is disabled
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=server,
            port=port,
            username=username,
            password=password,
            allow_agent=False
        )

        # create an SFTP client object
        with ssh_client.open_sftp() as ftp:
            # 'ls' like query on the specified directory
            ls_res = ftp.listdir(dir)

    return ls_res