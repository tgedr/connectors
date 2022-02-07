import logging
import os

import paramiko

logger = logging.getLogger(__name__)


class FTP:
    def __init__(self, host, username, password):
        """Simple object for uploading to a FTP server.


        Args:
            host (str): [description]
            username (str): [description]
            password (str): [description]
        """
        self._host = host
        self._username = username
        self._password = password

    def upload_file_to_ftp(self, path_local: str, path_remote: str):

        logger.info(f"Uploading to FTP - {path_local} to {path_remote}.")

        logger.info(f"Path exists: {os.path.isfile(path_local)}")
        if not os.path.isfile(path_local) and path_local.startswith("/mnt"):
            logger.info(
                f"path_local <{path_local}> does not exist but starts with '/mnt', which is only seen by spark but not local file IO. Prefixing with '/dbfs'"
            )
            path_local = "/dbfs" + path_local
            logger.info(f"path_local changed to <{path_local}>")

        logger.info(f"Path exists: {os.path.isfile(path_local)}")

        with paramiko.client.SSHClient() as ssh_client:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self._host, username=self._username, password=self._password)
            sftp_client = ssh_client.open_sftp()
            sftp_client.put(path_local, path_remote)
