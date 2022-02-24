import base64
import datetime
import hashlib
import hmac
import json
import logging

import requests

from tgedr.connectors.common import ConnectorException

logger = logging.getLogger(__name__)


class AzureTableStorage:

    __URI = "https://<ACCOUNT>.table.core.windows.net/<TABLE>"
    __API_VERSION = "2016-05-31"

    def __init__(self, storage_account: str, storage_account_key: str, table: str):
        logger.info(f"[AzureTableStorage.__init__|in] ({storage_account}, storage_account_key, {table})")
        self.__account = storage_account
        self.__key = storage_account_key
        self.__table = table
        logger.info("[AzureTableStorage.__init__|out]")

    def __compute_signature(self, date: str, filter: str = None):
        logger.info(f"[AzureTableStorage.__compute_signature|in] ({date})")
        canonicalized_resources = f"/{self.__account}/{self.__table}"
        if filter:
            canonicalized_resources += filter
        string_to_sign = date + "\n" + canonicalized_resources
        result = base64.b64encode(
            hmac.new(
                key=base64.b64decode(self.__key), msg=string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
            ).digest()
        ).decode()
        logger.info(f"[AzureTableStorage.__compute_signature|out] => {result}")
        return result

    def insert(self, entity: dict):
        logger.info(f"[AzureTableStorage.insert|in] ({entity})")

        uri = AzureTableStorage.__URI.replace("<ACCOUNT>", self.__account).replace("<TABLE>", self.__table)
        date = datetime.datetime.utcnow().strftime("%a, %-d %b %Y %H:%M:%S GMT")
        content = json.dumps(entity)
        signature = self.__compute_signature(date)

        headers = {
            "x-ms-date": date,
            "x-ms-version": AzureTableStorage.__API_VERSION,
            "Authorization": f"SharedKeyLite {self.__account}:{signature}",
            "Accept": "application/json;odata=nometadata",
            "Content-Type": "application/json",
        }

        logger.info(f"[AzureTableStorage.insert] headers: {headers}")

        response = requests.post(url=uri, headers=headers, data=content)
        if not response.ok:
            logger.error(f"[AzureTableStorage.insert] got status code => {response.status_code}")
            raise ConnectorException(f"response was not ok: {response.content}")
        result = response.json()
        logger.info(f"[AzureTableStorage.insert|out] => response: {result}")
        return result

    def get(self, partition_key: str, row_key: str):
        logger.info(f"[AzureTableStorage.get|in] ({partition_key}, {row_key})")

        filter_expression = ""
        if partition_key:
            filter_expression += f"PartitionKey='{partition_key}'"
        if row_key:
            filter_expression += "," if 0 < (len(filter_expression)) else ""
            filter_expression += f"RowKey='{row_key}'"
        filter_expression = f"({filter_expression})"

        uri = (
            AzureTableStorage.__URI.replace("<ACCOUNT>", self.__account).replace("<TABLE>", self.__table)
            + filter_expression
        )
        date = datetime.datetime.utcnow().strftime("%a, %-d %b %Y %H:%M:%S GMT")
        signature = self.__compute_signature(date, filter_expression)

        headers = {
            "x-ms-date": date,
            "x-ms-version": AzureTableStorage.__API_VERSION,
            "Authorization": f"SharedKeyLite {self.__account}:{signature}",
            "Accept": "application/json;odata=nometadata",
            "Content-Type": "application/json",
        }
        response = requests.get(url=uri, headers=headers)
        if not response.ok:
            logger.error(f"[AzureTableStorage.get] got status code => {response.status_code}")
            raise ConnectorException(f"response was not ok: {response.content}")
        result = response.json()

        logger.info(f"[AzureTableStorage.get|out] => response: {result}")
        return result
