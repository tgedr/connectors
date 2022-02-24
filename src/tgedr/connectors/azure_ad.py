import logging

import requests

from tgedr.connectors.common import ConnectorException

logger = logging.getLogger(__name__)


class AzureAD:

    __TOKEN_URI = "https://login.microsoftonline.com/<TENANT_ID>/oauth2/token"

    def get_token(self, tenant: str, client_id: str, client_secret: str, resource: str):
        logger.info(f"[AzureAD.get_token] in ({tenant}, {client_id}, client_secret, {resource})")

        uri = AzureAD.__TOKEN_URI.replace("<TENANT_ID>", tenant)
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "resource_key": resource,
        }
        response = requests.get(url=uri, headers={"Content-Type": "application/x-www-form-urlencoded"}, data=payload)
        if not response.ok:
            logger.error(f"[AzureAD.get_token] got status code => {response.status_code}")
            raise ConnectorException("response was not ok")
        result = response.json()
        logger.info(f"[AzureAD.get_token] out => response size: {len(result)}")
        return result
