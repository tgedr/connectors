import contextlib
import datetime
import json
import logging
import time
from typing import List

import jwt
import requests

from tgedr.connectors.common import ConnectorException

logger = logging.getLogger(__name__)


class Monetate:

    __TOKEN_TIME_LIMIT_IN_SECS = 30
    __TOKEN_URL = "https://api.monetate.net/api/auth/v0/"
    __DATA_URL = "https://api.monetate.net/api/data/v1/pandorademo/production/"
    __JWT_ALGO = "RS256"

    def __init__(self, username: str, private_key: str):
        logger.info("[Monetate.__init__] in")
        if (not username) or (0 == len(username)):
            raise ValueError("no username provided")

        if (not private_key) or (0 == len(private_key)):
            raise ValueError("no private_key provided")

        self.__username = username
        self.__private_key = private_key
        self.__token = None

        logger.info("[Monetate.__init__] out")

    def __is_token_valid(self) -> bool:
        logger.info("[Monetate.__is_token_valid] in")
        result = False
        if self.__token:
            expires_in = datetime.datetime.fromtimestamp(self.__token["expires_at"])
            still_valid_for = (expires_in - datetime.datetime.now()).seconds
            result = Monetate.__TOKEN_TIME_LIMIT_IN_SECS < still_valid_for
        logger.info(f"[Monetate.__is_token_valid] out => {result}")
        return result

    def __token_request_header(self):
        logger.info("[Monetate.__token_request_header] in")
        payload = jwt.encode(
            {"username": self.__username, "iat": time.time()}, self.__private_key, algorithm=Monetate.__JWT_ALGO
        )
        authorization = "JWT {}".format(payload)
        result = {"Authorization": authorization}
        logger.info(f"[Monetate.__token_request_header] out")
        return result

    def __default_request_header(self):
        logger.info("[Monetate.__default_request_header] in")
        result = {"Authorization": f"Token {self.__token['token']}"}
        logger.info("[Monetate.__default_request_header] out")
        return result

    @contextlib.contextmanager
    def __valid_token(self):
        logger.info("[Monetate.__valid_token] in")
        try:
            if not self.__is_token_valid():
                response = requests.get(f"{Monetate.__TOKEN_URL}refresh/", headers=self.__token_request_header())
                if not response.ok:
                    logger.error(f"[Monetate.__valid_token] got status code => {response.status_code}")
                    raise ConnectorException("response was not ok")

                self.__token = response.json()["data"]
                logger.info(f"[Monetate.__valid_token] got token valid till => {self.__token['expires_at']}")
            yield self.__token
        except Exception as x:
            raise ConnectorException() from x
        logger.info("[Monetate.__valid_token] out")

    def get_schemas(self) -> dict:
        logger.info("[Monetate.get_schemas] in")
        result = None
        with self.__valid_token():
            try:
                query_string = {'row_count': True}
                response = requests.get(f"{Monetate.__DATA_URL}schema/", params=query_string,
                                        headers=self.__default_request_header())
                if not response.ok:
                    logger.error(f"[Monetate.get_schemas] got status code => {response.status_code}")
                    raise ConnectorException("response was not ok")
                r_json = response.json()
                result = {"count": r_json["meta"]["count"], "schemas": r_json["data"]}
            except Exception as x:
                logger.error(f"[Monetate.get_schemas] got exception => {x}")
                raise ConnectorException("could not get schemas") from x
        logger.info(f"[Monetate.get_schemas] out => {result}")
        return result

    def get_record(self, schema: str, record_id: str) -> List:
        logger.info(f"[Monetate.get_record] in ({schema},{record_id})")
        result = None
        with self.__valid_token():
            try:
                query_string = {'id': record_id}
                url = f"{Monetate.__DATA_URL}data/{schema}/"
                response = requests.get(url, params=query_string,
                                        headers=self.__default_request_header())
                if not response.ok:
                    logger.error(f"[Monetate.get_record] got status code => {response.status_code}")
                    raise ConnectorException("response was not ok")
                result = {"rows": response.json()["data"]}
            except Exception as x:
                logger.error(f"[Monetate.get_record] got exception => {x}")
                raise ConnectorException("could not get record") from x
        logger.info(f"[Monetate.get_record] out => {result}")
        return result

    def post_record(self, schema: str, record: dict, content_type: str = "application/json") -> List:
        logger.info(f"[Monetate.post_record] in ({schema},{record})")
        result = None
        with self.__valid_token():
            try:
                payload = json.dumps({"schema_rows": [record]})
                url = f"{Monetate.__DATA_URL}data/{schema}/"

                headers = self.__default_request_header()
                headers["Content-Type"] = content_type

                response = requests.post(url, data=payload, headers=headers)
                if not response.ok:
                    logger.error(f"[Monetate.post_record] got status code => {response.status_code}")
                    raise ConnectorException("response was not ok")
                r_json = response.json()["data"]
                result = {"rows": r_json["schema_rows"]}
            except Exception as x:
                logger.error(f"[Monetate.post_record] got exception => {x}")
                raise ConnectorException("could not post record") from x
        logger.info(f"[Monetate.post_record] out => {result}")
        return result


