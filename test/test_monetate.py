import os

import pytest

from tgedr.connectors.common import ConnectorException
from tgedr.connectors.monetate_api import MonetateApi


def test_get_token_wrong_key():
    with pytest.raises(ConnectorException):
        user = os.environ.get("MONETATE_USER")
        dummy_key = "-----BEGIN RSA PRIVATE KEY----- xpto -----END RSA PRIVATE KEY-----"
        o = MonetateApi(username=user, private_key=dummy_key)
        with o._MonetateApi__valid_token():
            pass


def test_get_valid_token():
    key = os.environ["MONETATE_KEY"]
    user = os.environ.get("MONETATE_USER")
    o = MonetateApi(username=user, private_key=key)
    with o._MonetateApi__valid_token():
        pass
    assert o._MonetateApi__is_token_valid()


def test_get_schemas():
    key = os.environ["MONETATE_KEY"]
    user = os.environ.get("MONETATE_USER")
    o = MonetateApi(username=user, private_key=key)
    schemas = o.get_schemas()
    assert 0 <= schemas["count"]


def test_get_record():
    """
    this test is based on current 'next buy reco semi known users' dataset
    in  site 'dev.uk.pandora.net'
    """

    key = os.environ["MONETATE_KEY"]
    user = os.environ.get("MONETATE_USER")
    o = MonetateApi(username=user, private_key=key)
    response = o.get_record(
        schema="next buy reco semi known users", record_id="MCMID|00002812577406158014239605433479468079"
    )
    assert 0 <= len(response["rows"])


def test_post_record():

    key = os.environ["MONETATE_KEY"]
    user = os.environ.get("MONETATE_USER")
    o = MonetateApi(username=user, private_key=key)

    response = o.get_record(
        schema="next buy reco semi known users", record_id="MCMID|00002812577406158014239605433479468079"
    )
    record = response["rows"][0]
    print(f"old record: {record}")
    record["probability"] = record["probability"] + 0.1
    print(f"new record: {record}")
    response = o.post_record(schema="next buy reco semi known users", record=record)
    assert response["rows"][0] == record
