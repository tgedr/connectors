import os
import pytest
from tgedr.connectors.azure_ad import AzureAD


@pytest.mark.skip
def test_get_token():

    o = AzureAD()
    response = o.get_token(
        tenant=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
        resource="https://storage.azure.com/",
    )
    assert "access_token" in response.keys()
    assert 0 < len(response["access_token"])
