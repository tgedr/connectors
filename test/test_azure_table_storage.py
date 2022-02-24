import os
import random

import pytest

from tgedr.connectors.azure_table_storage import AzureTableStorage

STORAGE_ACCOUNT = "st0data0dev0weu0001"
TABLE = "stt0data0dev0weu0001"


@pytest.fixture
def entity():
    return {
        "address": "Mountain View",
        "country": "US",
        "age": 23,
        "name": "Edm.Guid",
        "id": "256",
        "PartitionKey": random.choice(["US", "IT", "DE"]),
        "RowKey": str(random.randint(1, 999999)),
    }

@pytest.mark.skip
def test_insert(entity):

    o = AzureTableStorage(
        storage_account=STORAGE_ACCOUNT, storage_account_key=os.environ["AZURE_STORAGE_ACCESS_KEY"], table=TABLE
    )
    response = o.insert(entity=entity)

    assert response["RowKey"] == entity["RowKey"]
    assert response["PartitionKey"] == entity["PartitionKey"]

@pytest.mark.skip
def test_get(entity):

    o = AzureTableStorage(
        storage_account=STORAGE_ACCOUNT, storage_account_key=os.environ["AZURE_STORAGE_ACCESS_KEY"], table=TABLE
    )
    o.insert(entity=entity)
    response = o.get(partition_key=entity["PartitionKey"], row_key=entity["RowKey"])

    assert response["RowKey"] == entity["RowKey"]
    assert response["PartitionKey"] == entity["PartitionKey"]
