import json
import pytest
import time

from fhir.resources.bundle import Bundle

from .conftest import DB_NAME


# These tests assumes an already existing store exists
# (store.bootstrap was run)

RESOURCES_TOTAL_COUNT = 702
STRUCTUREDEF_TOTAL_COUNT = 649


@pytest.fixture(scope="module")
def index_bundles(store):
    store.bootstrap(resource="StructureDefinition")
    store.bootstrap(resource="OperationDefinition")
    store.bootstrap(resource="CapabilityStatement")
    store.bootstrap(resource="CompartmentDefinition")
    for path in [
        "bundle-extensions.json",
        "bundle-profiles.json",
        "bundle-resources.json",
        "bundle-types.json",
    ]:
        # read and index the resource is ES
        with open(f"test/fixtures/{path}") as f:
            print(f"Uploading bundle {path}...")
            bundle = json.load(f)
            store.upload_bundle(bundle)
            print(f"Done!")

    doc_count = 0
    print("waiting for a bit...")
    while doc_count < RESOURCES_TOTAL_COUNT:
        _, _, doc_count = store.es.cat.count(index=DB_NAME).split(" ")
        doc_count = int(doc_count)
        time.sleep(1)
    print(f"ok let's do this, ({doc_count} documents indexed)")
    store.es.indices.refresh(index=DB_NAME)

    yield

    # cleanup ES
    store.reset()


def test_perf(store, index_bundles):
    ts = time.time()
    result = store.search("StructureDefinition", params={"_count": 1000})
    te = time.time()
    print(f"searching {result.total} StructureDefinition took {(te - ts) * 1000}ms")
    assert isinstance(result, Bundle)
    assert result.total == STRUCTUREDEF_TOTAL_COUNT
