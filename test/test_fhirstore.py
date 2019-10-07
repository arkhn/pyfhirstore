from fhirstore import FHIRStore

# For now, this class assumes an already existing store exists (store.bootstrap was run)
#
class TestFHIRStore:
    def test_create(self, fresh_store, mongo_client):
        # Depending on how the API evolves, should check wether an exception was raised / check if a document was properly inserted
        assert 1 == 2
