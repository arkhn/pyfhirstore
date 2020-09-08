import json
import pytest
from unittest.mock import patch

from bson.objectid import ObjectId
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from jsonschema.exceptions import ValidationError

from fhir.resources.operationoutcome import OperationOutcome
from fhir.resources.patient import Patient

from fhirstore import FHIRStore, BadRequestError, NotFoundError, ARKHN_CODE_SYSTEMS


@pytest.fixture(autouse=True)
def reset_store(store):
    store.reset()
    store.bootstrap(resource="Patient", show_progress=False)
    store.bootstrap(resource="MedicationRequest", show_progress=False)


# For now, this class assumes an already existing store exists
# (store.bootstrap was run)


class TestFHIRStore:
    "FHIRStore"

    ###
    # FHIRStore.create()
    ###
    def test_create_missing_resource_type(self, store: FHIRStore):
        """create() raises if resource type is not specified"""

        with pytest.raises(BadRequestError, match="resourceType is missing in resource"):
            store.create({})

    def test_create_bad_resource_type(self, store: FHIRStore):
        """create() raises if resource type is unknown"""

        with pytest.raises(NotFoundError, match='unsupported FHIR resource: "unknown"'):
            store.create({"resourceType": "unknown"})

    def test_create_invalid_resource(self, store: FHIRStore):
        """create() raises if json schema validation failed in mongo"""

        res = store.create({"resourceType": "Patient", "id": "ok", "nope": True})
        assert isinstance(res, OperationOutcome)
        assert len(res.issue) == 1
        assert res.issue[0].diagnostics == "extra fields not permitted: Patient.nope"

        res = store.create({"resourceType": "Patient", "id": "ok", "nope": True, "wow": "such"})
        assert isinstance(res, OperationOutcome)
        assert len(res.issue) == 2
        assert res.issue[0].diagnostics == "extra fields not permitted: Patient.nope"
        assert res.issue[1].diagnostics == "extra fields not permitted: Patient.wow"

        res = store.create({"resourceType": "Patient", "contact": [{"name": 1}]})
        assert isinstance(res, OperationOutcome)
        assert len(res.issue) == 1
        assert res.issue[0].diagnostics == "Validation error: Patient.contact.0.name"

    def test_create_resource_dict(self, store: FHIRStore, mongo_client: MongoClient, test_patient):
        """create() correctly inserts a document in the database"""
        result = store.create(test_patient)
        assert isinstance(result["_id"], ObjectId), "result _id must be an objectId"
        inserted = mongo_client["Patient"].find_one(
            {"_id": result["_id"]}, projection={"_id": False}
        )
        assert inserted == test_patient

    def test_create_resource_fhirmodel(
        self, store: FHIRStore, mongo_client: MongoClient, test_patient
    ):
        """create() correctly inserts a document in the database"""
        patient_model = Patient(**test_patient)
        result = store.create(patient_model)
        assert isinstance(result["_id"], ObjectId), "result _id must be an objectId"
        inserted = mongo_client["Patient"].find_one(
            {"_id": result["_id"]}, projection={"_id": False}
        )
        assert inserted == test_patient

    def test_create_resource_with_extension(self, store: FHIRStore, mongo_client: MongoClient):
        """resources using extensions are not
        handled yet, an error should be raised"""
        with open("test/fixtures/patient-example-with-extensions.json") as f:
            patient = json.load(f)
            result = store.create(patient)
            assert isinstance(result["_id"], ObjectId), "result _id must be an objectId"
            inserted = mongo_client["Patient"].find_one(
                {"_id": result["_id"]}, projection={"_id": False}
            )
            assert inserted == patient

    def test_unique_indices(self, store: FHIRStore, mongo_client: MongoClient):
        """create() raises if index is already present"""
        store.create(
            {
                "identifier": [
                    {"value": "val", "system": "sys"},
                    {"value": "just_value"},
                    {
                        "value": "value",
                        "system": "system",
                        "type": {"coding": [{"system": "type_system", "code": "type_code"}]},
                    },
                ],
                "resourceType": "Patient",
                "id": "pat1",
            }
        )

        # index on id
        with pytest.raises(DuplicateKeyError, match='dup key: { id: "pat1" }'):
            store.create({"resourceType": "Patient", "id": "pat1"})

        # index on (identifier.value, identifier.system)
        with pytest.raises(
            DuplicateKeyError,
            match='dup key: { identifier.system: "sys", identifier.value: "val", \
identifier.type.coding.system: null, identifier.type.coding.code: null }',
        ):
            store.create(
                {
                    "identifier": [{"value": "val", "system": "sys"}],
                    "resourceType": "Patient",
                    "id": "pat2",
                }
            )
        with pytest.raises(
            DuplicateKeyError,
            match='dup key: { identifier.system: null, identifier.value: "just_value", \
identifier.type.coding.system: null, identifier.type.coding.code: null }',
        ):
            store.create(
                {"identifier": [{"value": "just_value"}], "resourceType": "Patient", "id": "pat2"}
            )
        with pytest.raises(
            DuplicateKeyError,
            match='dup key: { identifier.system: "system", identifier.value: "value", \
identifier.type.coding.system: "type_system", identifier.type.coding.code: "type_code" }',
        ):
            store.create(
                {
                    "identifier": [
                        {"value": "new_val"},
                        {
                            "value": "value",
                            "system": "system",
                            "type": {"coding": [{"system": "type_system", "code": "type_code"}]},
                        },
                    ],
                    "resourceType": "Patient",
                    "id": "pat2",
                }
            )

    ###
    # FHIRStore.read()
    ###

    def test_read_bad_resource_type(self, store: FHIRStore):
        """read() raises if resource type is unknown"""

        with pytest.raises(NotFoundError, match='unsupported FHIR resource: "unknown"'):
            store.read("unknown", "864321")

    def test_read_resource_not_found(self, store: FHIRStore, test_patient):
        """read() returns None when no matching document was found"""

        with pytest.raises(NotFoundError):
            store.read("Patient", test_patient["id"])

    def test_read_resource(self, store: FHIRStore, test_patient):
        """read() finds a document in the database"""
        store.create(test_patient)
        result = store.read("Patient", test_patient["id"])
        assert result == test_patient

    ###
    # FHIRStore.update()
    ###
    def test_update_bad_resource_type(self, store: FHIRStore):
        """update() raises if resource type is unknown"""

        with pytest.raises(NotFoundError, match='unsupported FHIR resource: "unknown"'):
            store.update("864321", {"gender": "other", "resourceType": "unknown"})

    def test_update_resource_not_found(self, store: FHIRStore, test_patient):
        """update() returns None when no matching document was found"""

        res = store.update(
            test_patient["id"], {"name": [{"family": "Patator"}], "resourceType": "Patient"}
        )
        assert isinstance(res, OperationOutcome)
        assert len(res.issue) == 1
        assert res.issue[0].diagnostics == "Patient with id pat1 not found"

    def test_update_bad_resource_schema(self, store: FHIRStore, test_patient):
        """update() raises if json schema validation failed in mongo"""

        store.create(test_patient)
        res = store.update(test_patient["id"], {**test_patient, "gender": ["elephant"]})
        assert isinstance(res, OperationOutcome)
        assert len(res.issue) == 1
        assert res.issue[0].diagnostics == "str type expected: Patient.gender"

    def test_update_resource(self, store: FHIRStore, test_patient):
        """update() finds a document in the database"""
        store.create(test_patient)
        store.update(test_patient["id"], {**test_patient, "gender": "other"})
        assert store.read("Patient", test_patient["id"]) == {**test_patient, "gender": "other"}

    ###
    # FHIRStore.patch()
    ###
    def test_patch_bad_resource_type(self, store: FHIRStore):
        """patch() raises if resource type is unknown"""

        with pytest.raises(NotFoundError, match='unsupported FHIR resource: "unknown"'):
            store.patch("unknown", "864321", {"gender": "other"})

    def test_patch_resource_not_found(self, store: FHIRStore, test_patient):
        """patch() returns None when no matching document was found"""

        with pytest.raises(NotFoundError):
            store.patch("Patient", test_patient["id"], {"name": "Patator"})

    # TODO
    @pytest.mark.skip()
    def test_patch_bad_resource_schema(self, store: FHIRStore, test_patient):
        """patch() raises if json schema validation failed in mongo"""

        store.create(test_patient)
        with pytest.raises(ValidationError):
            store.patch("Patient", test_patient["id"], {"gender": "elephant"})

    def test_patch_resource(self, store: FHIRStore, test_patient):
        """patch() finds a document in the database"""
        store.create(test_patient)
        store.patch("Patient", test_patient["id"], {"gender": "other"})
        assert store.read("Patient", test_patient["id"]) == {**test_patient, "gender": "other"}

    ###
    # FHIRStore.delete()
    ###
    def test_delete_bad_resource_type(self, store: FHIRStore):
        """delete() raises if resource type is unknown"""

        with pytest.raises(NotFoundError, match='unsupported FHIR resource: "unknown"'):
            store.delete("unknown", "864321")

    def test_delete_resource_not_found(self, store: FHIRStore, test_patient):
        """delete() returns None when no matching document was found"""

        with pytest.raises(NotFoundError):
            store.delete("Patient", test_patient["id"])

    def test_delete_missing_param(self, store: FHIRStore, test_patient):
        """delete() returns None when no matching document was found"""

        with pytest.raises(
            BadRequestError,
            match="one of: 'instance_id', 'resource_id' or 'source_id' are required",
        ):
            store.delete("Patient")

    def test_delete_instance(self, store: FHIRStore, test_patient):
        """delete() finds a document in the database"""
        store.create(test_patient)
        result = store.delete("Patient", test_patient["id"])
        assert result == 1

    def test_delete_by_resource_id(self, store: FHIRStore, test_patient):
        """delete() finds a document in the database"""
        store.create(test_patient)

        resource_id = "pyrogResourceId"
        metadata = {
            "tag": [
                {"system": ARKHN_CODE_SYSTEMS.resource, "code": resource_id},
                {"code": "some-other-tag"},
            ]
        }
        store.create({"resourceType": "Patient", "id": "pat2", "meta": metadata})
        store.create({"resourceType": "Patient", "id": "pat3", "meta": metadata})

        result = store.delete("Patient", resource_id=resource_id)
        assert result == 2

    def test_delete_by_source_id(self, store: FHIRStore, test_patient):
        """delete() finds a document in the database"""
        store.create(test_patient)

        source_id = "pyrogSourceId"
        metadata = {
            "tag": [
                {"system": ARKHN_CODE_SYSTEMS.source, "code": source_id},
                {"code": "some-other-tag"},
            ]
        }
        store.create({"resourceType": "Patient", "id": "pat2", "meta": metadata})
        store.create({"resourceType": "Patient", "id": "pat3", "meta": metadata})

        result = store.delete("Patient", source_id=source_id)
        assert result == 2

    ###
    # FHIRStore.upload_bundle()
    ###
    @patch("fhirstore.FHIRStore.create")
    def test_upload_bundle(
        self, mock_create, store: FHIRStore, mongo_client: MongoClient, test_bundle
    ):
        """create() correctly inserts a document in the database"""
        store.upload_bundle(test_bundle)
        assert mock_create.call_count == 3
