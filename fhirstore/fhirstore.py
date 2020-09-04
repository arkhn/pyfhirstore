import sys
import logging
from typing import Union, Dict, List, Optional
import json

import elasticsearch
import pydantic
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm

import fhirpath
from fhirpath.search import SearchContext, Search
from fhir.resources import construct_fhir_element, FHIRAbstractModel
from fhir.resources.operationoutcome import OperationOutcome

from fhirstore import ARKHN_CODE_SYSTEMS
from fhirstore.search_engine import ElasticSearchEngine, create_search_engine


class NotFoundError(Exception):
    """
    NotFoundError is returned when a resource was not found in the database.
    """

    pass


class BadRequestError(Exception):
    """
    BadRequestError is returned when the client request could not be processed.
    """

    pass


class FHIRStore:
    def __init__(
        self, mongo_client: MongoClient, es_client: elasticsearch.Elasticsearch, db_name: str,
    ):
        self.es = es_client
        self.db = mongo_client[db_name]
        self.resources = self.db.list_collection_names()

        if self.es and len(self.es.transport.hosts) > 0:
            self.search_engine: ElasticSearchEngine = create_search_engine(self.es)
        else:
            logging.warning("No elasticsearch client provided, search features are disabled")

    @property
    def initialized(self):
        return len(self.resources) > 0

    def reset(self, mongo=True, es=True):
        """
        Drops all collections currently in the database.
        """
        if mongo and not es:
            raise Exception("You also need to drop ES indices when resetting mongo")

        if mongo:
            for collection in self.resources:
                self.db.drop_collection(collection)
            self.resources = []

        if es:
            self.es.indices.delete("_all")

    def bootstrap(self, resource: Optional[str] = None, show_progress: Optional[bool] = True):
        """
        Parses the FHIR json-schema and create the collections according to it.
        """
        existing_resources = self.db.list_collection_names()

        # Bootstrap elastic indices
        self.search_engine.create_es_index(resource=resource)

        # Bootstrap mongoDB collections
        self.resources = (
            [*self.resources, resource] if resource else self.search_engine.mappings.keys()
        )
        resources = [r for r in self.resources if r not in existing_resources]
        if show_progress:
            tqdm.write("\n", end="")
            resources = tqdm(resources, file=sys.stdout, desc="Bootstrapping collections...",)
        for resource_type in resources:
            self.db.create_collection(resource_type)
            # Add unique constraint on id
            self.db[resource_type].create_index("id", unique=True)
            # Add unique constraint on (identifier.system, identifier.value)
            self.db[resource_type].create_index(
                [
                    ("identifier.system", ASCENDING),
                    ("identifier.value", ASCENDING),
                    ("identifier.type.coding.system", ASCENDING),
                    ("identifier.type.coding.code", ASCENDING),
                ],
                unique=True,
                partialFilterExpression={"identifier": {"$exists": True}},
            )

    def validate_resource_type(self, resource_type):
        if not resource_type:
            raise BadRequestError("resourceType is missing in resource")
        elif resource_type not in self.resources:
            raise NotFoundError(f'unsupported FHIR resource: "{resource_type}"')

    def normalize_resource(self, resource: Union[Dict, FHIRAbstractModel]) -> FHIRAbstractModel:
        if isinstance(resource, dict):
            resource_type = resource.get("resourceType")
            self.validate_resource_type(resource_type)
            resource = construct_fhir_element(resource_type, resource)

        return resource

    def error(self, errors: List[str], severity="error", code="invalid") -> OperationOutcome:
        issues = [{"severity": severity, "code": code, "diagnostics": err} for err in errors]
        return OperationOutcome(**{"issue": issues})

    def create(self, resource: Union[Dict, FHIRAbstractModel], bypass_document_validation=False):
        """
        Creates a resource. The structure of the resource will be checked
        against its json-schema FHIR definition.

        Args:
            - resource: dict with the resource data

        Returns: The created resource.
        """
        try:
            resource = self.normalize_resource(resource)
        except pydantic.ValidationError as e:
            return self.error(
                [
                    f"{err['msg'] or 'Validation error'}: "
                    f"{e.model.get_resource_type()}.{'.'.join([str(l) for l in err['loc']])}"
                    for err in e.errors()
                ]
            )

        res = self.db[resource.resource_type].insert_one(
            json.loads(resource.json()), bypass_document_validation=bypass_document_validation
        )
        return {**resource.dict(), "_id": res.inserted_id}

    def read(self, resource_type, instance_id):
        """
        Finds a resource given its type and id.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The found resource.
        """
        self.validate_resource_type(resource_type)

        res = self.db[resource_type].find_one({"id": instance_id}, projection={"_id": False})
        if res is None:
            raise NotFoundError
        return res

    def update(self, instance_id, resource, bypass_document_validation=False):
        """
        Update a resource given its type, id and a resource. It applies
        a "replace" operation, therefore the resource will be overriden.
        The structure of the updated resource will  be checked against
        its json-schema FHIR definition.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').
            - resource: The updated resource.

        Returns: The updated resource.
        """
        try:
            resource = self.normalize_resource(resource)
        except pydantic.ValidationError as e:
            return self.error(
                [
                    f"{err['msg'] or 'Validation error'}: "
                    f"{e.model.get_resource_type()}.{'.'.join([str(l) for l in err['loc']])}"
                    for err in e.errors()
                ]
            )
        update_result = self.db[resource.resource_type].replace_one(
            {"id": instance_id},
            json.loads(resource.json()),
            bypass_document_validation=bypass_document_validation,
        )
        if update_result.matched_count == 0:
            return self.error([f"{resource.resource_type} with id {instance_id} not found"])
        return update_result

    def patch(self, resource_type, instance_id, patch, bypass_document_validation=False):
        """
        Update a resource given its type, id and a patch. It applies
        a "patch" operation rather than a "replace", only the fields
        specified in the third argument will be updated. The structure
        of the updated resource will  be checked against its json-schema
        FHIR definition.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').
            - patch: The patch to be applied on the resource.

        Returns: The updated resource.
        """
        self.validate_resource_type(resource_type)

        update_result = self.db[resource_type].update_one(
            {"id": instance_id},
            {"$set": patch},
            bypass_document_validation=bypass_document_validation,
        )
        if update_result.matched_count == 0:
            raise NotFoundError

        return update_result

    def delete(self, resource_type, instance_id=None, resource_id=None, source_id=None):
        """
        Deletes a resource given its type and id.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The id of the deleted resource.
        """
        self.validate_resource_type(resource_type)

        if instance_id:
            res = self.db[resource_type].delete_one({"id": instance_id})
        elif resource_id:
            res = self.db[resource_type].delete_many(
                {
                    "meta.tag": {
                        "$elemMatch": {
                            "code": {"$eq": resource_id},
                            "system": {"$eq": ARKHN_CODE_SYSTEMS.resource},
                        }
                    }
                }
            )
        elif source_id:
            res = self.db[resource_type].delete_many(
                {
                    "meta.tag": {
                        "$elemMatch": {
                            "code": {"$eq": source_id},
                            "system": {"$eq": ARKHN_CODE_SYSTEMS.source},
                        }
                    }
                }
            )
        else:
            raise BadRequestError(
                "one of: 'instance_id', 'resource_id' or 'source_id' are required"
            )

        if res.deleted_count == 0:
            raise NotFoundError

        return res.deleted_count

    def search(self, resource_type=None, query_string=None, params=None):
        """
        Searchs for params inside a resource.
        Returns a bundle of items, as required by FHIR standards.

        Args:
            - resource_type: FHIR resource (eg: 'Patient')
            - params: search parameters as returned by the API. For a simple
            search, the parameters should be of the type {"key": "value"}
            eg: {"gender":"female"}, with possible modifiers {"address.city:exact":"Paris"}.
            If a search is made one field with multiple arguments (eg: language is French
            OR English), params should be a payload of type {"multiple": {"language":
            ["French", "English"]}}.
            If a search has more than one field queried, params should be a payload of
            the form: {"address.city": ["Paris"], "multiple":
            {"language": ["French", "English"]}}.
        Returns: A bundle with the results of the search, as required by FHIR
        search standard.
        """
        if resource_type:
            self.validate_resource_type(resource_type)

        search_context = SearchContext(self.search_engine, resource_type)
        fhir_search = Search(search_context, query_string=query_string, params=params)
        try:
            return fhir_search()
        except elasticsearch.exceptions.NotFoundError as e:
            return OperationOutcome(
                **{
                    "issue": [
                        {
                            "severity": "error",
                            "code": "invalid",
                            "diagnostics": f"{e.info['error']['index']}"
                            "is not indexed in the database yet.",
                        }
                    ]
                }
            )
        except elasticsearch.exceptions.RequestError as e:
            return OperationOutcome(
                **{
                    "issue": [
                        {
                            "severity": "error",
                            "code": "invalid",
                            "diagnostics": e.info["error"]["root_cause"],
                        }
                    ]
                }
            )
        except elasticsearch.exceptions.AuthenticationException as e:
            return OperationOutcome(
                **{
                    "issue": [
                        {
                            "severity": "error",
                            "code": "invalid",
                            "diagnostics": e.info["error"]["root_cause"],
                        }
                    ]
                }
            )
        except pydantic.ValidationError as e:
            issues = []
            for err in e.errors():
                issues.append(
                    {
                        "severity": "error",
                        "code": "invalid",
                        "diagnostics": f"{err['msg']}: "
                        f"{','.join([f'{e.model.get_resource_type()}.{l}' for l in err['loc']])}",
                    }
                )
            return OperationOutcome(**{"issue": issues})
        except fhirpath.exceptions.ValidationError as e:
            return OperationOutcome(
                **{"issue": [{"severity": "error", "code": "invalid", "diagnostics": str(e)}]}
            )

    def upload_bundle(self, bundle):
        """
        Upload a bundle of resource instances to the store.

        Args:
            - bundle: the fhir bundle containing the resources.
        """
        if "resourceType" not in bundle or bundle["resourceType"] != "Bundle":
            raise Exception("input must be a FHIR Bundle resource")

        for entry in bundle["entry"]:
            if "resource" not in entry:
                raise Exception("Bundle entry is missing a resource.")

            try:
                self.create(entry["resource"])
            except DuplicateKeyError as e:
                logging.warning(f"Document already existed: {e}")
