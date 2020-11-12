import sys
import logging
from typing import Union, Dict, Optional
import json

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import (
    NotFoundError as ESNotFoundError,
    RequestError as ESRequestError,
    AuthenticationException as ESAuthenticationException,
)
import pydantic
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from tqdm import tqdm

import fhirpath
from fhirpath.enums import FHIR_VERSION
from fhirpath.search import SearchContext, Search
from fhir.resources import construct_fhir_element, FHIRAbstractModel
from fhir.resources.operationoutcome import OperationOutcome
from fhir.resources.bundle import Bundle

from fhirstore import ARKHN_CODE_SYSTEMS
from fhirstore.errors import (
    FHIRStoreError,
    NotSupportedError,
    ValidationError,
    DuplicateError,
    RequiredError,
    NotFoundError,
)
from fhirstore.search_engine import ElasticSearchEngine


class FHIRStore:
    def __init__(self, mongo_client: MongoClient, es_client: Elasticsearch, db_name: str):
        self.es = es_client
        self.db = mongo_client[db_name]
        self.resources = self.db.list_collection_names()

        if self.es and len(self.es.transport.hosts) > 0:
            self.search_engine = ElasticSearchEngine(FHIR_VERSION.R4, self.es, db_name)
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
            raise FHIRStoreError("You also need to drop ES indices when resetting mongo")

        if mongo:
            for collection in self.resources:
                self.db.drop_collection(collection)
            self.resources = []

        if es:
            self.search_engine.reset()

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
            resources = tqdm(resources, file=sys.stdout, desc="Bootstrapping collections...")
        for resource_type in resources:
            self.db.create_collection(resource_type)
            # Add unique constraint on id
            self.db[resource_type].create_index("id", unique=True)
            # Add unique constraint on (identifier.system, identifier.value)
            self.db[resource_type].create_index(
                [("identifier.value", ASCENDING), ("identifier.system", ASCENDING),],
                unique=True,
                partialFilterExpression={
                    "identifier.value": {"$exists": True},
                    "identifier.system": {"$exists": True},
                },
            )

    def normalize_resource(self, resource: Union[Dict, FHIRAbstractModel]) -> FHIRAbstractModel:
        if isinstance(resource, dict):
            resource_type = resource.get("resourceType")
            if not resource_type:
                raise RequiredError("resourceType is missing")
            elif resource_type not in self.resources:
                raise NotSupportedError(f'unsupported FHIR resource: "{resource_type}"')
            return construct_fhir_element(resource.get("resourceType"), resource)

        elif not isinstance(resource, FHIRAbstractModel):
            raise FHIRStoreError("Provided resource must be of type Union[Dict, FHIRAbstractModel]")

        return resource

    def create(
        self, resource: Union[Dict, FHIRAbstractModel]
    ) -> Union[FHIRAbstractModel, OperationOutcome]:
        """
        Creates a resource. The structure of the resource will be checked
        against its json-schema FHIR definition.

        Args:
            - resource: either a dict with the resource data or a fhir.resources.FHIRAbstractModel

        Returns: The created resource as fhir.resources.FHIRAbstractModel.
        """
        try:
            resource = self.normalize_resource(resource)
        except pydantic.ValidationError as e:
            return ValidationError(e).format()
        except FHIRStoreError as e:
            return e.format()

        try:
            self.db[resource.resource_type].insert_one(json.loads(resource.json()))
        except DuplicateKeyError as e:
            return DuplicateError(
                f"Resource {resource.resource_type} {resource.id} already exists: {e}"
            ).format()

        return resource

    def read(self, resource_type, instance_id) -> Union[FHIRAbstractModel, OperationOutcome]:
        """
        Finds a resource given its type and id.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The found resource.
        """
        if resource_type not in self.resources:
            return NotSupportedError(f'unsupported FHIR resource: "{resource_type}"').format()

        res = self.db[resource_type].find_one({"id": instance_id}, projection={"_id": False})
        if res is None:
            return NotFoundError(f"{resource_type} with id {instance_id} not found").format()

        return construct_fhir_element(resource_type, res)

    def update(self, instance_id, resource) -> Union[FHIRAbstractModel, OperationOutcome]:
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
            return ValidationError(e).format()
        except FHIRStoreError as e:
            return e.format()

        update_result = self.db[resource.resource_type].replace_one(
            {"id": instance_id}, json.loads(resource.json())
        )
        if update_result.matched_count == 0:
            return NotFoundError(
                f"{resource.resource_type} with id {instance_id} not found"
            ).format()

        return resource

    def patch(
        self, resource_type, instance_id, patch
    ) -> Union[FHIRAbstractModel, OperationOutcome]:
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
        if resource_type not in self.resources:
            return NotSupportedError(f'unsupported FHIR resource: "{resource_type}"').format()

        res = self.db[resource_type].find_one({"id": instance_id}, projection={"_id": False})
        if res is None:
            return NotFoundError(f"{resource_type} with id {instance_id} not found").format()

        patched_resource = {**construct_fhir_element(resource_type, res).dict(), **patch}
        try:
            resource = self.normalize_resource(patched_resource)
        except pydantic.ValidationError as e:
            return ValidationError(e).format()
        except FHIRStoreError as e:
            return e.format()

        update_result = self.db[resource_type].update_one({"id": instance_id}, {"$set": patch})
        if update_result.matched_count == 0:
            return NotFoundError(f"{resource_type} with id {instance_id} not found").format()

        return resource

    def delete(
        self, resource_type, instance_id=None, resource_id=None, source_id=None
    ) -> OperationOutcome:
        """
        Deletes a resource given its type and id.

        Args:
            - resource_type: type of the resource (eg: 'Patient')
            - id: The expected id is the resource 'id', not the
                  internal database identifier ('_id').

        Returns: The id of the deleted resource.
        """
        if resource_type not in self.resources:
            return NotSupportedError(f'unsupported FHIR resource: "{resource_type}"').format()

        if instance_id:
            res = self.db[resource_type].delete_one({"id": instance_id})
            if res.deleted_count == 0:
                return NotFoundError(f"{resource_type} with id {instance_id} not found").format()

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
            if res.deleted_count == 0:
                return NotFoundError(
                    f"{resource_type} with resource_id {resource_id} not found"
                ).format()

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
            if res.deleted_count == 0:
                return NotFoundError(
                    f"{resource_type} with source_id {source_id} not found"
                ).format()

        else:
            raise FHIRStoreError("one of: 'instance_id', 'resource_id' or 'source_id' are required")

        return OperationOutcome(
            issue=[
                {
                    "severity": "information",
                    "code": "informational",
                    "diagnostics": f"deleted {res.deleted_count} {resource_type}",
                }
            ]
        )

    def search(
        self, resource_type=None, query_string=None, params=None, as_json=False
    ) -> Union[Bundle, dict, OperationOutcome]:
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
        if resource_type and resource_type not in self.resources:
            return NotSupportedError(f'unsupported FHIR resource: "{resource_type}"').format(
                as_json
            )

        search_context = SearchContext(self.search_engine, resource_type)
        fhir_search = Search(search_context, query_string=query_string, params=params)
        try:
            return fhir_search(as_json=as_json)
        except ESNotFoundError as e:
            return NotFoundError(
                f"{e.info['error']['index']} is not indexed in the database yet."
            ).format(as_json)
        except (ESRequestError, ESAuthenticationException) as e:
            return FHIRStoreError(e.info["error"]["root_cause"]).format(as_json)
        except ESAuthenticationException as e:
            return FHIRStoreError(e.info["error"]["root_cause"]).format(as_json)
        except pydantic.ValidationError as e:
            return ValidationError(e).format(as_json)
        except fhirpath.exceptions.ValidationError as e:
            return ValidationError(str(e)).format(as_json)

    def upload_bundle(self, bundle) -> Union[None, OperationOutcome]:
        """
        Upload a bundle of resource instances to the store.

        Args:
            - bundle: the fhir bundle containing the resources.
        """
        if "resourceType" not in bundle or bundle["resourceType"] != "Bundle":
            return FHIRStoreError(
                f"input must be a FHIR Bundle resource, got {bundle.get('resourceType')}"
            ).format()

        try:
            res = self.create({**bundle, "entry": []})
            if isinstance(res, OperationOutcome):
                logging.error(
                    f"could not upload bundle {bundle['id']}: "
                    f"{[i.diagnostics for i in res.issue]}"
                )
        except DuplicateKeyError as e:
            logging.warning(f"Bundle already uploaded: {e}")

        for entry in bundle["entry"]:
            if "resource" not in entry:
                return RequiredError("Bundle entry is missing a resource.")

            try:
                res = self.create(entry["resource"])
                if isinstance(res, OperationOutcome):
                    logging.error(
                        f"could not upload resource {entry['resource']['resourceType']} "
                        f"with id {entry['resource']['id']}: {[i.diagnostics for i in res.issue]}"
                    )

            except DuplicateKeyError as e:
                logging.warning(f"Document already existed: {e}")

        return None
