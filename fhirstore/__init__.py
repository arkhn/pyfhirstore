from enum import Enum

ARKHN_TERMINOLOGY_SYSTEM = "http://terminology.arkhn.org/CodeSystem"


class ARKHN_CODE_SYSTEMS(Enum):
    resource = f"{ARKHN_TERMINOLOGY_SYSTEM}/resource"
    source = f"{ARKHN_TERMINOLOGY_SYSTEM}/source"


from .fhirstore import FHIRStore, BadRequestError, NotFoundError  # noqa
