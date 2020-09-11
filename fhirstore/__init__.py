ARKHN_TERMINOLOGY_SYSTEM = "http://terminology.arkhn.org/CodeSystem"


class ARKHN_CODE_SYSTEMS:
    resource = f"{ARKHN_TERMINOLOGY_SYSTEM}/resource"
    source = f"{ARKHN_TERMINOLOGY_SYSTEM}/source"


from .fhirstore import FHIRStore  # noqa
from .errors import (  # noqa
    FHIRStoreError,
    NotSupportedError,
    ValidationError,
    DuplicateError,
    RequiredError,
    NotFoundError,
)
