from typing import Union, List

import pydantic
from fhir.resources.operationoutcome import OperationOutcome


class FHIRStoreError(Exception):
    errors = None

    def __init__(self, error: Union[None, str, List[str]] = None, severity="error", code="invalid"):
        self.severity = severity
        self.code = code
        if isinstance(error, list):
            self.errors = error
        elif isinstance(error, str):
            self.errors = [error]

        super().__init__(self.errors)

    def format(self) -> OperationOutcome:
        issues = [
            {"severity": self.severity, "code": self.code, "diagnostics": err}
            for err in self.errors
        ]
        return OperationOutcome(issue=issues)


class NotSupportedError(FHIRStoreError):
    """
    NotSupportedError is returned when dealing with FHIR resources which are not supported.
    """

    def __init__(self, error: str):
        super().__init__(error, severity="error", code="not-supported")


class ValidationError(FHIRStoreError):
    """
    ValidationError wraps a pydantic.ValidationError to format it in OperationOutcome
    """

    def __init__(self, e: Union[pydantic.ValidationError, str]):
        if isinstance(e, pydantic.ValidationError):
            errors = [
                f"{err['msg'] or 'Validation error'}: "
                f"{e.model.get_resource_type()}.{'.'.join([str(l) for l in err['loc']])}"
                for err in e.errors()
            ]
        elif isinstance(e, str):
            errors = [e]
        else:
            raise FHIRStoreError(
                "ValidationError must be initiated with a pydantic.ValidationError or a string"
            )
        super().__init__(errors, severity="error", code="invalid")


class DuplicateError(FHIRStoreError):
    """
    DuplicateError is returned when attempting to create a duplicate record, collection or index.
    """

    def __init__(self, error: str):
        super().__init__(error, severity="error", code="duplicate")


class RequiredError(FHIRStoreError):
    """
    DuplicateError is returned when a required element is missing.
    """

    def __init__(self, error: str):
        super().__init__(error, severity="error", code="required")


class NotFoundError(FHIRStoreError):
    """
    DuplicateError is returned when a required element is missing.
    """

    def __init__(self, error: str):
        super().__init__(error, severity="error", code="not-found")
