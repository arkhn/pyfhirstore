import pytest
import json
from pytest import raises
from time import sleep

import fhirpath
from fhir.resources.bundle import Bundle

from fhirstore import FHIRStore, NotFoundError

# import logging
# logging.basicConfig(level=logging.DEBUG)

# These tests assumes an already existing store exists
# (store.bootstrap was run)


@pytest.fixture
def index_resources(request, es_client):
    marker = request.node.get_closest_marker("resources")
    if marker is None:
        return None

    indexed_resource_ids = []
    resource_paths = marker.args
    for path in resource_paths:
        # read and index the resource is ES
        with open(f"test/fixtures/{path}") as f:
            r = json.load(f)
            res = es_client.index(
                index="fhirstore", body={r["resourceType"]: r}, refresh="wait_for"
            )
            indexed_resource_ids.append(res["_id"])

    yield r

    # cleanup ES
    for r_id in indexed_resource_ids:
        es_client.delete("fhirstore", r_id, refresh="wait_for")


def test_search_bad_resource_type(store: FHIRStore):
    """search() raises error if resource type is unknown"""

    with raises(NotFoundError, match='unsupported FHIR resource: "unknown"'):
        store.search("unknown", params={})


# BASIC SEARCH PARAMETERS OPERATIONS


@pytest.mark.resources(
    "patient-example.json", "patient-example-2.json", "patient-example-with-extensions.json"
)
def test_search_all_of(store: FHIRStore, index_resources):
    """Check that the output type is correct
    """
    result = store.search("Patient", params={})
    assert isinstance(result, Bundle)
    assert result.total == 3


@pytest.mark.resources(
    "patient-example.json", "patient-example-2.json", "patient-example-with-extensions.json"
)
def test_search_all_of_qs(store: FHIRStore, index_resources):
    """Check that the output type is correct
    """
    result = store.search("Patient", query_string="")
    assert isinstance(result, Bundle)
    assert result.total == 3


def test_search_not_found(store: FHIRStore):
    """Check that the output type is correct
    """
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="gender=male")


def test_searchparam_not_exist(store: FHIRStore):
    """An error should be returned if a provided search parameter is unknown
    """

    with raises(
        fhirpath.exceptions.ValidationError,
        match="No search definition is available for "
        "search parameter ``kouakou`` on Resource ``Patient``",
    ):
        store.search("Patient", query_string="kouakou=XXX")


@pytest.mark.resources("patient-example.json")
def test_searchparam_single(store: FHIRStore, index_resources):
    """Search on a resource using a single searchparam
    """
    result = store.search("Patient", query_string="identifier=654321")
    assert result.total == 1
    assert result.entry[0].resource.identifier[0].value == "654321"


@pytest.mark.resources("patient-example.json")
def test_searchparam_multiple(store: FHIRStore, index_resources):
    """Search on a resource using multiple searchparams
    """
    result = store.search("Patient", query_string="identifier=654321&gender=male")
    assert result.total == 1
    assert result.entry[0].resource.identifier[0].value == "654321"
    assert result.entry[0].resource.gender == "male"


@pytest.mark.resources("patient-example.json")
def test_searchparam_and(store: FHIRStore, index_resources):
    """Search on a resource matching multiple criterias
    All parameters must be satisfied (AND opeation)
    """
    result = store.search("Patient", query_string="given=Duck&given=Ducky")
    assert result.total == 1
    assert "Duck" in result.entry[0].resource.name[0].given
    assert "Ducky" in result.entry[0].resource.name[1].given


@pytest.mark.resources(
    "patient-example.json", "patient-example-2.json", "patient-example-with-extensions.json"
)
def test_searchparam_or(store: FHIRStore, index_resources):
    """Search on an attribute that may match multiple values
    Performs an OR operation.
    """
    result = store.search("Patient", query_string="family=Chalmers,Levin")
    assert result.total == 2
    assert all(
        entry.resource.name[0].family == "Chalmers" or entry.resource.name[0].family == "Levin"
        for entry in result.entry
    )


@pytest.mark.resources(
    "patient-example.json", "patient-example-2.json", "patient-example-with-extensions.json"
)
def test_searchparam_and_or_combined(store: FHIRStore, index_resources):
    """Performs a search using simultaneous OR predicates
    This query reads `search patients which name.given is "Duck" OR "Peter"
    AND which address.city is "Paris" OR "PleassantVille"`
    """
    result = store.search(
        "Patient", query_string="given=Duck,Peter&address-city=Paris,PleasantVille"
    )
    assert result.total == 1
    assert "Peter" in result.entry[0].resource.name[0].given
    assert result.entry[0].resource.address[0].city == "PleasantVille"


def test_searchparam_casing(store: FHIRStore):
    """A search parameter must be case sensitive
    """
    with raises(
        fhirpath.exceptions.ValidationError,
        match="No search definition is available for search parameter "
        "``Given`` on Resource ``Patient``.",
    ):
        store.search("Patient", query_string="Given=Duck")


@pytest.mark.resources("patient-example.json")
def test_searchparam_complex(store: FHIRStore, index_resources):
    """A searchparam may target multiple fields of a resource
    Eg: address may match any of the string fields in the Address, including line, city, district,
    state, country, postalCode, and/or text.
    """
    result = store.search("Patient", query_string="address=Verson")
    assert result.total == 1
    assert result.entry[0].resource.address[0].city == "Verson"


# STANDARD SEARCH PARAMETERS THAT APPLY TO ALL RESOURCES


@pytest.mark.skip
def test_searchparam_standard_content(store: FHIRStore):
    """The _content param performs text search against the whole resource
    """
    pass


@pytest.mark.resources("patient-example.json", "patient-example-2.json")
def test_searchparam_standard_id(store: FHIRStore, index_resources):
    """The _id param searches on Resource.id
    """
    result = store.search("Patient", query_string="_id=pat1")
    assert result.total == 1
    assert result.entry[0].resource.id == "pat1"


@pytest.mark.resources("patient-example.json", "patient-example-2.json")
def test_searchparam_standard_lastUpdated(store: FHIRStore, index_resources):
    """The _lastUpdated param searches on Resource.meta.lastUpdated
    """
    instant = "2020-01-01T00:00:00Z"

    # find documents updated later than <instant>
    result = store.search("Patient", query_string=f"_lastUpdated=gt{instant}")
    assert result.total == 1
    assert str(result.entry[0].resource.meta.lastUpdated) > instant

    # find documents updated earlier than <instant>
    result = store.search("Patient", query_string=f"_lastUpdated=lt{instant}")
    assert result.total == 1
    assert str(result.entry[0].resource.meta.lastUpdated) < instant


@pytest.mark.resources("patient-example.json", "patient-example-with-extensions.json")
def test_searchparam_standard_profile(store: FHIRStore, index_resources):
    """The _profile param searches on Resource.meta.profile
    """
    result = store.search(
        "Patient", query_string="_profile=http://hl7.org/fhir/StructureDefinition/patient-custom"
    )
    assert result.total == 1
    assert (
        "http://hl7.org/fhir/StructureDefinition/patient-custom"
        in result.entry[0].resource.meta.profile
    )


@pytest.mark.skip()  # fhirpath does not handle it yet (Resource.meta does not seem to be indexed)
def test_searchparam_standard_query(store: FHIRStore):
    """The _query param performs precise queries, complex decision support-based requests,
    and direct queries that have human resolution.
    """
    pass


@pytest.mark.skip()  # fhirpath does not handle it yet (Resource.meta does not seem to be indexed)
def test_searchparam_standard_security(store: FHIRStore):
    """The _id param searches on Resource.meta.security
    """
    pass


@pytest.mark.resources("patient-example.json")
def test_searchparam_standard_tag(store: FHIRStore, index_resources):
    """The _tag param searches on Resource.meta.tag
    """
    # _tag=system|code
    result = store.search(
        "Patient", query_string="_tag=http://terminology.hl7.org/CodeSystem/v3-ActReason|HTEST"
    )
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search(
            "Patient", query_string="_tag=http://terminology.hl7.org/CodeSystem/v3-ActReason|WHAT"
        )

    # _tag=code
    result = store.search("Patient", query_string="_tag=HTEST")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="_tag=WHAT")


@pytest.mark.skip()  # fhirpath does not index Resource.text.div (Narrative) yet
def test_searchparam_standard_text(store: FHIRStore):
    """The _text param performs text search against the narrative of the resource
    """
    pass


@pytest.mark.skip()  # custom filtering is not implemented
def test_searchparam_standard_filter(store: FHIRStore):
    """The _filter param performs advanced filtering
    """
    pass


# SEARCH PARAMETER TYPES
# Each search parameter is defined by a type that specifies how the search parameter behaves


@pytest.mark.resources("patient-example.json")
def test_searchparam_type_string(store: FHIRStore, index_resources):
    """Handle string search parameters
    Search is case-insensitive and accent-insensitive. May match just the start of a string.
    String parameters may contain spaces.
    """
    # regular
    result = store.search("Patient", query_string="family=Donald")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="family=Unknown")

    # TODO: accent-insensitive
    # result = store.search("Patient", query_string="family=Donàld")
    # assert result.total == 1

    # TODO: case insensitive
    result = store.search("Patient", query_string="family=donald")
    assert result.total == 1

    # with spaces
    result = store.search("Patient", query_string="address=Basse Normandie")
    assert result.total == 1


@pytest.mark.skip()  # custom filtering is not implemented
def test_searchparam_type_special(store: FHIRStore):
    """Handle special search parameters
    """
    pass


@pytest.mark.resources("patient-example.json")
def test_searchparam_type_token_identifier(store: FHIRStore, index_resources):
    """Handle token search parameters
    Search parameter on a coded element or identifier. May be used to search through the text,
    display, code and code/codesystem (for codes) and label, system and key (for identifier).
    Its value is either a string or a pair of namespace and value, separated by a "|", depending
    on the modifier used.
    """

    # [parameter]=[code]: the value of [code] matches a Identifier.value
    # irrespective of the value of the system property
    result = store.search("Patient", query_string="identifier=654321")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="identifier=654322")

    # [parameter]=[system]|[code]: the value of [code] matches an Identifier.value,
    # and the value of [system] matches the system property of the Identifier
    result = store.search("Patient", query_string="identifier=urn:oid:0.1.2.3.4.5.6.7|654321")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="identifier=other|654321")

    # TODO: not working yet, when omitting the system, the search is applied only on the value
    # (it should also filter by empty system)
    # [parameter]=|[code]: the value of [code] matches a Identifier.value, and the
    # Identifier has no system property
    result = store.search("Patient", query_string="identifier=|654321")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="identifier=|654322")

    # [parameter]=[system]|: any element where the value of [system] matches the system property of
    # the Identifier
    result = store.search("Patient", query_string="identifier=urn:oid:0.1.2.3.4.5.6.7|")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="identifier=other|")


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_type_token_code(store: FHIRStore, index_resources):
    """Handle token search parameters
    Search parameter on a coded element or identifier. May be used to search through the text,
    display, code and code/codesystem (for codes) and label, system and key (for identifier).
    Its value is either a string or a pair of namespace and value, separated by a "|", depending
    on the modifier used.
    """

    # [parameter]=[code]: the value of [code] matches a Coding.code
    # irrespective of the value of the system property
    result = store.search("Observation", query_string="category=vital-signs")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="category=other")

    # [parameter]=[system]|[code]: the value of [code] matches a Coding.code,
    # and the value of [system] matches the system property of the Coding
    result = store.search(
        "Observation",
        query_string="category=http://terminology.hl7.org/CodeSystem/"
        "observation-category|vital-signs",
    )
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="category=other|vital-signs")

    # TODO: not working yet, when omitting the system, the search is applied only on the value
    # (it should also filter by empty system)
    # [parameter]=|[code]: the value of [code] matches a Coding.code and the
    # Coding has no system property
    result = store.search("Observation", query_string="category=|vital-signs")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="category=|other")

    # [parameter]=[system]|: any element where the value of [system] matches the system property of
    # the Coding
    result = store.search(
        "Observation",
        query_string="category=http://terminology.hl7.org/CodeSystem/observation-category|",
    )
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="category=other|")


@pytest.mark.resources("patient-example.json")
def test_searchparam_type_date_date(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "date"
    The date format is the standard XML format, though other formats may be supported
    """
    # eq
    result = store.search("Patient", query_string="birthdate=1995-12-25")
    assert result.total == 1
    result = store.search("Patient", query_string="birthdate=eq1995-12-25")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="birthdate=2009-09-09")

    # gt
    result = store.search("Patient", query_string="birthdate=gt1990-01-01")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="birthdate=gt2000-01-01")

    # lt
    result = store.search("Patient", query_string="birthdate=lt2000-01-01")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="birthdate=lt1990-01-01")

    # ne
    result = store.search("Patient", query_string="birthdate=ne1569-12-25")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="birthdate=ne1995-12-25")


@pytest.mark.resources("patient-example.json")
def test_searchparam_type_date_datetime(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "datetime"
    The date format is the standard XML format, though other formats may be supported
    """

    # TODO: the searchparam "date" on Observation should match effectiveDatetime or
    # effectivDuration or effectiveTiming or effectiveInstant. Currently, it tries to
    # search on Observation.effecive (which does not exist).
    # result = store.search("Observation", query_string="date=1999-07-02")
    # assert result.total == 1
    # with raises(fhirpath.exceptions.NoResultFound):
    #     store.search("Observation", query_string="date=2009-09-09")

    # TODO: SEARCH ON "datetime" TYPE WITHOUT SPECIFYING HOURS !
    # eg: Patient?death-date=2098-01-01
    # eq
    result = store.search("Patient", query_string="death-date=2098-01-01T12:10:30")
    assert result.total == 1
    result = store.search("Patient", query_string="death-date=eq2098-01-01T12:10:30")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="death-date=2009-09-09T12:10:30")

    # gt
    result = store.search("Patient", query_string="death-date=gt1990-01-01T00:00:00")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="death-date=gt2100-01-01T00:00:00")

    # lt
    result = store.search("Patient", query_string="death-date=lt2100-01-01T00:00:00")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="death-date=lt1990-01-01T00:00:00")

    # ne
    result = store.search("Patient", query_string="death-date=ne1990-01-01T00:00:00")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Patient", query_string="death-date=ne2098-01-01T12:10:30")


@pytest.mark.skip()
def test_searchparam_type_date_instant(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "instant"
    The date format is the standard XML format, though other formats may be supported
    """
    pass


@pytest.mark.skip()
def test_searchparam_type_date_period(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    """
    pass


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_type_reference_literal(store: FHIRStore, index_resources):
    """Handle reference search parameters (Reference or canonical)
    """

    # TODO
    # [parameter]=[id] the logical [id] of a resource using
    # a local reference (i.e. a relative reference)
    # result = store.search("Observation", query_string="subject=pat1")
    # assert result.total == 1
    # with raises(fhirpath.exceptions.NoResultFound):
    #     store.search("Observation", query_string="subject=patUnknown")

    # [parameter]=[type]/[id] the logical [id] of a resourceof a specified type using a local
    # reference (i.e. a relative reference), for when the reference can point to different
    # types of resources (e.g. Observation.subject)
    result = store.search("Observation", query_string="subject=Patient/pat1")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="subject=Patient/patUnknown")

    # [parameter]=[url] where the [url] is an absolute URL - a reference to a resource by its
    # absolute location, or by it's canonical URL
    result = store.search(
        "Observation", query_string="encounter=https://staging.arkhn.om/api/Encounter/f201"
    )
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search(
            "Observation", query_string="encounter=https://staging.arkhn.om/api/Encounter/unknown"
        )


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_type_reference_identifier(store: FHIRStore, index_resources):
    """Handle reference search parameters using the logical identifier.
    The modifier :identifier allows for searching by the
    identifier rather than the literal reference
    """

    # [param-ref]:identifier=[value]
    result = store.search("Observation", query_string="subject:identifier=654321")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="subject:identifier=123456789")

    # [param-ref]:identifier=[system]|[value]: the value of [code] matches an
    # reference.identifier.value, and the value of [system] matches the system
    # property of the Identifier
    result = store.search(
        "Observation", query_string="subject:identifier=urn:oid:0.1.2.3.4.5.6.7|654321"
    )
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search(
            "Observation", query_string="subject:identifier=urn:oid:0.1.2.3.4.5.6.7|123456789"
        )

    # TODO: not working yet, when omitting the system, the search is applied only on the value
    # (it should also filter by empty system)
    # [param-ref]:identifier=|[code]: the value of [code] matches a reference.identifier.value,
    # and the Identifier has no system property
    result = store.search("Observation", query_string="subject:identifier=|654321")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="subject:identifier=|123456789")

    # [param-ref]:identifier=[system]|: any element where the value of [system] matches the
    # system property of the Identifier
    result = store.search("Observation", query_string="subject:identifier=urn:oid:0.1.2.3.4.5.6.7|")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="subject:identifier=other|")


def test_searchparam_type_composite(store: FHIRStore):
    """Handle composite search parameter that combines a search on two values together.
    """
    pass


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_type_quantity(store: FHIRStore, index_resources):
    """Handle quantity search parameters (precision is 0.000001)
    """
    # all observations with a value of exactly 66.899999 (irrespective of the unit)
    result = store.search("Observation", query_string="value-quantity=66.899999")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="value-quantity=9")

    # all observations with a value greater/lower than 50 (irrespective of the unit)
    result = store.search("Observation", query_string="value-quantity=gt50")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="value-quantity=lt50")

    # Search for all the observations with a value of 66.899999(+/-0.05)
    # where "[in_i]" is understood as a UCUM unit (system/code)
    result = store.search(
        "Observation", query_string="value-quantity=66.899999|http://unitsofmeasure.org|[in_i]",
    )
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search(
            "Observation",
            query_string="value-quantity=66.899999|http://unitsofmeasure.org|[other]",
        )
    with raises(fhirpath.exceptions.NoResultFound):
        store.search(
            "Observation", query_string="value-quantity=66.899999|http://other.org|[in_i]",
        )

    # Search for all the observations with a value of 5.4(+/-0.05) mg where the
    # unit - either the code (code) or the stated human unit (unit) are "in"
    result = store.search("Observation", query_string="value-quantity=66.899999||in")
    assert result.total == 1
    with raises(fhirpath.exceptions.NoResultFound):
        store.search("Observation", query_string="value-quantity=66.899999||other")


def test_searchparam_type_uri(store: FHIRStore):
    """Handle uri search parameters
    """
    pass


# SEARCH PARAMETERS MODIFIERS
# Parameters are defined per resource. Parameter names may specify a modifier as a suffix.
# The modifiers are separated from the parameter name by a colon.


def test_searchparam_modifier_missing(store: FHIRStore):
    """Handle :missing modifier
    For all parameters.
    Searching for gender:missing=true will return all the resources that don't
    have a value for the gender parameter
     """
    pass


def test_searchparam_modifier_exact(store: FHIRStore):
    """Handle :exact modifier
    For string: :exact returns results that match the entire supplied parameter,
    including casing and combining characters
    """
    pass


def test_searchparam_modifier_contains(store: FHIRStore):
    """Handle :contains modifier
    For string: case insensitive and combining character-insensitive,
    search text matched anywhere in the string
    """
    pass


def test_searchparam_modifier_text(store: FHIRStore):
    """Handle :text modifier
    For token: :text (the match does a partial searches on the text portion of a CodeableConcept
    or the display portion of a Coding), instead of the default search which uses codes.
    """
    pass


def test_searchparam_modifier_type(store: FHIRStore):
    """Handle :[type] modifier
    For reference: :[type] where [type] is the name of a type of resource, :identifier
    """
    pass


def test_searchparam_modifier_below(store: FHIRStore):
    """Handle :below modifier
    For uri: :below indicate that instead of an exact match, either the search term
    left-matches the value
    """
    pass


def test_searchparam_modifier_above(store: FHIRStore):
    """Handle :above modifier
    For uri: :above indicate that instead of an exact match, either the search term
    right-matches the value
    """
    pass


# SEARCH PARAMETERS PREFIXES
# For the ordered parameter types of number, date, and quantity, a prefix to the parameter value
# may be used to control the nature of the matching.


def test_searchparam_prefix_eq(store: FHIRStore):
    """Handle :eq prefix
    the value for the parameter in the resource is equal to the provided value
    """
    pass


def test_searchparam_prefix_ne(store: FHIRStore):
    """Handle :ne prefix
    the value for the parameter in the resource is not equal to the provided value
    """
    pass


def test_searchparam_prefix_gt(store: FHIRStore):
    """Handle :gt prefix
    the value for the parameter in the resource is greater than the provided value
    """
    pass


def test_searchparam_prefix_lt(store: FHIRStore):
    """Handle :lt prefix
    the value for the parameter in the resource is less than the provided value
    """
    pass


def test_searchparam_prefix_ge(store: FHIRStore):
    """Handle :ge prefix
    the value for the parameter in the resource is greater or equal to the provided value
    """
    pass


def test_searchparam_prefix_le(store: FHIRStore):
    """Handle :le prefix
    the value for the parameter in the resource is less or equal to the provided value
    """
    pass


def test_searchparam_prefix_sa(store: FHIRStore):
    """Handle :sa prefix
    the value for the parameter in the resource starts after the provided value
    """
    pass


def test_searchparam_prefix_eb(store: FHIRStore):
    """Handle :eb prefix
    the value for the parameter in the resource ends before the provided value
    """
    pass


def test_searchparam_prefix_ap(store: FHIRStore):
    """Handle :eq prefix
    the value for the parameter in the resource is approximately the same to the provided value.
    Note that the recommended value for the approximation is 10% of the stated value
    """
    pass


# CHAINED PARAMETERS
# reference parameters may be "chained" by appending them with a period (.) followed by the name of
# a search parameter defined for the target resource. This can be done recursively, following a
# logical path through a graph of related resources, separated by .
# For instance, given that the resource DiagnosticReport has a search parameter named subject,
# which is usually a reference to a Patient resource, and the Patient resource includes a parameter
# name which searches on patient name, then the search
# GET [base]/DiagnosticReport?subject.name=peter
# is a request to return all the lab reports that have a subject whose name includes "peter".


def test_searchparam_chained_simple(store: FHIRStore):
    """Handle a single chained parameter
    DiagnosticReport?subject.name=peter
    """
    pass


def test_searchparam_chained_multiple(store: FHIRStore):
    """Handle multiple chained parameters
    Patient?general-practitioner.name=Joe&general-practitioner.address-state=MN
    """
    pass


def test_searchparam_chained_typed(store: FHIRStore):
    """Handle a typed chained parameter
    Patient?general-practitioner.name=Joe&general-practitioner.address-state=MN
    """
    pass


# REVERSE CHAINING
# The _has parameter provides limited support for reverse chaining - that is, selecting resources
# based on the properties of resources that refer to them (instead of chaining, above, where
# resources can be selected based on the properties of resources that they refer to)


def test_searchparam_reverse_chaining(store: FHIRStore):
    """Handle a single chained parameter
    Patient?_has:Observation:patient:code=1234-5
    """
    pass


def test_searchparam_reverse_chaining_chained(store: FHIRStore):
    """Handle a single chained parameter
    Patient?_has:Observation:patient:_has:AuditEvent:entity:user=MyUserId
    """
    pass


# SEARCHING BY LIST
# The _list parameter allows for the retrieval of resources that are referenced by a List resource.


def test_searchparam_list(store: FHIRStore):
    """Handle _list parameter
    Patient?_list=42&gender=female
    This request returns all female Patient resources that are referenced from the list found at
    List/42 in List.entry.item
    """
    pass


# SEARCHING ON MULTIPLE RESOURCES
# When searching on multiple resources,the search criteria may need to specify one or more resource
# types that the search applies to. This can be done by using the _type parameter.
#
# If no type is specified, the only search parameters that be can be used in global search like
# this are the base parameters that apply to all resources. If multiple types are specified, any
# search parameters shared across the entire set of specified resources may be used


def test_searchparam_mutltiple_types(store: FHIRStore):
    """Handle searching on multiple resource types
    GET [base]/?params...
    """
    pass


def test_searchparam_mutltiple_types_bad_param(store: FHIRStore):
    """Handle searching on multiple resource types
    GET [base]/?params...
    """
    pass


def test_searchparam_mutltiple_types_constrained(store: FHIRStore):
    """Handle searching on multiple resource types while restraining the returned resources
    GET [base]/?_type=Observation,Condition&other params...
    """
    pass


# SORTING RESULTS
# The client can indicate which order to return the results by using the parameter _sort, which
# can contain a comma-separated list of sort rules in priority order.


def test_searchparam_sort(store: FHIRStore):
    """Handle searching on multiple resource types while restraining the returned resources
    Observation?_sort=status,-date,category
    """
    pass


# PAGE COUNT
# The parameter _count is defined as an instruction to the server regarding how many resources
# should be returned in a single page.
# If _count has the value 0, this shall be treated the same as _summary=count.


def test_searchparam_count(store: FHIRStore):
    """Handle _count
    Observation?_count=10
    """
    pass


def test_searchparam_count_zero(store: FHIRStore):
    """Handle _count
    Observation?_count=0
    """
    pass


# INCLUDING RESOURCES
# Clients may request that the engine return resources related to the search results, in order to
# reduce the overall network delay of repeated retrievals of related resources. The client can use
# the _include parameter to indicate that the subject resources be included in the results.
#
# An alternative scenario is where the client wishes to fetch a particular resource, and any
# resources that refer to it. This is known as a reverse include, and is specified by providing a
# _revinclude parameter.
#
# Both _include and _revinclude are based on search parameters, rather than paths in the resource,
# since joins, such as chaining, are already done by search parameter.
#
#  Parameter values for both _include and _revinclude have three parts, separated by a : character:
#     - The name of the source resource from which the join comes
#     - The name of the search parameter which must be of type reference
#     - (Optional) A specific of type of target resource (for when the search parameter refers to
#       multiple possible target types)


def test_searchparam_include(store: FHIRStore):
    """Handle _include
    MedicationRequest?_include=MedicationRequest:patient
    """
    pass


def test_searchparam_revinclude(store: FHIRStore):
    """Handle _include
    MedicationRequest?_revinclude=Provenance:target
    """
    pass


# SUMMARY


@pytest.mark.resources("patient-example.json")
def test_searchparam_summary_true(store: FHIRStore, index_resources):
    """Handle _summary=true
    Return a limited subset of elements from the resource. This subset SHOULD consist solely of all
    supported elements that are marked as "summary" in the base definition of the resource(s)
    """
    result = store.search("Patient", query_string="_summary=true")
    assert result.entry[0].resource.text is None
    assert result.entry[0].resource.id is not None
    assert result.entry[0].resource.meta is not None
    assert result.entry[0].resource.birthDate is not None  # birthDate should not be in summary
    assert result.entry[0].resource.maritalStatus is None  # maritalStatus should not be in summary


@pytest.mark.resources("patient-example.json")
def test_searchparam_summary_false(store: FHIRStore, index_resources):
    """Handle _summary=false
    Return all parts of the resource(s)
    """
    result = store.search("Patient", query_string="_summary=false")
    assert result.entry[0].resource.text is not None
    assert result.entry[0].resource.id is not None
    assert result.entry[0].resource.meta is not None
    assert result.entry[0].resource.link is not None
    assert result.entry[0].resource.birthDate is not None


@pytest.mark.resources("patient-example.json")
def test_searchparam_summary_text(store: FHIRStore, index_resources):
    """Handle _summary=text
    Return only the "text" element, the 'id' element, the 'meta' element, and only top-level
    mandatory elements
    """
    result = store.search("Patient", query_string="_summary=text")
    assert result.entry[0].resource.text is not None
    assert result.entry[0].resource.id is not None
    assert result.entry[0].resource.meta is not None
    assert result.entry[0].resource.link is not None  # link is a mandatory top element
    # communication would also be a mandatory top element but is not in the document
    assert result.entry[0].resource.birthDate is None  # birthDate is not mandatory


@pytest.mark.resources("patient-example.json")
def test_searchparam_summary_data(store: FHIRStore, index_resources):
    """Handle _summary=data
    Remove the text element
    """
    result = store.search("Patient", query_string="_summary=data")
    assert result.entry[0].resource.text is None
    assert result.entry[0].resource.id is not None
    assert result.entry[0].resource.meta is not None
    assert result.entry[0].resource.link is not None
    assert result.entry[0].resource.birthDate is not None


@pytest.mark.resources("patient-example.json")
def test_searchparam_summary_count(store: FHIRStore, index_resources):
    """Handle _summary=count
    Search only: just return a count of the matching resources, without returning the actual matches
    """
    result = store.search("Patient", query_string="_summary=count")
    assert result.entry == []
    assert result.total == 1


# ELEMENTS
# a client can request a specific set of elements be returned as part of a resource in the search of
# results using the _elements parameter. The _elements parameter consists of a comma-separated list
# base element names such as, elements defined at the root level in the resource.
# Only elements that are listed are to be returned. Clients SHOULD list all mandatory and modifier
# elements in a resource as part of the list of elements.


def test_searchparam_elements(store: FHIRStore):
    """Handle _elements
    Patient?_elements=identifier,active,link
    """
    pass


def test_searchparam_element_missing_required(store: FHIRStore):
    """Handle _elements
    Clients must select all required attributes otherwise an error is returned.
    """
    pass
