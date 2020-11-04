import pytest
import json

from fhir.resources.bundle import Bundle
from fhir.resources.operationoutcome import OperationOutcome

from fhirstore import FHIRStore


def assert_empty_bundle(result):
    assert isinstance(result, Bundle)
    assert result.total == 0
    assert len(result.entry) == 0


# These tests assumes an already existing store exists
# (store.bootstrap was run)


@pytest.fixture
def index_resources(request, es_client):
    marker = request.node.get_closest_marker("resources")
    if marker is None:
        return None

    indexed_resource_ids = []
    resource_paths = marker.args
    resources = {}
    for path in resource_paths:
        # read and index the resource is ES
        with open(f"test/fixtures/{path}") as f:
            r = json.load(f)
            resources[r["resourceType"]] = r
            res = es_client.index(index="fhirstore", body={r["resourceType"]: r}, refresh=True)
            indexed_resource_ids.append(res["_id"])

    yield resources

    # cleanup ES
    for r_id in indexed_resource_ids:
        es_client.delete("fhirstore", r_id, refresh=True)


def test_search_bad_resource_type(store: FHIRStore):
    """search() raises error if resource type is unknown"""

    res = store.search("unknown", params={})
    assert isinstance(res, OperationOutcome)
    assert len(res.issue) == 1
    assert res.issue[0].diagnostics == 'unsupported FHIR resource: "unknown"'


# BASIC SEARCH PARAMETERS OPERATIONS


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json", "patient-b966.json")
def test_search_all_of(store: FHIRStore, index_resources):
    """Check that the output type is correct
    """
    result = store.search("Patient", params={})
    assert isinstance(result, Bundle)
    assert result.total == 3


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json", "patient-b966.json")
def test_search_as_json(store: FHIRStore, index_resources):
    """Check that the output type is correct
    """
    # Bundle is returned as a dict
    result = store.search("Patient", params={}, as_json=True)
    assert isinstance(result, dict)
    assert result["total"] == 3

    # OperationOutcome is returned as dict
    result = store.search("Patient", query_string="kouakou=XXX", as_json=True)
    assert isinstance(result, dict)
    assert (
        result["issue"][0]["diagnostics"] == "No search definition is available for search "
        "parameter ``kouakou`` on Resource ``Patient``."
    )



@pytest.mark.resources("patient-pat1.json", "patient-pat2.json", "patient-b966.json")
def test_search_all_of_qs(store: FHIRStore, index_resources):
    """Check that the output type is correct
    """
    result = store.search("Patient", query_string="")
    assert isinstance(result, Bundle)
    assert result.total == 3


def test_search_not_found(store: FHIRStore):
    """Check that an exception is raised if no results is found.
    """
    result = store.search("Patient", query_string="gender=male")
    assert_empty_bundle(result)


def test_searchparam_not_exist(store: FHIRStore):
    """An error should be returned if a provided search parameter is unknown
    """

    result = store.search("Patient", query_string="kouakou=XXX")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "No search definition is available for search parameter "
        "``kouakou`` on Resource ``Patient``."
    )


@pytest.mark.resources("patient-pat1.json")
def test_search_empty(store: FHIRStore, index_resources):
    """En empty search parameter should be ignored
    """
    result = store.search("Patient", query_string="_id=")
    assert result.total == 1


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_single(store: FHIRStore, index_resources):
    """Search on a resource using a single searchparam
    """
    result = store.search("Patient", query_string="identifier=654321")
    assert result.total == 1
    assert result.entry[0].resource.identifier[0].value == "654321"


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_multiple(store: FHIRStore, index_resources):
    """Search on a resource using multiple searchparams
    """
    result = store.search("Patient", query_string="identifier=654321&gender=male")
    assert result.total == 1
    assert result.entry[0].resource.identifier[0].value == "654321"
    assert result.entry[0].resource.gender == "male"


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_and(store: FHIRStore, index_resources):
    """Search on a resource matching multiple criterias
    All parameters must be satisfied (AND operation)
    """
    result = store.search("Patient", query_string="given=Duck&gender=male")
    assert result.total == 1
    assert "Duck" in result.entry[0].resource.name[0].given
    assert result.entry[0].resource.gender == "male"


@pytest.mark.skip()
@pytest.mark.resources("patient-pat1.json")
def test_searchparam_and_same_list_field(store: FHIRStore, index_resources):
    """Search on a resource where an array field must contain multiple values.
    All parameters must be satisfied (AND operation)
    """
    result = store.search("Patient", query_string="given=Duck&given=Ducky")
    assert result.total == 1
    assert "Duck" in result.entry[0].resource.name[0].given
    assert "Ducky" in result.entry[0].resource.name[1].given


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json", "patient-b966.json")
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


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json", "patient-b966.json")
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
    result = store.search("Patient", query_string="Given=Duck")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "No search definition is available for search parameter "
        "``Given`` on Resource ``Patient``."
    )


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_complex(store: FHIRStore, index_resources):
    """A searchparam may target multiple fields of a resource
    Eg: address may match any of the string fields in the Address, including line, city, district,
    state, country, postalCode, and/or text.
    """
    result = store.search("Patient", query_string="address=Verson")
    assert result.total == 1
    assert result.entry[0].resource.address[0].city == "Verson"


# STANDARD SEARCH PARAMETERS THAT APPLY TO ALL RESOURCES


@pytest.mark.skip()
def test_searchparam_standard_content(store: FHIRStore):
    """The _content param performs text search against the whole resource
    """
    pass


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json")
def test_searchparam_standard_id(store: FHIRStore, index_resources):
    """The _id param searches on Resource.id
    """
    result = store.search("Patient", query_string="_id=pat1")
    assert result.total == 1
    assert result.entry[0].resource.id == "pat1"


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json")
def test_searchparam_standard_id_exact(store: FHIRStore, index_resources):
    """The _id param searches on Resource.id
    """
    result = store.search("Patient", query_string="_id:exact=pat1")
    assert result.total == 1
    assert result.entry[0].resource.id == "pat1"


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "patient-b966.json",
    "patient-75414a6f-ffee-4a32-b966-85cc12081603.json",
)
def test_searchparam_standard_id_tokenization(store: FHIRStore, index_resources):
    """The _id param searches on Resource.id
    If the ES tokenizer is not customized to handle IDs containing separators (eg: "-"), IDs will be
    broken down in multiple tokens and results in erroneous search results.
    """
    result = store.search("Patient", query_string="_id=75414a6f-ffee-4a32-b966-85cc12081603")
    assert result.total == 1
    assert result.entry[0].resource.id == "75414a6f-ffee-4a32-b966-85cc12081603"

    result = store.search("Patient", query_string="_id=b966")
    assert result.total == 1
    assert result.entry[0].resource.id == "b966"


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "patient-b966.json",
    "patient-75414a6f-ffee-4a32-b966-85cc12081603.json",
)
def test_searchparam_phone_tokenized(store: FHIRStore, index_resources):
    """The _id param searches on Resource.id
    If the ES tokenizer is not customized to handle strings containing separators (eg: " "),
    strings will be broken down in multiple tokens and results in erroneous search results.
    """
    result = store.search("Patient", query_string="phone=03 77 73 32 56")
    assert result.total == 1

    result = store.search("Patient", query_string="phone=03")
    assert result.total == 0

    result = store.search("Patient", query_string="phone=03 77 73 32 56,09 28 63 57 41")
    assert result.total == 2


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json")
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


@pytest.mark.resources("patient-pat1.json", "patient-b966.json")
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


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_standard_tag(store: FHIRStore, index_resources):
    """The _tag param searches on Resource.meta.tag
    """
    # _tag=system|code
    result = store.search(
        "Patient", query_string="_tag=http://terminology.hl7.org/CodeSystem/v3-ActReason|HTEST"
    )
    assert result.total == 1

    result = store.search(
        "Patient", query_string="_tag=http://terminology.hl7.org/CodeSystem/v3-ActReason|WHAT"
    )
    assert_empty_bundle(result)

    # _tag=code
    result = store.search("Patient", query_string="_tag=HTEST")
    assert result.total == 1

    result = store.search("Patient", query_string="_tag=WHAT")
    assert_empty_bundle(result)


# fhirpath does not index Resource.text.div (Narrative) yet
@pytest.mark.skip()
@pytest.mark.resources("patient-pat1.json")
def test_searchparam_standard_text(store: FHIRStore, index_resources):
    """The _text param performs text search against the narrative of the resource
    """
    result = store.search("Patient", query_string="_text=Patient Donald DUCK @ Acme Healthcare")
    assert result.total == 1

    result = store.search("Patient", query_string="_text=DUCK")
    assert result.total == 1

    result = store.search("Patient", query_string="_text=rocket")
    assert_empty_bundle(result)


# custom filtering is not implemented
@pytest.mark.skip()
def test_searchparam_standard_filter(store: FHIRStore):
    """The _filter param performs advanced filtering
    """
    pass


# SEARCH PARAMETER TYPES
# Each search parameter is defined by a type that specifies how the search parameter behaves


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_type_string(store: FHIRStore, index_resources):
    """Handle string search parameters
    Search is case-insensitive and accent-insensitive. May match just the start of a string.
    String parameters may contain spaces.
    """
    # regular
    result = store.search("Patient", query_string="family=Donald")
    assert result.total == 1

    result = store.search("Patient", query_string="family=Unknown")
    assert_empty_bundle(result)

    # accent-insensitive
    result = store.search("Patient", query_string="family=Donàld")
    assert result.total == 1

    # case insensitive
    result = store.search("Patient", query_string="family=donaLd")
    assert result.total == 1

    # with spaces
    result = store.search("Patient", query_string="address=Basse normandie")
    assert result.total == 1

    # prefix are not taken into account when dealing with strings
    # (le, ge, lt, gt, eq, ne, sa,  eb, ap)
    result = store.search("Patient", query_string="name=leslie")
    assert result.total == 1
    result = store.search("Patient", query_string="name=gerard")
    assert result.total == 1
    result = store.search("Patient", query_string="name=ltamine")
    assert result.total == 1
    result = store.search("Patient", query_string="name=gtlabas")
    assert result.total == 1
    result = store.search("Patient", query_string="name=equarte")
    assert result.total == 1
    result = store.search("Patient", query_string="name=nerval")
    assert result.total == 1
    result = store.search("Patient", query_string="name=samuel")
    assert result.total == 1
    result = store.search("Patient", query_string="name=ebadidonc")
    assert result.total == 1
    result = store.search("Patient", query_string="name=apiculteur")
    assert result.total == 1


# TODO: special search parameters are not yet implemented
@pytest.mark.skip()
def test_searchparam_type_special(store: FHIRStore):
    """Handle special search parameters
    """
    pass


@pytest.mark.resources("patient-pat1.json")
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

    result = store.search("Patient", query_string="identifier=654322")
    assert_empty_bundle(result)

    # [parameter]=[system]|[code]: the value of [code] matches an Identifier.value,
    # and the value of [system] matches the system property of the Identifier
    result = store.search("Patient", query_string="identifier=urn:oid:0.1.2.3.4.5.6.7|654321")
    assert result.total == 1

    result = store.search("Patient", query_string="identifier=other|654321")
    assert_empty_bundle(result)

    # TODO: not working yet, when omitting the system, the search is applied only on the value
    # (it should also filter by empty system)
    # [parameter]=|[code]: the value of [code] matches a Identifier.value, and the
    # Identifier has no system property
    result = store.search("Patient", query_string="identifier=|654321")
    assert result.total == 1

    result = store.search("Patient", query_string="identifier=|654322")
    assert_empty_bundle(result)

    # [parameter]=[system]|: any element where the value of [system] matches the system property of
    # the Identifier
    result = store.search("Patient", query_string="identifier=urn:oid:0.1.2.3.4.5.6.7|")
    assert result.total == 1

    result = store.search("Patient", query_string="identifier=other|")
    assert_empty_bundle(result)

    # prefix are not taken into account when dealing with tokens
    # (le, ge, lt, gt, eq, ne, sa,  eb, ap)
    result = store.search("Patient", query_string="identifier=eq654321")
    assert result.total == 1
    result = store.search("Patient", query_string="identifier=ge654321")
    assert result.total == 1
    result = store.search("Patient", query_string="identifier=lt654321")
    assert result.total == 1
    result = store.search("Patient", query_string="identifier=gt654321")
    assert result.total == 1
    result = store.search("Patient", query_string="identifier=le654321")
    assert result.total == 1
    result = store.search("Patient", query_string="identifier=ne654321")
    assert result.total == 1
    result = store.search("Patient", query_string="identifier=sa654321")
    assert result.total == 1
    result = store.search("Patient", query_string="identifier=eb654321")
    assert result.total == 1
    result = store.search("Patient", query_string="identifier=ap654321")
    assert result.total == 1


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

    result = store.search("Observation", query_string="category=other")
    assert_empty_bundle(result)

    # [parameter]=[system]|[code]: the value of [code] matches a Coding.code,
    # and the value of [system] matches the system property of the Coding
    result = store.search(
        "Observation",
        query_string="category=http://terminology.hl7.org/CodeSystem/"
        "observation-category|vital-signs",
    )
    assert result.total == 1

    result = store.search("Observation", query_string="category=other|vital-signs")
    assert_empty_bundle(result)

    # TODO: not working yet, when omitting the system, the search is applied only on the value
    # (it should also filter by empty system)
    # [parameter]=|[code]: the value of [code] matches a Coding.code and the
    # Coding has no system property
    result = store.search("Observation", query_string="category=|vital-signs")
    assert result.total == 1

    result = store.search("Observation", query_string="category=|other")
    assert_empty_bundle(result)

    # [parameter]=[system]|: any element where the value of [system] matches the system property of
    # the Coding
    result = store.search(
        "Observation",
        query_string="category=http://terminology.hl7.org/CodeSystem/observation-category|",
    )
    assert result.total == 1

    result = store.search("Observation", query_string="category=other|")
    assert_empty_bundle(result)


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_type_date_date(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "date"
    The date format is the standard XML format, though other formats may be supported
    """
    # eq
    result = store.search("Patient", query_string="birthdate=1995-12-25")
    assert result.total == 1
    result = store.search("Patient", query_string="birthdate=eq1995-12-25")
    assert result.total == 1

    result = store.search("Patient", query_string="birthdate=2009-09-09")
    assert_empty_bundle(result)

    # gt
    result = store.search("Patient", query_string="birthdate=gt1990-01-01")
    assert result.total == 1

    result = store.search("Patient", query_string="birthdate=gt2000-01-01")
    assert_empty_bundle(result)

    # lt
    result = store.search("Patient", query_string="birthdate=lt2000-01-01")
    assert result.total == 1

    result = store.search("Patient", query_string="birthdate=lt1990-01-01")
    assert_empty_bundle(result)

    # ne
    result = store.search("Patient", query_string="birthdate=ne1569-12-25")
    assert result.total == 1

    result = store.search("Patient", query_string="birthdate=ne1995-12-25")
    assert_empty_bundle(result)


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_type_date_datetime(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "datetime"
    The date format is the standard XML format, though other formats may be supported
    """

    # TODO: the searchparam "date" on Observation should match effectiveDatetime or
    # effectivDuration or effectiveTiming or effectiveInstant. Currently, it tries to
    # search on Observation.effective (which does not exist).
    # result = store.search("Observation", query_string="date=1999-07-02")
    # assert result.total == 1
    #
    # result = store.search("Observation", query_string="date=2009-09-09")
    # assert_empty_bundle(result)

    # eq
    result = store.search("Patient", query_string="death-date=2098-01-01")
    assert result.total == 1
    result = store.search("Patient", query_string="death-date=eq2098-01-01T12:10:30")
    assert result.total == 1

    result = store.search("Patient", query_string="death-date=2009-09-09T12:10:30")
    assert_empty_bundle(result)

    # gt
    result = store.search("Patient", query_string="death-date=gt1990-01-01T00:00:00")
    assert result.total == 1

    result = store.search("Patient", query_string="death-date=gt2100-01-01T00:00:00")
    assert_empty_bundle(result)

    # lt
    result = store.search("Patient", query_string="death-date=lt2100-01-01T00:00:00")
    assert result.total == 1

    result = store.search("Patient", query_string="death-date=lt1990-01-01T00:00:00")
    assert_empty_bundle(result)

    # ne
    result = store.search("Patient", query_string="death-date=ne1990-01-01T00:00:00")
    assert result.total == 1

    result = store.search("Patient", query_string="death-date=ne2098-01-01T12:10:30")
    assert_empty_bundle(result)


@pytest.mark.resources("appointment-example.json")
def test_searchparam_type_date_instant(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "instant"
    The date format is the standard XML format, though other formats may be supported
    """
    result = store.search("Appointment", query_string="date=2013-12-10T09:00:00Z")
    assert result.total == 1
    result = store.search("Appointment", query_string="date=eq2013-12-10T09:00:00Z")
    assert result.total == 1

    result = store.search("Appointment", query_string="date=2013-11-10T09:00:00Z")
    assert_empty_bundle(result)

    # gt
    result = store.search("Appointment", query_string="date=gt2013-10-10T09:00:00Z")
    assert result.total == 1

    result = store.search("Appointment", query_string="date=gt2013-12-10T09:00:00Z")
    assert_empty_bundle(result)
    result = store.search("Appointment", query_string="date=gt2013-12-10T09:00:20Z")
    assert_empty_bundle(result)

    # lt
    result = store.search("Appointment", query_string="date=lt2015-12-10T09:00:00Z")
    assert result.total == 1

    result = store.search("Appointment", query_string="date=lt2013-12-10T09:00:00Z")
    assert_empty_bundle(result)
    result = store.search("Appointment", query_string="date=lt2011-12-10T09:00:20Z")
    assert_empty_bundle(result)

    # ne
    result = store.search("Appointment", query_string="date=ne2015-12-10T09:00:00Z")
    assert result.total == 1

    result = store.search("Appointment", query_string="date=ne2013-12-10T09:00:00Z")
    assert_empty_bundle(result)


@pytest.mark.resources("encounter-example-home.json")
def test_searchparam_type_date_period_eq(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    """
    result = store.search("Encounter", query_string="date=2015-01-17")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=2015-01-17T15:15:00")
    assert_empty_bundle(result)


@pytest.mark.resources("encounter-example-home.json", "encounter-example-long.json")
def test_searchparam_type_date_period_ne(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    prefix ne: the range of the search value does not fully contain the range of the target value
    """
    result = store.search("Encounter", query_string="date=ne2015-01-17")
    assert result.total == 1
    assert result.entry[0].resource.id == "long"

    result = store.search("Encounter", query_string="date=ne2015-01-17T16:15:00")
    assert result.total == 2


@pytest.mark.resources("encounter-example-home.json")
def test_searchparam_type_date_period_gt(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    prefix gt: the range above the search value intersects (i.e. overlaps)
        with the range of the target value
    """
    # TODO handle timezone "date=gt2015-01-17T16:15:00+10:00"
    result = store.search("Encounter", query_string="date=gt2015-01-17T16:15:00")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=gt2015-01-17")
    assert_empty_bundle(result)


@pytest.mark.resources("encounter-example-home.json")
def test_searchparam_type_date_period_lt(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    prefix lt: the range below the search value intersects (i.e. overlaps)
        with the range of the target value
    """
    result = store.search("Encounter", query_string="date=lt2015-01-17T16:15:00")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=lt2015-01-17")
    assert_empty_bundle(result)


@pytest.mark.resources("encounter-example-home.json")
def test_searchparam_type_date_period_ge(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    prefix ge: the range above the search value intersects (i.e. overlaps)
        with the range of the target value, or the range of the search
        value fully contains the range of the target value
    """
    result = store.search("Encounter", query_string="date=ge2015-01-17T16:15:00")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=ge2015-01-17")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=ge2015-01-17T16:40:00")
    assert_empty_bundle(result)


@pytest.mark.resources("encounter-example-home.json")
def test_searchparam_type_date_period_le(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    prefix le: the range below the search value intersects (i.e. overlaps)
        with the range of the target value or the range of the search
        value fully contains the range of the target value
    """
    result = store.search("Encounter", query_string="date=le2015-01-17T16:15:00")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=le2015-01-17")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=le2015-01-17T15:15:00")
    assert_empty_bundle(result)


@pytest.mark.resources("encounter-example-home.json")
def test_searchparam_type_date_period_sa(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    prefix sa: the range of the search value does not overlap with the range of the target value,
        and the range above the search value contains the range of the target value
    """
    result = store.search("Encounter", query_string="date=sa2015-01-17T15:15:00")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=sa2015-01-17T16:00:00")
    assert_empty_bundle(result)


@pytest.mark.resources("encounter-example-home.json")
def test_searchparam_type_date_period_eb(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    prefix eb: the range of the search value does overlap not with the range of the target value,
        and the range below the search value contains the range of the target value
    """
    result = store.search("Encounter", query_string="date=eb2015-01-17T16:45:00")
    assert result.total == 1

    result = store.search("Encounter", query_string="date=eb2015-01-17T16:30:00")
    assert_empty_bundle(result)


@pytest.mark.resources("encounter-example-home.json", "encounter-example-long.json")
def test_searchparam_type_date_period_ap(store: FHIRStore, index_resources):
    """Handle date search parameters on FHIR data type "period"
    The date format is the standard XML format, though other formats may be supported
    prefix ap: the range of the search value overlaps with the range of the target value
    """
    result = store.search("Encounter", query_string="date=ap2015-01-17")
    assert result.total == 2

    result = store.search("Encounter", query_string="date=ap2015-01-18T16:15:00")
    assert result.total == 1
    assert result.entry[0].resource.id == "long"

    result = store.search("Encounter", query_string="date=ap2015-01-21")
    assert_empty_bundle(result)


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_type_reference_literal(store: FHIRStore, index_resources):
    """Handle reference search parameters (Reference or canonical)
    """

    # [parameter]=[id] the logical [id] of a resource using
    # a local reference (i.e. a relative reference)
    result = store.search("Observation", query_string="subject=pat1")
    assert result.total == 1

    result = store.search("Observation", query_string="subject=patUnknown")
    assert_empty_bundle(result)

    # [parameter]=[type]/[id] the logical [id] of a resourceof a specified type using a local
    # reference (i.e. a relative reference), for when the reference can point to different
    # types of resources (e.g. Observation.subject)
    result = store.search("Observation", query_string="subject=Patient/pat1")
    assert result.total == 1

    result = store.search("Observation", query_string="subject=Location/pat1")
    assert_empty_bundle(result)

    result = store.search("Observation", query_string="subject=Patient/patUnknown")
    assert_empty_bundle(result)

    # [parameter]=[url] where the [url] is an absolute URL - a reference to a resource by its
    # absolute location, or by it's canonical URL
    result = store.search(
        "Observation", query_string="encounter=https://staging.arkhn.om/api/Encounter/f201"
    )
    assert result.total == 1

    result = store.search(
        "Observation", query_string="encounter=https://staging.arkhn.om/api/Encounter/unknown"
    )
    assert_empty_bundle(result)


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_type_reference_identifier(store: FHIRStore, index_resources):
    """Handle reference search parameters using the logical identifier.
    The modifier :identifier allows for searching by the
    identifier rather than the literal reference
    """

    # [param-ref]:identifier=[value]
    result = store.search("Observation", query_string="subject:identifier=654321")
    assert result.total == 1

    result = store.search("Observation", query_string="subject:identifier=123456789")
    assert_empty_bundle(result)

    # [param-ref]:identifier=[system]|[value]: the value of [code] matches an
    # reference.identifier.value, and the value of [system] matches the system
    # property of the Identifier
    result = store.search(
        "Observation", query_string="subject:identifier=urn:oid:0.1.2.3.4.5.6.7|654321"
    )
    assert result.total == 1

    result = store.search(
        "Observation", query_string="subject:identifier=urn:oid:0.1.2.3.4.5.6.7|123456789"
    )
    assert_empty_bundle(result)

    # TODO: not working yet, when omitting the system, the search is applied only on the value
    # (it should also filter by empty system)
    # [param-ref]:identifier=|[code]: the value of [code] matches a reference.identifier.value,
    # and the Identifier has no system property
    result = store.search("Observation", query_string="subject:identifier=|654321")
    assert result.total == 1

    result = store.search("Observation", query_string="subject:identifier=|123456789")
    assert_empty_bundle(result)

    # [param-ref]:identifier=[system]|: any element where the value of [system] matches the
    # system property of the Identifier
    result = store.search("Observation", query_string="subject:identifier=urn:oid:0.1.2.3.4.5.6.7|")
    assert result.total == 1

    result = store.search("Observation", query_string="subject:identifier=other|")
    assert_empty_bundle(result)


# TODO: the "$" syntax is not yet handled in fhirpath
@pytest.mark.skip()
def test_searchparam_type_composite(store: FHIRStore):
    """Handle composite search parameter that combines a search on two values together.
    eg: Observation?component-code-value-quantity=http://loinc.org|8480-6$lt60
    """
    pass


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_type_quantity(store: FHIRStore, index_resources):
    """Handle quantity search parameters (precision is 0.000001)
    """
    # all observations with a value of exactly 66.899999 (irrespective of the unit)
    result = store.search("Observation", query_string="value-quantity=66.899999")
    assert result.total == 1

    result = store.search("Observation", query_string="value-quantity=9")
    assert_empty_bundle(result)

    # all observations with a value greater/lower than 50 (irrespective of the unit)
    result = store.search("Observation", query_string="value-quantity=gt50")
    assert result.total == 1

    result = store.search("Observation", query_string="value-quantity=lt50")
    assert_empty_bundle(result)

    # Search for all the observations with a value of 66.899999(+/-0.05)
    # where "[in_i]" is understood as a UCUM unit (system/code)
    result = store.search(
        "Observation", query_string="value-quantity=66.899999|http://unitsofmeasure.org|[in_i]",
    )
    assert result.total == 1

    result = store.search(
        "Observation", query_string="value-quantity=66.899999|http://unitsofmeasure.org|[other]",
    )
    assert_empty_bundle(result)

    result = store.search(
        "Observation", query_string="value-quantity=66.899999|http://other.org|[in_i]",
    )
    assert_empty_bundle(result)

    # Search for all the observations with a value of 5.4(+/-0.05) mg where the
    # unit - either the code (code) or the stated human unit (unit) are "in"
    result = store.search("Observation", query_string="value-quantity=66.899999||in")
    assert result.total == 1

    result = store.search("Observation", query_string="value-quantity=66.899999||other")
    assert_empty_bundle(result)


@pytest.mark.resources("codesystem-example.json")
def test_searchparam_type_uri(store: FHIRStore, index_resources):
    """Handle uri search parameters
    """
    result = store.search(
        "CodeSystem", query_string="system=http://hl7.org/fhir/CodeSystem/example"
    )
    assert result.total == 1

    result = store.search("CodeSystem", query_string="system=http://hl7.org/fhir/CodeSystem/other")
    assert_empty_bundle(result)


@pytest.mark.resources("codesystem-example.json")
def test_searchparam_type_uri_below(store: FHIRStore, index_resources):
    """Handle uri search parameters
    """
    result = store.search("CodeSystem", query_string="system:below=http://hl7.org/fhir/")
    assert result.total == 1

    result = store.search(
        "CodeSystem", query_string="system:below=http://hl7.org/fhir/CodeSystem/Arkhn"
    )
    assert_empty_bundle(result)

    result = store.search(
        "CodeSystem", query_string="system:below=http://hl7.org/fhir/CodeSystem/example"
    )
    assert result.total == 1


# TODO: this seems to be implemented by fhirpath but not the right way.
@pytest.mark.skip()
@pytest.mark.resources("codesystem-example.json")
def test_searchparam_type_uri_above(store: FHIRStore, index_resources):
    """Handle uri search parameters
    """
    result = store.search(
        "CodeSystem", query_string="system:above=http://hl7.org/fhir/CodeSystem/example/24"
    )
    assert result.total == 1

    result = store.search("CodeSystem", query_string="system:above=http://hl7.org/fhir/CodeSystem/")
    assert_empty_bundle(result)


# SEARCH PARAMETERS MODIFIERS
# Parameters are defined per resource. Parameter names may specify a modifier as a suffix.
# The modifiers are separated from the parameter name by a colon.


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_modifier_missing(store: FHIRStore, index_resources):
    """Handle :missing modifier
    For all parameters.
    Searching for gender:missing=true will return all the resources that don't
    have a value for the gender parameter
    """
    result = store.search("Patient", query_string="general-practitioner:missing=true")
    assert result.total == 1

    result = store.search("Patient", query_string="general-practitioner:missing=false")
    assert_empty_bundle(result)

    result = store.search("Patient", query_string="gender:missing=false")
    assert result.total == 1

    result = store.search("Patient", query_string="gender:missing=true")
    assert_empty_bundle(result)


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_modifier_exact(store: FHIRStore, index_resources):
    """Handle :exact modifier
    For string: :exact returns results that match the entire supplied parameter,
    including casing and combining characters
    """
    result = store.search("Patient", query_string="family:exact=Donald")
    assert result.total == 1

    result = store.search("Patient", query_string="family:exact=Other")
    assert_empty_bundle(result)

    result = store.search("Patient", query_string="family:exact=donald")
    assert_empty_bundle(result)

    result = store.search("Patient", query_string="family:exact=Donàld")
    assert_empty_bundle(result)


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_modifier_contains(store: FHIRStore, index_resources):
    """Handle :contains modifier
    For string: case insensitive and combining character-insensitive,
    search text matched anywhere in the string
    """
    result = store.search("Patient", query_string="family:contains=Don")
    assert result.total == 1

    result = store.search("Patient", query_string="family:contains=don")
    assert result.total == 1

    result = store.search("Patient", query_string="family:contains=D")
    assert result.total == 1

    result = store.search("Patient", query_string="family:contains=ld")
    assert result.total == 1

    result = store.search("Patient", query_string="family:contains=Donald")
    assert result.total == 1

    result = store.search("Patient", query_string="family:contains=dOnàld")
    assert result.total == 1

    result = store.search("Patient", query_string="family:contains=Dan")
    assert_empty_bundle(result)

    result = store.search("Patient", query_string="family:contains=dnoald")
    assert_empty_bundle(result)


# TODO: we will need this at some point
@pytest.mark.skip()
def test_searchparam_modifier_text(store: FHIRStore):
    """Handle :text modifier
    For token: :text (the match does a partial searches on the text portion of a CodeableConcept
    or the display portion of a Coding), instead of the default search which uses codes.
    """
    pass


# TODO: we can do this afer implementing chaining / reverse chaining
@pytest.mark.skip()
def test_searchparam_modifier_type(store: FHIRStore):
    """Handle :[type] modifier
    For reference: :[type] where [type] is the name of a type of resource, :identifier
    """
    pass


@pytest.mark.resources("codesystem-example.json")
def test_searchparam_modifier_below(store: FHIRStore, index_resources):
    """Handle :below modifier
    For uri: :below indicate that instead of an exact match, either the search term
    left-matches the value
    """
    result = store.search("CodeSystem", query_string="system:below=http://hl7.org/fhir/")
    assert result.total == 1

    result = store.search(
        "CodeSystem", query_string="system:below=http://hl7.org/fhir/CodeSystem/Arkhn"
    )
    assert_empty_bundle(result)

    result = store.search(
        "CodeSystem", query_string="system:below=http://hl7.org/fhir/CodeSystem/example"
    )
    assert result.total == 1


# TODO: this seems to be implemented by fhirpath but not the right way.
@pytest.mark.skip()
@pytest.mark.resources("codesystem-example.json")
def test_searchparam_modifier_above(store: FHIRStore, index_resources):
    """Handle :above modifier
    For uri: :above indicate that instead of an exact match, either the search term
    right-matches the value
    """
    result = store.search(
        "CodeSystem", query_string="system:above=http://hl7.org/fhir/CodeSystem/example/24"
    )
    assert result.total == 1

    result = store.search("CodeSystem", query_string="system:above=http://hl7.org/fhir/CodeSystem/")
    assert_empty_bundle(result)


# SEARCH PARAMETERS PREFIXES
# For the ordered parameter types of number, date, and quantity, a prefix to the parameter value
# may be used to control the nature of the matching.


@pytest.mark.resources(
    "patient-pat1.json", "sequence-graphic-example-1.json", "observation-bodyheight-example.json"
)
def test_searchparam_prefix_eq(store: FHIRStore, index_resources):
    """Handle :eq prefix
    the value for the parameter in the resource is equal to the provided value
    """
    # number
    result = store.search("MolecularSequence", query_string="variant-start=eq128273725")
    assert len(result.entry) == 1

    result = store.search("MolecularSequence", query_string="variant-start=eq128275")
    assert_empty_bundle(result)

    # date
    result = store.search("Patient", query_string="birthdate=eq1995-12-25")
    assert len(result.entry) == 1

    result = store.search("Patient", query_string="birthdate=eq1995-12-24")
    assert_empty_bundle(result)

    # quantity
    result = store.search("Observation", query_string="value-quantity=eq66.899999999999991")
    assert len(result.entry) == 1

    result = store.search("Observation", query_string="value-quantity=eq67")
    assert_empty_bundle(result)


@pytest.mark.resources(
    "patient-pat1.json", "sequence-graphic-example-1.json", "observation-bodyheight-example.json"
)
def test_searchparam_prefix_ne(store: FHIRStore, index_resources):
    """Handle :ne prefix
    the value for the parameter in the resource is not equal to the provided value
    """
    # number
    result = store.search("MolecularSequence", query_string="variant-start=ne128275")
    assert len(result.entry) == 1

    result = store.search("MolecularSequence", query_string="variant-start=ne128273725")
    assert_empty_bundle(result)

    # date
    result = store.search("Patient", query_string="birthdate=ne1995-12-24")
    assert len(result.entry) == 1

    result = store.search("Patient", query_string="birthdate=ne1995-12-25")
    assert_empty_bundle(result)

    # quantity
    result = store.search("Observation", query_string="value-quantity=ne67")
    assert len(result.entry) == 1

    result = store.search("Observation", query_string="value-quantity=ne66.899999999999991")
    assert_empty_bundle(result)


@pytest.mark.resources(
    "patient-pat1.json", "sequence-graphic-example-1.json", "observation-bodyheight-example.json"
)
def test_searchparam_prefix_gt(store: FHIRStore, index_resources):
    """Handle :gt prefix
    the value for the parameter in the resource is greater than the provided value
    """
    # number
    result = store.search("MolecularSequence", query_string="variant-start=gt128271")
    assert len(result.entry) == 1

    result = store.search("MolecularSequence", query_string="variant-start=gt128273725")
    assert_empty_bundle(result)

    result = store.search("MolecularSequence", query_string="variant-start=gt128273729")
    assert_empty_bundle(result)

    # date
    result = store.search("Patient", query_string="birthdate=gt1995-12-24")
    assert len(result.entry) == 1

    result = store.search("Patient", query_string="birthdate=gt1995-12-25")
    assert_empty_bundle(result)

    result = store.search("Patient", query_string="birthdate=gt1995-12-28")
    assert_empty_bundle(result)

    # quantity
    result = store.search("Observation", query_string="value-quantity=gt66")
    assert len(result.entry) == 1

    result = store.search("Observation", query_string="value-quantity=gt66.899999999999991")
    assert_empty_bundle(result)

    result = store.search("Observation", query_string="value-quantity=gt67")
    assert_empty_bundle(result)


@pytest.mark.resources(
    "patient-pat1.json", "sequence-graphic-example-1.json", "observation-bodyheight-example.json"
)
def test_searchparam_prefix_lt(store: FHIRStore, index_resources):
    """Handle :lt prefix
    the value for the parameter in the resource is less than the provided value
    """
    # number
    result = store.search("MolecularSequence", query_string="variant-start=lt128273729")
    assert len(result.entry) == 1

    result = store.search("MolecularSequence", query_string="variant-start=lt128273725")
    assert_empty_bundle(result)

    result = store.search("MolecularSequence", query_string="variant-start=lt128273721")
    assert_empty_bundle(result)

    # date
    result = store.search("Patient", query_string="birthdate=lt1995-12-28")
    assert len(result.entry) == 1

    result = store.search("Patient", query_string="birthdate=lt1995-12-25")
    assert_empty_bundle(result)

    result = store.search("Patient", query_string="birthdate=lt1995-12-24")
    assert_empty_bundle(result)

    # quantity
    result = store.search("Observation", query_string="value-quantity=lt67")
    assert len(result.entry) == 1

    result = store.search("Observation", query_string="value-quantity=lt66.899999999999991")
    assert_empty_bundle(result)

    result = store.search("Observation", query_string="value-quantity=lt66")
    assert_empty_bundle(result)


@pytest.mark.resources(
    "patient-pat1.json", "sequence-graphic-example-1.json", "observation-bodyheight-example.json"
)
def test_searchparam_prefix_ge(store: FHIRStore, index_resources):
    """Handle :ge prefix
    the value for the parameter in the resource is greater or equal to the provided value
    """
    # number
    result = store.search("MolecularSequence", query_string="variant-start=ge128271")
    assert len(result.entry) == 1
    result = store.search("MolecularSequence", query_string="variant-start=ge128273725")
    assert len(result.entry) == 1

    result = store.search("MolecularSequence", query_string="variant-start=ge128273729")
    assert_empty_bundle(result)

    # date
    result = store.search("Patient", query_string="birthdate=ge1995-12-24")
    assert len(result.entry) == 1
    result = store.search("Patient", query_string="birthdate=ge1995-12-25")
    assert len(result.entry) == 1

    result = store.search("Patient", query_string="birthdate=ge1995-12-28")
    assert_empty_bundle(result)

    # quantity
    result = store.search("Observation", query_string="value-quantity=ge66")
    assert len(result.entry) == 1
    result = store.search("Observation", query_string="value-quantity=ge66.899999999999991")
    assert len(result.entry) == 1

    result = store.search("Observation", query_string="value-quantity=gt67")
    assert_empty_bundle(result)


@pytest.mark.resources(
    "patient-pat1.json", "sequence-graphic-example-1.json", "observation-bodyheight-example.json"
)
def test_searchparam_prefix_le(store: FHIRStore, index_resources):
    """Handle :le prefix
    the value for the parameter in the resource is less or equal to the provided value
    """
    # number
    result = store.search("MolecularSequence", query_string="variant-start=le128273729")
    assert len(result.entry) == 1
    result = store.search("MolecularSequence", query_string="variant-start=le128273725")
    assert len(result.entry) == 1

    result = store.search("MolecularSequence", query_string="variant-start=le128273721")
    assert_empty_bundle(result)

    # date
    result = store.search("Patient", query_string="birthdate=le1995-12-28")
    assert len(result.entry) == 1
    result = store.search("Patient", query_string="birthdate=le1995-12-25")
    assert len(result.entry) == 1

    result = store.search("Patient", query_string="birthdate=le1995-12-24")
    assert_empty_bundle(result)

    # quantity
    result = store.search("Observation", query_string="value-quantity=le67")
    assert len(result.entry) == 1
    result = store.search("Observation", query_string="value-quantity=le66.899999999999991")
    assert len(result.entry) == 1

    result = store.search("Observation", query_string="value-quantity=le66")
    assert_empty_bundle(result)


@pytest.mark.skip()
def test_searchparam_prefix_sa(store: FHIRStore):
    """Handle :sa prefix
    the value for the parameter in the resource starts after the provided value
    """
    pass


@pytest.mark.skip()
def test_searchparam_prefix_eb(store: FHIRStore):
    """Handle :eb prefix
    the value for the parameter in the resource ends before the provided value
    """
    pass


@pytest.mark.skip()
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


@pytest.mark.skip()
@pytest.mark.resources("patient-pat1.json", "observation-bodyheight-example.json")
def test_searchparam_chained_simple(store: FHIRStore, index_resources):
    """Handle a single chained parameter
    DiagnosticReport?subject.name=peter
    """
    result = store.search("Observation", query_string="subject.name=peter")
    assert len(result.entry) == 1


@pytest.mark.skip()
def test_searchparam_chained_multiple(store: FHIRStore):
    """Handle multiple chained parameters
    Patient?general-practitioner.name=Joe&general-practitioner.address-state=MN
    """
    pass


@pytest.mark.skip()
def test_searchparam_chained_typed(store: FHIRStore):
    """Handle a typed chained parameter
    Patient?general-practitioner.name=Joe&general-practitioner.address-state=MN
    """
    pass


# REVERSE CHAINING
# The _has parameter provides limited support for reverse chaining - that is, selecting resources
# based on the properties of resources that refer to them (instead of chaining, above, where
# resources can be selected based on the properties of resources that they refer to)


@pytest.mark.resources(
    "patient-pat1.json", "patient-pat2.json", "observation-bodyheight-example.json"
)
def test_searchparam_reverse_chaining(store: FHIRStore, index_resources):
    """Handle a single chained parameter
    Patient?_has:Observation:patient:code=8302-2
    """
    observation_result = store.search("Observation", query_string="code=8302-2")
    assert len(observation_result.entry) == 1
    obs = observation_result.entry[0].resource

    result = store.search("Patient", query_string="_has:Observation:patient:code=8302-2")
    assert len(result.entry) == 1
    assert f"Patient/{result.entry[0].resource.id}" == obs.subject.reference

    result = store.search("Patient", query_string="_has:Observation:patient:code=XXX")
    assert len(result.entry) == 0


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "location-patient-home.json",
    "observation-bodyheight-example.json",
    "observation-vp-oyster.json",
)
def test_searchparam_reverse_chaining_where_constraint(store: FHIRStore, index_resources):
    """Handle search params expressions with fhirpath .where(resolve() is <resource_type>)
    Patient?_has:Observation:patient:code=8302-2
    """

    # the following observation's subject is a Patient, therefore it should have one result
    result = store.search("Patient", query_string="_has:Observation:patient:code=8302-2")
    assert len(result.entry) == 1

    # the following observation's subject is a Location, therefore result should be emtpy
    result = store.search("Patient", query_string="_has:Observation:patient:code=41857-4")
    assert_empty_bundle(result)


@pytest.mark.skip()
def test_searchparam_reverse_chaining_chained(store: FHIRStore):
    """Handle a single chained parameter
    Patient?_has:Observation:patient:_has:AuditEvent:entity:user=MyUserId
    """
    pass


# SEARCHING BY LIST
# The _list parameter allows for the retrieval of resources that are referenced by a List resource.


@pytest.mark.skip()
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


@pytest.mark.resources("patient-pat1.json", "practitioner-example.json")
def test_searchparam_no_type_provided(store: FHIRStore, index_resources):
    """Handle searching on multiple resource types
    GET [base]?params...
    """
    result = store.search(query_string="_tag=testme")
    assert len(result.entry) == 2

    result = store.search(query_string="_tag=notfound")
    assert_empty_bundle(result)


def test_searchparam_no_type_provided_bad_param(store: FHIRStore):
    """Handle searching on multiple resource types
    GET [base]?params...
    """
    result = store.search(query_string="address-city=Verzon")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "No search definition is available for search parameter "
        "``address-city`` on Resource ``Resource``."
    )


@pytest.mark.resources("patient-pat1.json", "practitioner-example.json")
def test_searchparam_mutltiple_types(store: FHIRStore, index_resources):
    """Handle searching on multiple resource types while restraining the returned resources
    GET [base]?_type=Observation,Condition&other params...
    """
    result = store.search(query_string="_type=Patient,Practitioner&address-city=Verson")
    assert len(result.entry) == 1

    result = store.search(query_string="_type=Patient,Practitioner&address-city=PleasantVille")
    assert len(result.entry) == 1

    result = store.search(query_string="_type=Patient,Practitioner&address-city=NotFound")
    assert_empty_bundle(result)


def test_searchparam_mutltiple_types_bad_param(store: FHIRStore):
    """Should raise an error if the used search parameters are not
    shared across the entire set of specified resources
    """
    result = store.search(query_string="_type=Patient,Observation&address-city=Verson")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "No search definition is available for search parameter "
        "``address-city`` on Resource ``Observation``."
    )


# SORTING RESULTS
# The client can indicate which order to return the results by using the parameter _sort, which
# can contain a comma-separated list of sort rules in priority order.


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json", "patient-b966.json")
def test_searchparam_sort(store: FHIRStore, index_resources):
    """Handle searching on multiple resource types while restraining the returned resources
    Observation?_sort=status,-date,category
    """
    # ascending order
    result = store.search("Patient", query_string="_sort=family")
    result_names = [
        sorted(name.family for name in entry.resource.name if name.family is not None)[0]
        for entry in result.entry
    ]
    assert result_names == sorted(result_names)

    # descending order
    result = store.search("Patient", query_string="_sort=-address-city")
    result_cities = [
        sorted(address.city for address in entry.resource.address if address.city is not None)[-1]
        for entry in result.entry
    ]
    assert result_cities == sorted(result_cities, reverse=True)

    # several fields
    result = store.search("Patient", query_string="_sort=address-city,-family")
    result_ids = [entry.resource.id for entry in result.entry]
    assert result_ids == ["b966", "pat2", "pat1"]


# PAGE COUNT
# The parameter _count is defined as an instruction to the server regarding how many resources
# should be returned in a single page.
# If _count has the value 0, this shall be treated the same as _summary=count.


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json", "patient-b966.json")
def test_searchparam_count(store: FHIRStore, index_resources):
    """Handle _count
    Observation?_count=10
    """
    result = store.search("Patient", query_string="_count=10")
    assert len(result.entry) == 3

    result = store.search("Patient", query_string="_count=2")
    assert len(result.entry) == 2


@pytest.mark.resources("patient-pat1.json", "patient-pat2.json", "patient-b966.json")
def test_searchparam_count_zero(store: FHIRStore, index_resources):
    """Handle _count
    Observation?_count=0
    """
    result = store.search("Patient", query_string="_count=0")
    assert result.entry == []
    assert result.total == 3


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


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "observation-bodyheight-example.json",
    "observation-glucose.json",
)
def test_searchparam_include(store: FHIRStore, index_resources):
    """Handle _include while specifying the target type"""
    result = store.search(
        "Observation", query_string="_id=body-height&_include=Observation:subject:Patient",
    )

    # both the observation and the patient should have been returned
    assert len(result.entry) == 2

    assert result.entry[0].resource.resource_type == "Observation"
    assert result.entry[0].search.mode == "match"

    assert result.entry[1].resource.resource_type == "Patient"
    assert result.entry[1].search.mode == "include"


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "observation-bodyheight-example.json",
    "observation-glucose.json",
    "observation-vp-oyster.json",
    "location-patient-home.json",
)
def test_searchparam_include_untyped(store: FHIRStore, index_resources):
    """Handle _include while specifying the target type"""
    result = store.search("Observation", query_string="_include=Observation:subject",)

    assert len(result.entry) == 6

    assert result.entry[0].resource.resource_type == "Observation"
    assert result.entry[0].search.mode == "match"
    assert result.entry[1].resource.resource_type == "Observation"
    assert result.entry[1].search.mode == "match"
    assert result.entry[2].resource.resource_type == "Observation"
    assert result.entry[2].search.mode == "match"

    assert result.entry[3].resource.resource_type == "Patient"
    assert result.entry[3].search.mode == "include"

    assert result.entry[4].resource.resource_type == "Patient"
    assert result.entry[4].search.mode == "include"

    assert result.entry[5].resource.resource_type == "Location"
    assert result.entry[5].search.mode == "include"


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "observation-bodyheight-example.json",
    "observation-glucose.json",
    "observation-vp-oyster.json",
    "location-patient-home.json",
)
def test_searchparam_include_many_types(store: FHIRStore, index_resources):
    """Handle _include and specify mutliple target types"""
    result = store.search(
        "Observation",
        query_string="_include=Observation:subject:Patient&_include=Observation:subject:Location",
    )

    # both the observation and the patient should have been returned
    assert len(result.entry) == 6

    assert result.entry[0].resource.resource_type == "Observation"
    assert result.entry[0].search.mode == "match"
    assert result.entry[1].resource.resource_type == "Observation"
    assert result.entry[1].search.mode == "match"
    assert result.entry[2].resource.resource_type == "Observation"
    assert result.entry[2].search.mode == "match"

    assert result.entry[3].resource.resource_type == "Patient"
    assert result.entry[3].search.mode == "include"

    assert result.entry[4].resource.resource_type == "Patient"
    assert result.entry[4].search.mode == "include"

    assert result.entry[5].resource.resource_type == "Location"
    assert result.entry[5].search.mode == "include"


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "observation-bodyheight-example.json",
    "observation-glucose.json",
    "observation-vp-oyster.json",
    "location-patient-home.json",
)
def test_searchparam_include_where_constraint(store: FHIRStore, index_resources):
    """Handle search params expressions with fhirpath .where(resolve() is <resource_type>)
    Observation?_include=Observation:patient
    """

    result = store.search("Observation", query_string="_include=Observation:patient",)

    # only the patients should have been returned
    assert len(result.entry) == 5

    assert result.entry[0].resource.resource_type == "Observation"
    assert result.entry[0].search.mode == "match"
    assert result.entry[1].resource.resource_type == "Observation"
    assert result.entry[1].search.mode == "match"
    assert result.entry[2].resource.resource_type == "Observation"
    assert result.entry[2].search.mode == "match"

    assert result.entry[3].resource.resource_type == "Patient"
    assert result.entry[3].search.mode == "include"

    assert result.entry[4].resource.resource_type == "Patient"
    assert result.entry[4].search.mode == "include"


@pytest.mark.resources(
    "observation-bodyheight-example.json",
    "patient-pat1.json",
    "location-patient-home.json",
    "practitioner-example.json",
)
def test_searchparam_include_many_references(store: FHIRStore, index_resources):
    """Handle _include multiple references"""
    result = store.search(
        "Observation",
        query_string="_id=body-height&_include=Observation:subject&_include=Observation:performer",
    )

    # both the observation and the patient should have been returned
    assert len(result.entry) == 3

    assert result.entry[0].resource.resource_type == "Observation"
    assert result.entry[0].search.mode == "match"

    assert result.entry[1].resource.resource_type == "Patient"
    assert result.entry[1].search.mode == "include"

    assert result.entry[2].resource.resource_type == "Practitioner"
    assert result.entry[2].search.mode == "include"


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_include_bad_searchparam_syntax(store: FHIRStore, index_resources):
    result = store.search("Observation", query_string="_include=subject")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics
        == "bad _include param 'subject', should be Resource:search_param[:target_type]"
    )

    result = store.search("Observation", query_string="_include=Observation:subject:too:long")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "bad _include param 'Observation:subject:too:long', "
        "should be Resource:search_param[:target_type]"
    )


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_include_bad_searchparam(store: FHIRStore, index_resources):
    result = store.search("Observation", query_string="_include=Observation:category")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics
        == "search parameter Observation.category must be of type 'reference', got token"
    )


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_include_unknown_searchparam(store: FHIRStore, index_resources):
    result = store.search("Observation", query_string="_include=Observation:unknown")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "No search definition is available for search "
        "parameter ``unknown`` on Resource ``Observation``."
    )


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_include_bad_target(store: FHIRStore, index_resources):
    result = store.search(
        "Observation", query_string="_include=Observation:subject:DocumentReference"
    )
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics
        == "the search param Observation.subject may refer to Group, Device, Patient, Location"
        ", not to DocumentReference"
    )


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "observation-bodyheight-example.json",
    "observation-glucose.json",
)
def test_searchparam_revinclude(store: FHIRStore, index_resources):
    """Handle _include
    Patient?_revinclude=Observation:subject
    """

    result = store.search("Patient", query_string="_id=pat1&_revinclude=Observation:subject",)

    # both the observation and the patient should have been returned
    assert len(result.entry) == 2

    assert result.entry[0].resource.resource_type == "Patient"
    assert result.entry[0].search.mode == "match"

    assert result.entry[1].resource.resource_type == "Observation"
    assert result.entry[1].search.mode == "include"


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "observation-bodyheight-example.json",
    "observation-glucose.json",
)
def test_searchparam_revinclude_typed(store: FHIRStore, index_resources):
    """Handle _revinclude when typing the reference search parameter
    """

    result = store.search(
        "Patient", query_string="_id=pat1&_revinclude=Observation:subject:Patient",
    )

    # both the observation and the patient should have been returned
    assert len(result.entry) == 2

    assert result.entry[0].resource.resource_type == "Patient"
    assert result.entry[0].search.mode == "match"

    assert result.entry[1].resource.resource_type == "Observation"
    assert result.entry[1].search.mode == "include"


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "observation-bodyheight-example.json",
    "observation-glucose.json",
)
def test_searchparam_revinclude_with_has(store: FHIRStore, index_resources):
    """Handle _revinclude coupled with _has
    """

    result = store.search(
        "Patient",
        query_string="_has:Observation:subject:code=8302-2&_revinclude=Observation:subject",
    )

    # both the observation and the patient should have been returned
    assert len(result.entry) == 2

    assert result.entry[0].resource.resource_type == "Patient"
    assert result.entry[0].search.mode == "match"

    assert result.entry[1].resource.resource_type == "Observation"
    assert result.entry[1].search.mode == "include"


@pytest.mark.resources(
    "patient-pat1.json",
    "patient-pat2.json",
    "observation-bodyheight-example.json",
    "observation-glucose.json",
    "medicationrequest-example.json",
)
def test_searchparam_revinclude_double(store: FHIRStore, index_resources):
    """Handle _include
    Patient?_revinclude=Provenance:target
    """

    result = store.search(
        "Patient",
        query_string="_has:Observation:subject:code=8302-2"
        "&_revinclude=Observation:subject&_revinclude=MedicationRequest:subject",
    )

    # both the observation and the patient should have been returned
    assert len(result.entry) == 3

    assert result.entry[0].resource.resource_type == "Patient"
    assert result.entry[0].search.mode == "match"

    assert result.entry[1].resource.resource_type == "Observation"
    assert result.entry[1].search.mode == "include"

    assert result.entry[2].resource.resource_type == "MedicationRequest"
    assert result.entry[2].search.mode == "include"


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_revinclude_bad_searchparam_syntax(store: FHIRStore, index_resources):
    result = store.search("Patient", query_string="_revinclude=subject")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics
        == "bad _revinclude param 'subject', should be Resource:search_param[:target_type]"
    )

    result = store.search("Patient", query_string="_revinclude=Observation:subject:too:long")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "bad _revinclude param 'Observation:subject:too:long', "
        "should be Resource:search_param[:target_type]"
    )


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_revinclude_bad_searchparam(store: FHIRStore, index_resources):
    result = store.search("Patient", query_string="_revinclude=Observation:category")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics
        == "search parameter Observation.category must be of type 'reference', got token"
    )


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_revinclude_unknown_searchparam(store: FHIRStore, index_resources):
    result = store.search("Patient", query_string="_revinclude=Observation:unknown")
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "No search definition is available for search "
        "parameter ``unknown`` on Resource ``Observation``."
    )


@pytest.mark.resources("observation-bodyheight-example.json")
def test_searchparam_revinclude_bad_target(store: FHIRStore, index_resources):
    result = store.search(
        "Observation", query_string="_revinclude=Observation:subject:DocumentReference"
    )
    assert isinstance(result, OperationOutcome)
    assert len(result.issue) == 1
    assert (
        result.issue[0].diagnostics == "invalid reference Observation.subject "
        "(Group,Device,Patient,Location) in the current search context (Observation)"
    )


# SUMMARY


@pytest.mark.resources("patient-pat1.json")
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


@pytest.mark.resources("patient-pat1.json")
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


@pytest.mark.resources("patient-pat1.json")
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


@pytest.mark.resources("patient-pat1.json")
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


@pytest.mark.resources("patient-pat1.json")
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


@pytest.mark.resources("patient-pat1.json")
def test_searchparam_elements(store: FHIRStore, index_resources):
    """Handle _elements
    Patient?_elements=identifier,active,link
    """
    result = store.search("Patient", query_string="_elements=identifier,active,link")
    assert result.entry[0].resource.id is not None
    assert result.entry[0].resource.identifier is not None
    assert result.entry[0].resource.active is not None
    assert result.entry[0].resource.link is not None
    assert result.entry[0].resource.meta is None
    assert result.entry[0].resource.birthDate is None
    assert result.entry[0].resource.maritalStatus is None
