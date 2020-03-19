import pytest
import json
from pytest import raises

from time import sleep
from fhirstore import FHIRStore, NotFoundError
from fhirstore.search.search_methods import build_element_query, build_simple_query
from collections.abc import Mapping

# For now, this class assumes an already existing store exists
# (store.bootstrap was run)


@pytest.fixture(scope="module")
def insert_es(es_client):
    if not es_client.indices.exists("fhirstore.patient"):
        with open("test/fixtures/patient-example.json") as f:
            patient_1 = json.load(f)
            es_client.index(index="fhirstore.patient", body=patient_1)

        with open("test/fixtures/patient-example-2.json") as g:
            patient_2 = json.load(g)
            es_client.index(index="fhirstore.patient", body=patient_2)

        with open("test/fixtures/patient-example-with-extensions.json") as h:
            patient_3 = json.load(h)
            es_client.index(index="fhirstore.patient", body=patient_3)

    while es_client.count(index="fhirstore.patient")["count"] < 3:
        sleep(2 / 10000)
    return es_client


###
# search_methods
###


def test_simple_query_input_not_dict():
    """raises an error if the input is not 
    a dictionary"""
    with raises(AssertionError, match="parameters must be a dictionary"):
        build_simple_query("gender")


def test_simple_query_input_not_length_1():
    """raises an error if the input is not 
    of length 1"""
    with raises(AssertionError, match="sub-parameters must be of length 1"):
        build_simple_query({"gender": "female", "birthDate": "2001-02-04"})


def test_simple_query_output():
    """Validates that the output of build_simple_query
    is a dictionary
    """
    result = build_simple_query({"gender": "female"})
    assert isinstance(result, Mapping)


def test_element_query_output():
    """Validates that the output of build_element_query
    is a dictionary
    """
    result = build_element_query("gender", "female")
    assert isinstance(result, Mapping)


def test_simple_query_correct():
    """Validates that the ES query is correct"""
    result = build_simple_query({"birthDate": ["1974-12-25"]})
    assert result == {
        "simple_query_string": {"fields": ["birthDate"], "query": "(1974-12-25)*"}
    }


def test_simple_query_correct_or():
    """Validates that the ES query for "OR" is correct
    """
    result = build_simple_query({"multiple": {"name.family": ["Donald", "Chalmers"]}})
    assert result == {
        "bool": {
            "should": [
                {"match": {"name.family": "Donald"}},
                {"match": {"name.family": "Chalmers"}},
            ]
        }
    }


def test_simple_query_correct_and():
    """Validates that the ES query for "AND" is correct
    """
    result = build_simple_query({"name.family": ["Donald", "Chalmers"]})
    assert result == {
        "bool": {
            "must": [
                {
                    "simple_query_string": {
                        "fields": ["name.family"],
                        "query": "(Donald)*",
                    }
                },
                {
                    "simple_query_string": {
                        "fields": ["name.family"],
                        "query": "(Chalmers)*",
                    }
                },
            ]
        }
    }


def test_simple_query_contains():
    """Validates that the ES query is correct with modifier contains
    """
    result = build_simple_query({"name.family:contains": ["Dona"]})
    assert result == {
        "query_string": {"query": "*Dona*", "default_field": "name.family"}
    }


def test_simple_query_exact():
    """Validates that the ES query is correct with modifier contains
    """
    result = build_simple_query({"name.family:exact": ["Donald"]})
    assert result == {"query_string": {"query": "Donald", "fields": ["name.family"]}}


def test_simple_query_gt():
    """Validates that the ES query is correct with modifier 'gt'
    """
    result = build_simple_query({"birthDate": ["gt1974-12-25"]})
    assert result == {"range": {"birthDate": {"gt": "1974-12-25"}}}


def test_simple_query_ge():
    """Validates that the ES query is correct with modifier 'ge'
    """
    result = build_simple_query({"birthDate": ["ge1974-12-25"]})
    assert result == {"range": {"birthDate": {"gte": "1974-12-25"}}}


def test_simple_query_le():
    """Validates that the ES query is correct with modifier 'le'
    """
    result = build_simple_query({"birthDate": ["le1974-12-25"]})
    assert result == {"range": {"birthDate": {"lte": "1974-12-25"}}}


def test_simple_query_lt():
    """Validates that the ES query is correct with modifier 'lt'
    """
    result = build_simple_query({"birthDate": ["lt1974-12-25"]})
    assert result == {"range": {"birthDate": {"lt": "1974-12-25"}}}


def test_simple_query_eq():
    """Validates that the ES query is correct with modifier 'eq'
    """
    result = build_simple_query({"birthDate": ["eq1974-12-25"]})
    assert result == {"match": {"birthDate": "1974-12-25"}}


def test_composite_query_modifiers():
    """Validates that the ES query for "AND" with modifiers is correct
    """
    result = build_simple_query({"birthDate": ["lt20200101", "gt20100101"]})
    assert result == {
        "bool": {
            "must": [
                {"range": {"birthDate": {"lt": "20200101"}}},
                {"range": {"birthDate": {"gt": "20100101"}}},
            ]
        }
    }


###
# FHIRStore.search()
###
def test_search_bad_resource_type(store: FHIRStore):
    """search() raises error if resource type is unknown"""

    with raises(NotFoundError, match='unsupported FHIR resource: "unknown"'):
        store.search("unknown", {})


def test_search_bad_params(store: FHIRStore):
    """search() raises an error if params is not a dictionary"""

    with raises(AssertionError, match="parameters must be a dictionary"):
        store.search("Patient", "gender")


def test_search_output_type(store: FHIRStore, insert_es):
    """Check that the output type is correct
    """
    result = store.search("Patient", {})
    assert result["resource_type"] == "Bundle"


def test_search_no_parameters(store: FHIRStore):
    """Checks that all elements of the resource are returned
    """
    result = store.search("Patient", {})
    assert len(result["items"]) == 3


def test_search_one_param_simple(store: FHIRStore):
    """Checks simple one parameter search
    """
    result = store.search("Patient", {"identifier.value": ["654321"]})
    assert len(result["items"]) == 1
    assert all(
        element["identifier"][0]["value"] == "654321" for element in result["items"]
    )


def test_search_one_param_multiple(store: FHIRStore):
    """Checks that multiple elements of one parameter are queried
    """
    result = store.search(
        "Patient", {"multiple": {"name.family": ["Chalmers", "Levin"]}}
    )
    assert len(result["items"]) == 2
    assert any(
        element["name"][0]["family"] == ("Chalmers" or "Levin")
        for element in result["items"]
    )
    assert all(element["name"][0]["family"] != "Donald" for element in result["items"])


def test_search_one_param_modifier_num(store: FHIRStore):
    """Checks that numeric modifier work
    """
    number_modifier_matching = {
        "gt": ">",
        "ge": ">=",
        "lt": "<",
        "le": "<=",
        "eq": "==",
    }

    for mod in number_modifier_matching.keys():
        result = store.search("Patient", {"identifier.value": [f"{mod}654321"]})
        modif = number_modifier_matching[mod]
        for element in result["items"]:
            vals = element["identifier"][0]["value"]
            assert f"{vals}{modif}654321"


def test_search_one_param_modifier_str_contains(store: FHIRStore):
    """Checks that "contains" string modifier works
    """
    result = store.search(
        "Patient", {"managingOrganization.reference:contains": ["Organization"]}
    )
    assert len(result["items"]) == 3
    assert any(
        element["managingOrganization"]["reference"] == "Organization/1"
        for element in result["items"]
    )
    assert any(
        element["managingOrganization"]["reference"]
        != "Organization/2.16.840.1.113883.19.5"
        for element in result["items"]
    )


def test_search_one_param_modifier_str_exact(store: FHIRStore):
    """Checks that "exact" string modifier works
    """
    result = store.search("Patient", {"name.family:exact": ["Donald"]})
    assert len(result["items"]) == 1
    assert all(element["name"][0]["family"] == "Donald" for element in result["items"])
    assert all(
        element["name"][0]["family"] != ("Chalmers" or "Levin")
        for element in result["items"]
    )


def test_search_two_params_and(store: FHIRStore):
    """Checks two parameter "and" search
    """
    result = store.search(
        "Patient", {"identifier.value": ["12345"], "name.family": ["Chalmers"]}
    )
    assert all(
        element["identifier"][0]["value"] == "12345" for element in result["items"]
    )
    assert all(
        element["identifier"][0]["value"] != "654321" for element in result["items"]
    )
    assert all(
        element["name"][0]["family"] == "Chalmers" for element in result["items"]
    )
    assert all(
        element["name"][0]["family"] != "Donald" or "Levin"
        for element in result["items"]
    )
    assert len(result["items"]) == 1


def test_search_one_params_and(store: FHIRStore):
    """Checks one parameter "and" search
    """
    result = store.search("Patient", {"name.given": ["Peter", "James"]})
    assert len(result["items"]) == 1
    assert any(
        element["name"][0]["given"] == "Peter" or "James" for element in result["items"]
    )
    assert all(element["name"][0]["given"] != "Henry" for element in result["items"])
    assert all(element["name"][0]["given"] != "Duck" for element in result["items"])


def test_search_and_or(store: FHIRStore):
    """Checks two parameter "and,or" search
    """
    result = store.search(
        "Patient",
        {
            "multiple": {"name.family": ["Levin", "Chalmers"]},
            "identifier.value": ["12345"],
        },
    )
    assert len(result["items"]) == 2
    assert all(
        element["identifier"][0]["value"] != "654321" for element in result["items"]
    )
    assert all(
        element["identifier"][0]["value"] == "12345" for element in result["items"]
    )
    assert all(
        element["name"][0]["family"] == "Levin" or "Chalmers"
        for element in result["items"]
    )
    assert all(element["name"][0]["family"] != "Donald" for element in result["items"])


def test_search_nothing_found(store: FHIRStore):
    """Check that nothing is returned when nothing matches the query
    """
    result = store.search("Patient", {"identifier.value": ["654321", "12345"]})
    assert result["items"] == []


def test_search_max_size(store: FHIRStore):
    result = store.search("Patient", {}, result_size=2)
    assert len(result["items"]) == 2


def test_search_zero_size(store: FHIRStore):
    result = store.search("Patient", {}, result_size=0)
    assert len(result["items"]) == 0


def test_search_offset(store: FHIRStore):
    result = store.search("Patient", {}, offset=1)
    assert len(result["items"]) == 2


def test_search_offset_reach_max(store: FHIRStore):
    result = store.search("Patient", {}, offset=3)
    assert len(result["items"]) == 0

def test_search_size_reach_max(store: FHIRStore):
    result = store.search("Patient", {}, result_size=101)
    assert len(result["items"]) == 3


def test_search_simple_array(store: FHIRStore):
    result = store.search("Patient", {"name.family": ["Windsor"]})
    assert result["items"][0]["name"][2]["family"] == "Windsor"


def test_search_multiple_modifiers(store: FHIRStore):
    result = store.search("Patient", {"birthDate": ["lt1947-01-15", "gt1932-08-01"]})
    assert len(result["items"]) == 1


def test_search_multiple_modifiers_include(store: FHIRStore):
    result = store.search("Patient", {"birthDate": ["le1974-12-25", "gt1932-08-01"]})
    assert len(result["items"]) == 2


def test_search_ne(store: FHIRStore):
    result = store.search("Patient", {"identifier.value": ["ne12345"]})
    assert len(result["items"]) == 1


def test_search_identifier(store: FHIRStore):
    result = store.search("Patient", {"managingOrganization:identifier": ["98765"]})
    assert (
        result["items"][0]["managingOrganization"]["identifier"][0]["value"] == "98765"
    )


def test_count_all(store: FHIRStore):
    result = store.count("Patient", {})
    assert result["total"] == 3
    assert result["tag"]["code"] == "SUBSETTED"


def test_count_some(store: FHIRStore):
    result = store.count("Patient", {"identifier.value": ["ne12345"]})
    assert result["total"] == 1
    assert result["tag"]["code"] == "SUBSETTED"


def test_search_element(store: FHIRStore):
    result = store.search("Patient", {}, elements=["birthDate", "gender"])
    assert result["total"] == 3
    assert result["tag"]["code"] == "SUBSETTED"
    assert result["items"] == [
        {"gender": "male"},
        {"gender": "male", "birthDate": "1932-09-24"},
        {"gender": "male", "birthDate": "1974-12-25"},
    ]


def test_search_two_elements(store: FHIRStore):
    result = store.search("Patient", {}, elements=["birthDate"])
    assert result["total"] == 3
    assert result["tag"]["code"] == "SUBSETTED"
    assert result["items"] == [
        {},
        {"birthDate": "1932-09-24"},
        {"birthDate": "1974-12-25"},
    ]


def test_search_summary_text(store: FHIRStore):
    result = store.search("Patient", {}, elements=["text", "id", "meta"])
    assert result["total"] == 3
    assert result["tag"]["code"] == "SUBSETTED"
    assert result["items"] == [
        {
            "id": "pat1",
            "meta": {
                "tag": [
                    {
                        "code": "HTEST",
                        "display": "test health data",
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActReason",
                    }
                ]
            },
            "text": {
                "div": '<div xmlns="http://www.w3.org/1999/xhtml">\n      \n      <p>Patient Donald DUCK @ Acme Healthcare, Inc. MR = 654321</p>\n    \n    </div>',
                "status": "generated",
            },
        },
        {
            "id": "xcda",
            "text": {
                "div": '<div xmlns="http://www.w3.org/1999/xhtml">\n      \n      <p>Henry Levin the 7th</p>\n    \n    </div>',
                "status": "generated",
            },
        },
        {
            "id": "example",
            "meta": {
                "tag": [
                    {
                        "code": "HTEST",
                        "display": "test health data",
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ActReason",
                    }
                ]
            },
            "text": {
                "div": '<div xmlns="http://www.w3.org/1999/xhtml">\n\t\t\t<table>\n\t\t\t\t<tbody>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Name</td>\n\t\t\t\t\t\t<td>Peter James \n              <b>Chalmers</b> (&quot;Jim&quot;)\n            </td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Address</td>\n\t\t\t\t\t\t<td>534 Erewhon, Pleasantville, Vic, 3999</td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Contacts</td>\n\t\t\t\t\t\t<td>Home: unknown. Work: (03) 5555 6473</td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Id</td>\n\t\t\t\t\t\t<td>MRN: 12345 (Acme Healthcare)</td>\n\t\t\t\t\t</tr>\n\t\t\t\t</tbody>\n\t\t\t</table>\n\t\t</div>',
                "status": "generated",
            },
        },
    ]

