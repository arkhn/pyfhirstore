import pytest
import json
from pytest import raises

from time import sleep
from fhirstore import FHIRStore, NotFoundError
from collections.abc import Mapping
from werkzeug.datastructures import ImmutableMultiDict

# For now, this class assumes an already existing store exists
# (store.bootstrap was run)


@pytest.fixture(scope="module")
def insert_patient(es_client):
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
        sleep(5 / 10000)
    return es_client


@pytest.fixture(scope="module")
def insert_medicationrequest(es_client):
    if not es_client.indices.exists("fhirstore.medicationrequest"):
        with open("test/fixtures/medicationrequest-example.json") as i:
            medicationrequest_1 = json.load(i)
            es_client.index(index="fhirstore.medicationrequest", body=medicationrequest_1)

    while es_client.count(index="fhirstore.medicationrequest")["count"] < 1:
        sleep(5 / 10000)
    return es_client


def test_search_output_type(store: FHIRStore, insert_patient):
    """Check that the output type is correct
    """
    result = store.comprehensive_search("Patient", ImmutableMultiDict([]))
    assert result.content["resource_type"] == "Bundle"


def test_search_medicationrequest(store: FHIRStore, insert_medicationrequest):
    """Check that medicationrequest was inserted properly
    """
    result = store.comprehensive_search("MedicationRequest", {})
    assert result.content["total"] == 1


def test_search_no_parameters(store: FHIRStore):
    """Checks that all elements of the resource are returned
    """
    result = store.comprehensive_search("Patient", ImmutableMultiDict([]))
    assert len(result.content["entry"]) == 3


def test_search_one_param_simple(store: FHIRStore):
    """Checks simple one parameter search
    """
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("identifier.value", "654321")])
    )
    assert len(result.content["entry"]) == 1
    assert all(
        element["resource"]["identifier"][0]["value"] == "654321"
        for element in result.content["entry"]
    )


def test_search_one_param_multiple(store: FHIRStore):
    """Checks that multiple elements of one parameter are queried
    """
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("name.family", "Chalmers,Levin")])
    )
    assert len(result.content["entry"]) == 2
    assert any(
        element["resource"]["name"][0]["family"] == ("Chalmers" or "Levin")
        for element in result.content["entry"]
    )
    assert all(
        element["resource"]["name"][0]["family"] != "Donald" for element in result.content["entry"]
    )


def test_search_one_param_modifier_str_contains(store: FHIRStore):
    """Checks that "contains" string modifier works
    """
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("managingOrganization.reference:contains", "Organization")])
    )
    assert len(result.content["entry"]) == 3
    assert any(
        element["resource"]["managingOrganization"]["reference"] == "Organization/1"
        for element in result.content["entry"]
    )
    assert any(
        element["resource"]["managingOrganization"]["reference"]
        != "Organization/2.16.840.1.113883.19.5"
        for element in result.content["entry"]
    )


def test_search_one_param_modifier_str_exact(store: FHIRStore):
    """Checks that "exact" string modifier works
    """
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("name.family:exact", "Donald")])
    )
    assert len(result.content["entry"]) == 1
    assert all(
        element["resource"]["name"][0]["family"] == "Donald" for element in result.content["entry"]
    )
    assert all(
        element["resource"]["name"][0]["family"] != ("Chalmers" or "Levin")
        for element in result.content["entry"]
    )


def test_search_one_param_modifier_str_not(store: FHIRStore):
    """Checks that "not" string modifier works
    """
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("name.family:not", "Donald")])
    )
    assert all(
        element["resource"]["name"][0]["family"] != "Donald" for element in result.content["entry"]
    )


def test_search_two_params_and(store: FHIRStore):
    """Checks two parameter "and" search
    """
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("name.family", "Chalmers"), ("identifier.value", "12345")])
    )
    assert all(
        element["resource"]["identifier"][0]["value"] == "12345"
        for element in result.content["entry"]
    )
    assert all(
        element["resource"]["identifier"][0]["value"] != "654321"
        for element in result.content["entry"]
    )
    assert all(
        element["resource"]["name"][0]["family"] == "Chalmers"
        for element in result.content["entry"]
    )
    assert all(
        element["resource"]["name"][0]["family"] != "Donald" or "Levin"
        for element in result.content["entry"]
    )
    assert len(result.content["entry"]) == 1


def test_search_one_params_and(store: FHIRStore):
    """Checks one parameter "and" search
    """
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("name.given", "Peter,James")])
    )
    assert len(result.content["entry"]) == 1
    assert any(
        element["resource"]["name"][0]["given"] == "Peter" or "James"
        for element in result.content["entry"]
    )
    assert all(
        element["resource"]["name"][0]["given"] != "Henry" for element in result.content["entry"]
    )
    assert all(
        element["resource"]["name"][0]["given"] != "Duck" for element in result.content["entry"]
    )


def test_search_and_or(store: FHIRStore):
    """Checks two parameter "and,or" search
    """
    result = store.comprehensive_search(
        "Patient",
        ImmutableMultiDict([("name.family", "Levin,Chalmers"), ("identifier.value", "12345")]),
    )
    assert len(result.content["entry"]) == 2
    assert all(
        element["resource"]["identifier"][0]["value"] != "654321"
        for element in result.content["entry"]
    )
    assert all(
        element["resource"]["identifier"][0]["value"] == "12345"
        for element in result.content["entry"]
    )
    assert all(
        element["resource"]["name"][0]["family"] == "Levin" or "Chalmers"
        for element in result.content["entry"]
    )
    assert all(
        element["resource"]["name"][0]["family"] != "Donald" for element in result.content["entry"]
    )


def test_search_identifier(store: FHIRStore):
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("managingOrganization:identifier", "98765")])
    )
    assert (
        result.content["entry"][0]["resource"]["managingOrganization"]["identifier"][0]["value"]
        == "98765"
    )


def test_count_all(store: FHIRStore):
    result = store.comprehensive_search("Patient", ImmutableMultiDict([("_summary", "count")]))
    assert result.content["total"] == 3
    assert result.content["tag"]["code"] == "SUBSETTED"


def test_count_medicationrequest(store: FHIRStore):
    result = store.comprehensive_search(
        "MedicationRequest", ImmutableMultiDict([("_summary", "count")])
    )
    assert result.content["total"] == 1


def test_count_some(store: FHIRStore):
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("_summary", "count"), ("identifier.value", "ne12345")])
    )
    assert result.content["total"] == 1
    assert result.content["tag"]["code"] == "SUBSETTED"


def test_search_element(store: FHIRStore):
    result = store.comprehensive_search(
        "Patient", ImmutableMultiDict([("_elements", "birthDate,gender")])
    )
    assert result.content["total"] == 3
    assert result.content["tag"]["code"] == "SUBSETTED"
    assert result.content["entry"] == [
        {"resource": {"gender": "male"}, "search": {"mode": "match"}},
        {"resource": {"gender": "male", "birthDate": "1932-09-24"}, "search": {"mode": "match"},},
        {"resource": {"gender": "male", "birthDate": "1974-12-25"}, "search": {"mode": "match"},},
    ]


def test_search_two_elements(store: FHIRStore):
    result = store.comprehensive_search("Patient", ImmutableMultiDict([("_elements", "birthDate")]))
    assert result.content["total"] == 3
    assert result.content["tag"]["code"] == "SUBSETTED"
    assert result.content["entry"] == [
        {"resource": {}, "search": {"mode": "match"}},
        {"resource": {"birthDate": "1932-09-24"}, "search": {"mode": "match"}},
        {"resource": {"birthDate": "1974-12-25"}, "search": {"mode": "match"}},
    ]


def test_search_summary_text(store: FHIRStore):
    result = store.comprehensive_search("Patient", ImmutableMultiDict([("_summary", "text")]))
    assert result.content["total"] == 3
    assert result.content["tag"]["code"] == "SUBSETTED"
    assert result.content["entry"] == [
        {
            "resource": {
                "meta": {
                    "tag": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-ActReason",
                            "code": "HTEST",
                            "display": "test health data",
                        }
                    ]
                },
                "id": "pat1",
                "text": {
                    "div": '<div xmlns="http://www.w3.org/1999/xhtml">\n      \n      <p>Patient Donald DUCK @ Acme Healthcare, Inc. MR = 654321</p>\n    \n    </div>',
                    "status": "generated",
                },
            },
            "search": {"mode": "match"},
        },
        {
            "resource": {
                "id": "xcda",
                "text": {
                    "div": '<div xmlns="http://www.w3.org/1999/xhtml">\n      \n      <p>Henry Levin the 7th</p>\n    \n    </div>',
                    "status": "generated",
                },
            },
            "search": {"mode": "match"},
        },
        {
            "resource": {
                "meta": {
                    "tag": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-ActReason",
                            "code": "HTEST",
                            "display": "test health data",
                        }
                    ]
                },
                "id": "example",
                "text": {
                    "div": '<div xmlns="http://www.w3.org/1999/xhtml">\n\t\t\t<table>\n\t\t\t\t<tbody>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Name</td>\n\t\t\t\t\t\t<td>Peter James \n              <b>Chalmers</b> (&quot;Jim&quot;)\n            </td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Address</td>\n\t\t\t\t\t\t<td>534 Erewhon, Pleasantville, Vic, 3999</td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Contacts</td>\n\t\t\t\t\t\t<td>Home: unknown. Work: (03) 5555 6473</td>\n\t\t\t\t\t</tr>\n\t\t\t\t\t<tr>\n\t\t\t\t\t\t<td>Id</td>\n\t\t\t\t\t\t<td>MRN: 12345 (Acme Healthcare)</td>\n\t\t\t\t\t</tr>\n\t\t\t\t</tbody>\n\t\t\t</table>\n\t\t</div>',
                    "status": "generated",
                },
            },
            "search": {"mode": "match"},
        },
    ]


def test_handle_pipe(store: FHIRStore):
    result = store.comprehensive_search(
        "MedicationRequest",
        ImmutableMultiDict([("contained.code.coding", "http://snomed.info/sct|324252006")]),
    )
    assert result.content["entry"][0]["resource"]["id"] == "medrx0302"
    assert (
        result.content["entry"][0]["resource"]["contained"][0]["code"]["coding"][0]["system"]
        == "http://snomed.info/sct"
    )
    assert (
        result.content["entry"][0]["resource"]["contained"][0]["code"]["coding"][0]["code"]
        == "324252006"
    )


def test_sort(store: FHIRStore):
    result = store.comprehensive_search("Patient", ImmutableMultiDict([("_sort", "birthDate")]))
    assert (
        result.content["entry"][0]["resource"]["birthDate"]
        <= result.content["entry"][1]["resource"]["birthDate"]
    )


def test_sort_desc(store: FHIRStore):
    result = store.comprehensive_search("Patient", ImmutableMultiDict([("_sort", "-birthDate")]))
    assert (
        result.content["entry"][0]["resource"]["birthDate"]
        >= result.content["entry"][1]["resource"]["birthDate"]
    )
