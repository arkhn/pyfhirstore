import pytest
import json
from pytest import raises

from time import sleep
from fhirstore import FHIRStore, NotFoundError
from fhirstore.search import SearchArguments, parse_comma, pre_process_params
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


def test_search_bad_resource_type(store: FHIRStore):
    """search() raises error if resource type is unknown"""

    with raises(NotFoundError, match='unsupported FHIR resource: "unknown"'):
        store.comprehensive_search(resource_type="unknown", args={})


def test_search_output_type(store: FHIRStore, insert_patient):
    """Check that the output type is correct
    """
    result = store.comprehensive_search("Patient", {})
    assert result.content["resource_type"] == "Bundle"


def test_search_medicationrequest(store: FHIRStore, insert_medicationrequest):
    """Check that medicationrequest was inserted properly
    """
    result = store.comprehensive_search("MedicationRequest", {})
    assert result.content["total"] == 1


def test_parse_comma_simple():
    key, value = parse_comma("gender", "male")
    assert key == "gender"
    assert value == ["male"]


def test_parse_comma_multiple():
    key, value = parse_comma("gender", "female,male")
    assert key == "multiple"
    assert value == {"gender": ["female", "male"]}


def test_no_params():
    resp = pre_process_params(ImmutableMultiDict([]))
    assert resp == {}


def test_one_param_no_comma():
    resp = pre_process_params(ImmutableMultiDict([("gender", "female")]))
    assert resp == {"gender": ["female"]}


def test_one_param_one_comma():
    resp = pre_process_params(ImmutableMultiDict([("gender", "female,male")]))
    assert resp == {"multiple": {"gender": ["female", "male"]}}


def test_one_param_two_entry_no_comma():

    resp = pre_process_params(ImmutableMultiDict([("name", "John"), ("name", "Lena")]))
    assert resp == {"name": ["John", "Lena"]}


def test_one_param_two_entries_one_comma():
    resp = pre_process_params(ImmutableMultiDict([("language", "FR"), ("language", "NL,EN")]))
    assert resp == {"language": ["FR"], "multiple": {"language": ["NL", "EN"]}}


def test_count_summary():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_summary", "count")]), "Patient")
    assert search_args.resource_type == "Patient"
    assert search_args.core_args == {}
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] is None
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is True
    assert search_args.formatting_args["is_summary_count"] is True
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 100


def test_text_summary():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_summary", "text")]), "Patient")
    assert search_args.resource_type == "Patient"
    assert search_args.core_args == {}
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] == ["text", "id", "meta"]
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is True
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 100


def test_element():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_elements", "birthDate")]), "Patient")
    assert search_args.resource_type == "Patient"
    assert search_args.core_args == {}
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] == ["birthDate"]
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is False
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 100


def test_elements():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_elements", "birthDate,gender")]), "Patient")
    assert search_args.resource_type == "Patient"
    assert search_args.core_args == {}
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] == ["birthDate", "gender"]
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is False
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 100


def test_result_size():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_count", "2")]), "Patient")
    assert search_args.resource_type == "Patient"
    assert search_args.core_args == {}
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] is None
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is False
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 2


def test_result_size_elements():
    search_args = SearchArguments()
    search_args.parse(
        ImmutableMultiDict(
            [("_count", "2"), ("_summary", "False"), ("_elements", "birthDate,gender")]
        ),
        "Patient",
    )
    assert search_args.resource_type == "Patient"
    assert search_args.core_args == {}
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] == ["birthDate", "gender"]
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is True
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 2


def test_mix_parameters():
    search_args = SearchArguments()
    search_args.parse(
        ImmutableMultiDict(
            [
                ("_count", "200"),
                ("_summary", "text"),
                ("_elements", "birthDate,gender"),
                ("language", "FR"),
                ("language", "EN,NL"),
            ]
        ),
        "Patient",
    )
    assert search_args.resource_type == "Patient"
    assert search_args.core_args == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] == ["text", "id", "meta"]
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is True
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 200


def test_mix_params_count():
    search_args = SearchArguments()
    search_args.parse(
        ImmutableMultiDict([("_summary", "count"), ("language", "FR"), ("language", "EN,NL")]),
        "Patient",
    )
    assert search_args.resource_type == "Patient"
    assert search_args.core_args == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] is None
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is True
    assert search_args.formatting_args["is_summary_count"] is True
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 100


def test_mix_params_sort():
    search_args = SearchArguments()
    search_args.parse(
        ImmutableMultiDict([("language", "FR"), ("language", "EN,NL"), ("_sort", "birthDate")]),
        "Patient",
    )
    assert search_args.core_args == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert search_args.formatting_args["sort"] == ["birthDate"]
    assert search_args.formatting_args["elements"] is None
    assert search_args.formatting_args["include"] is None
    assert search_args.formatting_args["summary"] is False
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 100


def test_mix_params_include():
    search_args = SearchArguments()
    search_args.parse(
        ImmutableMultiDict(
            [
                ("language", "FR"),
                ("language", "EN,NL"),
                ("_include", "Patient:managingOrganization"),
            ]
        ),
        "Patient",
    )
    assert search_args.core_args == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] is None
    assert search_args.formatting_args["include"] == ["managingOrganization"]
    assert search_args.formatting_args["summary"] is False
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 100


def test_mix_params_include_multiple():
    search_args = SearchArguments()
    search_args.parse(
        ImmutableMultiDict([("_include", "MedicationRequest:subject,MedicationRequest:requester")]),
        "Patient",
    )
    assert search_args.core_args == {}
    assert search_args.formatting_args["sort"] is None
    assert search_args.formatting_args["elements"] is None
    assert search_args.formatting_args["include"] == ["subject", "requester"]
    assert search_args.formatting_args["summary"] is False
    assert search_args.formatting_args["is_summary_count"] is False
    assert search_args.meta_args["offset"] == 0
    assert search_args.meta_args["result_size"] == 100


def test_sort_param():
    search_args = SearchArguments()
    search_args.parse({"_sort": ["birthDate"]}, "Patient")
    assert search_args.formatting_args["sort"] == ["birthDate"]


def test_no_sort_param():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("language", "FR")]), "Patient")
    assert search_args.formatting_args["sort"] is None


def test_sort_param_desc():
    search_args = SearchArguments()
    search_args.parse({"_sort": ["-birthDate"]}, "Patient")
    assert search_args.formatting_args["sort"] == [{"birthDate": {"order": "desc"}}]


def test_sort_score():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_sort", "_score")]), "Patient")
    assert search_args.formatting_args["sort"] == ["_score"]


def test_sort_score_asc():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_sort", "-_score")]), "Patient")
    assert search_args.formatting_args["sort"] == [{"_score": {"order": "asc"}}]


def test_sort_params():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_sort", "-birthDate,active")]), "Patient")
    assert search_args.formatting_args["sort"] == [{"birthDate": {"order": "desc"}}, "active"]


def test_sort_params_score():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_sort", "-birthDate,_score")]), "Patient")
    assert search_args.formatting_args["sort"] == [{"birthDate": {"order": "desc"}}, "_score"]


def test_sort_params_desc_and_score_asc():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_sort", "-birthDate,-_score")]), "Patient")
    assert search_args.formatting_args["sort"] == [
        {"birthDate": {"order": "desc"}},
        {"_score": {"order": "asc"}},
    ]
