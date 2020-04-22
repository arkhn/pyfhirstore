import pytest
import json
from pytest import raises

from time import sleep
from fhirstore import FHIRStore, NotFoundError
from fhirstore.search.url_parser import URL_Parser, parse_comma, process_params
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
        store.search(resource_type="unknown", args={})


# def test_search_bad_params(store: FHIRStore):
#     """search() raises an error if params is not a dictionary"""

#     with raises(AssertionError, match="parameters must be a dictionary"):
#         store.search(resource_type="Patient", args="gender")


def test_search_output_type(store: FHIRStore, insert_patient):
    """Check that the output type is correct
    """
    result = store.search("Patient", {})
    assert result["resource_type"] == "Bundle"


def test_search_medicationrequest(store: FHIRStore, insert_medicationrequest):
    """Check that medicationrequest was inserted properly
    """
    result = store.search("MedicationRequest", {})
    assert result["total"] == 1


def test_parse_comma_simple():
    key, value = parse_comma("gender", "male")
    assert key == "gender"
    assert value == ["male"]


def test_parse_comma_multiple():
    key, value = parse_comma("gender", "female,male")
    assert key == "multiple"
    assert value == {"gender": ["female", "male"]}


def test_no_params():
    resp = process_params(ImmutableMultiDict([]))
    assert resp == {}


def test_one_param_no_comma():
    resp = process_params(ImmutableMultiDict([("gender", "female")]))
    assert resp == {"gender": ["female"]}


def test_one_param_one_comma():
    resp = process_params(ImmutableMultiDict([("gender", "female,male")]))
    assert resp == {"multiple": {"gender": ["female", "male"]}}


def test_one_param_two_entry_no_comma():

    resp = process_params(ImmutableMultiDict([("name", "John"), ("name", "Lena")]))
    assert resp == {"name": ["John", "Lena"]}


def test_one_param_two_entries_one_comma():
    resp = process_params(ImmutableMultiDict([("language", "FR"), ("language", "NL,EN")]))
    assert resp == {"language": ["FR"], "multiple": {"language": ["NL", "EN"]}}


def test_count_summary():
    url_parser = URL_Parser(ImmutableMultiDict([("_summary", "count")]), "Patient")
    url_parser.process_params()
    assert url_parser.processed_params == {}
    assert url_parser.resource_type == "Patient"
    assert url_parser.core_args == {}
    assert url_parser.sort is None
    assert url_parser.elements is None
    assert url_parser.include is False
    assert url_parser.summary is True
    assert url_parser.is_summary_count is True
    assert url_parser.offset == 0
    assert url_parser.result_size == 100


def test_text_summary():
    url_parser = URL_Parser(ImmutableMultiDict([("_summary", "text")]), "Patient")
    url_parser.process_params()
    assert url_parser.processed_params == {}
    assert url_parser.resource_type == "Patient"
    assert url_parser.core_args == {}
    assert url_parser.sort is None
    assert url_parser.elements == ["text", "id", "meta"]
    assert url_parser.include is False
    assert url_parser.summary is True
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 100


def test_element():
    url_parser = URL_Parser(ImmutableMultiDict([("_element", "birthDate")]), "Patient")
    url_parser.process_params()
    assert url_parser.processed_params == {}
    assert url_parser.resource_type == "Patient"
    assert url_parser.core_args == {}
    assert url_parser.sort is None
    assert url_parser.elements == ["birthDate"]
    assert url_parser.include is False
    assert url_parser.summary is False
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 100


def test_elements():
    url_parser = URL_Parser(ImmutableMultiDict([("_element", "birthDate,gender")]), "Patient")
    url_parser.process_params()
    assert url_parser.processed_params == {}
    assert url_parser.resource_type == "Patient"
    assert url_parser.core_args == {}
    assert url_parser.sort is None
    assert url_parser.elements == ["birthDate", "gender"]
    assert url_parser.include is False
    assert url_parser.summary is False
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 100


def test_result_size():
    url_parser = URL_Parser(ImmutableMultiDict([("_count", "2")]), "Patient")
    url_parser.process_params()
    assert url_parser.processed_params == {}
    assert url_parser.resource_type == "Patient"
    assert url_parser.core_args == {}
    assert url_parser.sort is None
    assert url_parser.elements is None
    assert url_parser.include is False
    assert url_parser.summary is False
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 2


def test_result_size_elements():
    url_parser = URL_Parser(
        ImmutableMultiDict(
            [("_count", "2"), ("_summary", "False"), ("_element", "birthDate,gender")]
        ),
        "Patient",
    )
    url_parser.process_params()
    assert url_parser.processed_params == {}
    assert url_parser.resource_type == "Patient"
    assert url_parser.core_args == {}
    assert url_parser.sort is None
    assert url_parser.elements == ["birthDate", "gender"]
    assert url_parser.include is False
    assert url_parser.summary is True
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 2


def test_mix_parameters():
    url_parser = URL_Parser(
        ImmutableMultiDict(
            [
                ("_count", "200"),
                ("_summary", "text"),
                ("_element", "birthDate,gender"),
                ("language", "FR"),
                ("language", "EN,NL"),
            ]
        ),
        "Patient",
    )
    url_parser.process_params()
    assert url_parser.processed_params == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert url_parser.resource_type == "Patient"
    assert url_parser.core_args == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert url_parser.sort is None
    assert url_parser.elements == ["text", "id", "meta"]
    assert url_parser.include is False
    assert url_parser.summary is True
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 200


def test_mix_params_count():
    url_parser = URL_Parser(
        ImmutableMultiDict([("_summary", "count"), ("language", "FR"), ("language", "EN,NL")]),
        "Patient",
    )
    url_parser.process_params()
    assert url_parser.processed_params == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert url_parser.resource_type == "Patient"
    assert url_parser.core_args == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert url_parser.sort is None
    assert url_parser.elements is None
    assert url_parser.include is False
    assert url_parser.summary is True
    assert url_parser.is_summary_count is True
    assert url_parser.offset == 0
    assert url_parser.result_size == 100


def test_mix_params_sort():
    url_parser = URL_Parser(
        ImmutableMultiDict([("language", "FR"), ("language", "EN,NL"), ("_sort", "birthDate")]),
        "Patient",
    )
    url_parser.process_params()
    assert url_parser.processed_params == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert url_parser.core_args == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert url_parser.sort == ["birthDate"]
    assert url_parser.elements is None
    assert url_parser.include is False
    assert url_parser.summary is False
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 100


def test_mix_params_include():
    url_parser = URL_Parser(
        ImmutableMultiDict(
            [
                ("language", "FR"),
                ("language", "EN,NL"),
                ("_include", "Patient:managingOrganization"),
            ]
        ),
        "Patient",
    )
    url_parser.process_params()
    assert url_parser.processed_params == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert url_parser.core_args == {
        "language": ["FR"],
        "multiple": {"language": ["EN", "NL"]},
    }
    assert url_parser.sort is None
    assert url_parser.elements is None
    assert url_parser.include == ["managingOrganization"]
    assert url_parser.summary is False
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 100


def test_mix_params_include_multiple():
    url_parser = URL_Parser(
        ImmutableMultiDict([("_include", "MedicationRequest:subject,MedicationRequest:requester")]),
        "Patient",
    )
    url_parser.process_params()
    assert url_parser.processed_params == {}
    assert url_parser.core_args == {}
    assert url_parser.sort is None
    assert url_parser.elements is None
    assert url_parser.include == ["subject", "requester"]
    assert url_parser.summary is False
    assert url_parser.is_summary_count is False
    assert url_parser.offset == 0
    assert url_parser.result_size == 100


def test_sort_param():
    url_parser = URL_Parser(ImmutableMultiDict([("_sort", "birthDate")]), "Patient")
    url_parser.sort_params()
    assert url_parser.sort == ["birthDate"]


def test_no_sort_param():
    url_parser = URL_Parser(ImmutableMultiDict([("language", "FR")]), "Patient")
    url_parser.sort_params()
    assert url_parser.sort is None


def test_sort_param_desc():
    url_parser = URL_Parser(ImmutableMultiDict([("_sort", "-birthDate")]), "Patient")
    url_parser.sort_params()
    assert url_parser.sort == [{"birthDate": {"order": "desc"}}]


def test_sort_score():
    url_parser = URL_Parser(ImmutableMultiDict([("_sort", "_score")]), "Patient")
    url_parser.sort_params()
    assert url_parser.sort == ["_score"]


def test_sort_score_asc():
    url_parser = URL_Parser(ImmutableMultiDict([("_sort", "-_score")]), "Patient")
    url_parser.sort_params()
    assert url_parser.sort == [{"_score": {"order": "asc"}}]


def test_sort_params():
    url_parser = URL_Parser(ImmutableMultiDict([("_sort", "-birthDate,active")]), "Patient")
    url_parser.sort_params()
    assert url_parser.sort == [{"birthDate": {"order": "desc"}}, "active"]


def test_sort_params_score():
    url_parser = URL_Parser(ImmutableMultiDict([("_sort", "-birthDate,_score")]), "Patient")
    url_parser.sort_params()
    assert url_parser.sort == [{"birthDate": {"order": "desc"}}, "_score"]


def test_sort_params_desc_and_score_asc():
    url_parser = URL_Parser(ImmutableMultiDict([("_sort", "-birthDate,-_score")]), "Patient")
    url_parser.sort_params()
    assert url_parser.sort == [{"birthDate": {"order": "desc"}}, {"_score": {"order": "asc"}}]
