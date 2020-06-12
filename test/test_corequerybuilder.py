import pytest
import json
from pytest import raises

from fhirstore.search import build_simple_query, build_core_query, build_element_query, check_prefix, is_numeric_type
from collections.abc import Mapping
from fhirstore import FHIRStore, NotFoundError


######
## TEST BUILD SIMPLE QUERY
##
######

def test_is_numeric_type():
    integers = ["1", "-1", "123456789"]
    decimals = ["1.1", "-1.1", "0.1", "1e2", "1e-3"]

    datetimes = ["2017-01-01T00:00:00Z", "2015-02-07T13:28:17.239+02:00", "2015-02-07T13:28:17-05:00","2018", "1973-06","1905-08-23"]
    not_numeric = ["patient", "in-progress", "012312432",".45", "cdcd3132-2eb8-496c-ae88-1fabdaea3efa", "0vrvbb01154wnqyot495mp", "ckb0vrvbb01154wnqyot495mp"]

    for item in integers:
        assert is_numeric_type(item)
    for item in decimals:
        assert is_numeric_type(item)
    for item in datetimes:
        assert is_numeric_type(item)
    for item in not_numeric:
        assert is_numeric_type(item) is None

def test_check_prefix():

    integers = ["eq1", "eq-1", "eq123456789"]
    decimals = ["eq1.1", "eq-1.1", "eq0.1", "eq1e2", "eq1e-3"]
    datetimes = ["eq2017-01-01T00:00:00Z", "eq2015-02-07T13:28:17.239+02:00", "eq2015-02-07T13:28:17-05:00","eq2018", "eq1973-06","eq1905-08-23",]
    not_numeric = ["eqpatient", "eqin-progress", "eq012312432", "eq.45", "eq0vrvbb01154wnqyot495mp","eqb6azyyd01384knzanc76mja","s010101"]
    special_suffixes = ["le1974-12-25"]

    for item in integers:
        suffix, argument = check_prefix(item)
        assert suffix== "eq"
    for item in decimals:
        suffix, argument = check_prefix(item)
        assert suffix=="eq"

    for item in datetimes:
        suffix, argument = check_prefix(item)
        assert suffix=="eq"
    for item in not_numeric:
        suffix, argument = check_prefix(item)
        assert suffix==None
    for item in special_suffixes:
        suffix, argument = check_prefix(item)
        assert suffix=="le"
        assert argument=="1974-12-25"


def test_basic_int_query():
    result = build_simple_query({"value": [3]})
    assert result == {"match": {"value": 3}}


def test_basic_float_query():
    result = build_simple_query({"value": [3.5]})
    assert result == {"match": {"value": 3.5}}


def test_simple_query_contains():
    """Validates that the ES query is correct with modifier contains
    """
    result = build_simple_query({"name.family:contains": ["Dona"]})
    assert result == {"query_string": {"query": "*Dona*", "fields": ["name.family"]}}


def test_simple_query_exact():
    """Validates that the ES query is correct with modifier contains
    """
    result = build_core_query({"name.family:exact": ["Donald"]})
    assert result == {
        "simple_query_string": {"query": '"Donald"', "fields": ["name.family"], "flags": "PHRASE"}
    }


def test_simple_query_not():
    """Validates that the ES query is correct with modifier contains
    """
    result = build_core_query({"name.family:not": ["Donald"]})
    assert result == {"simple_query_string": {"query": "-Donald", "fields": ["name.family"]}}


def test_simple_query_identifier():
    """Validates that the ES query is correct with modifier contains
    """
    result = build_simple_query(
        {"managingOrganization:identifier": ["urn:oid:0.7.6.5.4.3.2|98765"]}
    )
    assert result == {
        "simple_query_string": {
            "query": '"urn:oid:0.7.6.5.4.3.2|98765"',
            "fields": ["managingOrganization.identifier.value"],
            "flags": "PHRASE",
        }
    }


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


def test_simple_query_ne():
    """Validates that the ES query is correct with modifier 'gt'
    """
    result = build_core_query({"birthDate": ["ne1974-12-25"]})
    assert result == {"simple_query_string": {"query": "-1974-12-25", "fields": ["birthDate"]}}


def test_composite_query_pipe():
    """Validates that the ES query with token | is correct
    """
    result = build_simple_query({"contained.code.coding": ["http://snomed.info/sct|324252006"]})
    assert result == {
        "bool": {
            "must": [
                {
                    "simple_query_string": {
                        "query": '"http://snomed.info/sct"',
                        "fields": ["contained.code.coding.system"],
                        "flags": "PHRASE",
                    }
                },
                {
                    "simple_query_string": {
                        "query": '"324252006"',
                        "fields": ["contained.code.coding.code", "contained.code.coding.value"],
                        "flags": "PHRASE",
                    }
                },
            ]
        }
    }


def test_simple_query_string():
    """Validates that the ES query is correct"""
    result = build_simple_query({"birthDate": ["1974-12-25"]})
    assert result == {
        "simple_query_string": {"fields": ["birthDate"], "query": '"1974-12-25"', "flags": "PHRASE"}
    }


def test_simple_query_output():
    """Validates that the output of build_simple_query
    is a dictionary
    """
    result = build_simple_query({"gender": ["female"]})
    assert isinstance(result, Mapping)


def test_element_query_output():
    """Validates that the output of build_simple_query
    is a dictionary
    """
    element_query = build_element_query("gender", "female")
    assert isinstance(element_query, Mapping)


##########
##
# Test build_core_query
##
##########


def test_core_query_builder_validate():
    with raises(AssertionError, match="parameters must be a dictionary"):
        build_core_query(["name"])


def test_core_query_builder():
    corequerybuilder = build_core_query({})
    assert corequerybuilder == {"match_all": {}}


def test_core_query_builder_int():
    corequerybuilder = build_core_query({"value": [3]})
    assert corequerybuilder == {"match": {"value": 3}}


def test_core_query_builder_float():
    corequerybuilder = build_core_query({"value": [3.5]})
    assert corequerybuilder == {"match": {"value": 3.5}}


def test_core_query_correct_string():
    """Validates that the ES query is correct"""
    corequerybuilder = build_core_query({"birthDate": ["1974-12-25"]})
    assert corequerybuilder == {
        "simple_query_string": {"fields": ["birthDate"], "query": '"1974-12-25"', "flags": "PHRASE"}
    }


def test_core_query_multiple():
    corequerybuilder = build_core_query({"multiple": {"language": ["NL", "EN"]}})
    assert corequerybuilder == {
        "bool": {
            "should": [
                {
                    "simple_query_string": {
                        "query": '"NL"',
                        "fields": ["language"],
                        "flags": "PHRASE",
                    }
                },
                {
                    "simple_query_string": {
                        "query": '"EN"',
                        "fields": ["language"],
                        "flags": "PHRASE",
                    }
                },
            ]
        }
    }


def test_core_query_composite():
    corequerybuilder = build_core_query(
        {"language": ["FR"], "multiple": {"language": ["NL", "EN"]}}
    )
    assert corequerybuilder == {
        "bool": {
            "must": [
                {
                    "simple_query_string": {
                        "query": '"FR"',
                        "fields": ["language"],
                        "flags": "PHRASE",
                    }
                },
                {
                    "bool": {
                        "should": [
                            {
                                "simple_query_string": {
                                    "query": '"NL"',
                                    "fields": ["language"],
                                    "flags": "PHRASE",
                                }
                            },
                            {
                                "simple_query_string": {
                                    "query": '"EN"',
                                    "fields": ["language"],
                                    "flags": "PHRASE",
                                }
                            },
                        ]
                    }
                },
            ]
        }
    }


def test_simple_query_correct_or():
    """Validates that the ES query for "OR" is correct
    """
    result = build_simple_query({"multiple": {"name.family": ["Donald", "Chalmers"]}})
    assert result == {
        "bool": {
            "should": [
                {
                    "simple_query_string": {
                        "query": '"Donald"',
                        "fields": ["name.family"],
                        "flags": "PHRASE",
                    }
                },
                {
                    "simple_query_string": {
                        "query": '"Chalmers"',
                        "fields": ["name.family"],
                        "flags": "PHRASE",
                    }
                },
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
                        "query": '"Donald"',
                        "flags": "PHRASE",
                    }
                },
                {
                    "simple_query_string": {
                        "fields": ["name.family"],
                        "query": '"Chalmers"',
                        "flags": "PHRASE",
                    }
                },
            ]
        }
    }


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


def test_core_query_output():
    """Validates that the output of build_element_query
    is a dictionary
    """
    result = build_simple_query({"gender": ["female"]})
    assert isinstance(result, Mapping)
