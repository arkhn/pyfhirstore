import pytest
import json
from pytest import raises

from fhirstore.search import build_element_query, CoreQueryBuilder
from collections.abc import Mapping
from fhirstore import FHIRStore, NotFoundError


######
## TEST BUILD SIMPLE QUERY
##
######


def test_basic_int_query():
    corequerybuilder = CoreQueryBuilder({"value": [3]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"match": {"value": 3}}


def test_basic_float_query():
    corequerybuilder = CoreQueryBuilder({"value": [3.5]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"match": {"value": 3.5}}


def test_simple_query_contains():
    """Validates that the ES query is correct with modifier contains
    """
    corequerybuilder = CoreQueryBuilder({"name.family:contains": ["Dona"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"query_string": {"query": "*Dona*", "default_field": "name.family"}}


def test_simple_query_exact():
    """Validates that the ES query is correct with modifier contains
    """
    corequerybuilder = CoreQueryBuilder({"name.family:exact": ["Donald"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"query_string": {"query": "Donald", "fields": ["name.family"]}}


# TODO : new tests
# def test_simple_query_not():
#     """Validates that the ES query is correct with modifier contains
#     """
#     corequerybuilder = CoreQueryBuilder({"name.family:exact": ["Donald"]})
#     result = corequerybuilder.build_simple_query(corequerybuilder.args)
#     assert result == {"query_string": {"query": "Donald", "fields": ["name.family"]}}


# def test_simple_query_notin():
#     """Validates that the ES query is correct with modifier contains
#     """
#     corequerybuilder = CoreQueryBuilder({"name.family:exact": ["Donald"]})
#     result = corequerybuilder.build_simple_query(corequerybuilder.args)
#     assert result == {"query_string": {"query": "Donald", "fields": ["name.family"]}}

# def test_simple_query_in():
#     """Validates that the ES query is correct with modifier contains
#     """
#     corequerybuilder = CoreQueryBuilder({"name.family:exact": ["Donald"]})
#     result = corequerybuilder.build_simple_query(corequerybuilder.args)
#     assert result == {"query_string": {"query": "Donald", "fields": ["name.family"]}}

# def test_simple_query_below():
#     """Validates that the ES query is correct with modifier contains
#     """
#     corequerybuilder = CoreQueryBuilder({"name.family:exact": ["Donald"]})
#     result = corequerybuilder.build_simple_query(corequerybuilder.args)
#     assert result == {"query_string": {"query": "Donald", "fields": ["name.family"]}}


# def test_simple_query_identifier():
#     """Validates that the ES query is correct with modifier contains
#     """
#     corequerybuilder = CoreQueryBuilder({"name.family:exact": ["Donald"]})
#     result = corequerybuilder.build_simple_query(corequerybuilder.args)
#     assert result == {"query_string": {"query": "Donald", "fields": ["name.family"]}}


def test_simple_query_gt():
    """Validates that the ES query is correct with modifier 'gt'
    """
    corequerybuilder = CoreQueryBuilder({"birthDate": ["gt1974-12-25"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"range": {"birthDate": {"gt": "1974-12-25"}}}


def test_simple_query_ge():
    """Validates that the ES query is correct with modifier 'ge'
    """
    corequerybuilder = CoreQueryBuilder({"birthDate": ["ge1974-12-25"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"range": {"birthDate": {"gte": "1974-12-25"}}}


def test_simple_query_le():
    """Validates that the ES query is correct with modifier 'le'
    """
    corequerybuilder = CoreQueryBuilder({"birthDate": ["le1974-12-25"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"range": {"birthDate": {"lte": "1974-12-25"}}}


def test_simple_query_lt():
    """Validates that the ES query is correct with modifier 'lt'
    """
    corequerybuilder = CoreQueryBuilder({"birthDate": ["lt1974-12-25"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"range": {"birthDate": {"lt": "1974-12-25"}}}


def test_simple_query_eq():
    """Validates that the ES query is correct with modifier 'eq'
    """
    corequerybuilder = CoreQueryBuilder({"birthDate": ["eq1974-12-25"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"match": {"birthDate": "1974-12-25"}}


# TODO: test ne
# def test_simple_query_ne():
#     """Validates that the ES query is correct with modifier 'gt'
#     """
#     corequerybuilder = CoreQueryBuilder({"birthDate": ["gt1974-12-25"]})
#     result = corequerybuilder.build_simple_query(corequerybuilder.args)
#     assert result == {"range": {"birthDate": {"gt": "1974-12-25"}}}


def test_composite_query_pipe():
    """Validates that the ES query with token | is correct
    """
    corequerybuilder = CoreQueryBuilder(
        {"contained.code.coding": ["http://snomed.info/sct|324252006"]}
    )
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {
        "bool": {
            "must": [
                {"match": {"contained.code.coding.system": "http://snomed.info/sct"}},
                {
                    "simple_query_string": {
                        "query": "324252006",
                        "fields": ["contained.code.coding.code", "contained.code.coding.value",],
                    },
                },
            ]
        }
    }


def test_simple_query_string():
    """Validates that the ES query is correct"""
    corequerybuilder = CoreQueryBuilder({"birthDate": ["1974-12-25"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {"simple_query_string": {"fields": ["birthDate"], "query": "(1974-12-25)*"}}


def test_simple_query_output():
    """Validates that the output of build_simple_query
    is a dictionary
    """
    corequerybuilder = CoreQueryBuilder({"gender": ["female"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert isinstance(result, Mapping)


def test_element_query_output():
    """Validates that the output of build_simple_query
    is a dictionary
    """
    element_query = build_element_query("gender", "female")
    assert isinstance(element_query, Mapping)


##########
##
# Test CoreQueryBuilder
##
##########

def test_core_query_builder_validate():
    with raises(AssertionError, match="parameters must be a dictionary"):
        CoreQueryBuilder(["name"])


def test_core_query_builder():
    corequerybuilder = CoreQueryBuilder({})
    corequerybuilder.build_core_query()
    assert corequerybuilder.query == {"match_all": {}}


def test_core_query_builder_int():
    corequerybuilder = CoreQueryBuilder({"value": [3]})
    corequerybuilder.build_core_query()
    assert corequerybuilder.query == {"match": {"value": 3}}


def test_core_query_builder_float():
    corequerybuilder = CoreQueryBuilder({"value": [3.5]})
    corequerybuilder.build_core_query()
    assert corequerybuilder.query == {"match": {"value": 3.5}}


def test_core_query_correct_string():
    """Validates that the ES query is correct"""
    corequerybuilder = CoreQueryBuilder({"birthDate": ["1974-12-25"]})
    corequerybuilder.build_core_query()
    assert corequerybuilder.query == {
        "simple_query_string": {"fields": ["birthDate"], "query": "(1974-12-25)*"}
    }


def test_core_query_multiple():
    corequerybuilder = CoreQueryBuilder({"multiple": {"language": ["NL", "EN"]}})
    corequerybuilder.build_core_query()
    assert corequerybuilder.query == {
        "bool": {"should": [{"match": {"language": "NL"}}, {"match": {"language": "EN"}}]}
    }


def test_core_query_composite():
    corequerybuilder = CoreQueryBuilder(
        {"language": ["FR"], "multiple": {"language": ["NL", "EN"]}}
    )
    corequerybuilder.build_core_query()
    assert corequerybuilder.query == {
        "bool": {
            "must": [
                {"simple_query_string": {"query": "(FR)*", "fields": ["language"]}},
                {
                    "bool": {
                        "should": [{"match": {"language": "NL"}}, {"match": {"language": "EN"}}]
                    }
                },
            ]
        }
    }


def test_simple_query_correct_or():
    """Validates that the ES query for "OR" is correct
    """
    corequerybuilder = CoreQueryBuilder({"multiple": {"name.family": ["Donald", "Chalmers"]}})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
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
    corequerybuilder = CoreQueryBuilder({"name.family": ["Donald", "Chalmers"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
    assert result == {
        "bool": {
            "must": [
                {"simple_query_string": {"fields": ["name.family"], "query": "(Donald)*",}},
                {"simple_query_string": {"fields": ["name.family"], "query": "(Chalmers)*",}},
            ]
        }
    }


def test_composite_query_modifiers():
    """Validates that the ES query for "AND" with modifiers is correct
    """
    corequerybuilder = CoreQueryBuilder({"birthDate": ["lt20200101", "gt20100101"]})
    result = corequerybuilder.build_simple_query(corequerybuilder.args)
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
    corequerybuilder = CoreQueryBuilder({"gender": ["female"]})
    corequerybuilder.build_core_query()
    assert isinstance(corequerybuilder.query, Mapping)
