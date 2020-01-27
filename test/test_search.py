from pytest import raises
from fhirstore import BadRequestError, NotFoundError
from fhirstore.search.search_methods import build_element_query, build_simple_query
from collections import Mapping


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
