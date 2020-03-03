import sys
import re
import json

from collections import defaultdict
from collections.abc import Mapping


from elasticsearch import Elasticsearch

number_prefix_matching = {"gt": "gt", "ge": "gte", "lt": "lt", "le": "lte"}


def validate_parameters(params):
    """Validates that parameters is in dictionary form
    """
    assert isinstance(params, Mapping), "parameters must be a dictionary"


def validate_sub_parameters(sub_param):
    """Validates that sub-parameters have length 1
    """
    assert len(sub_param) == 1, "sub-parameters must be of length 1"


def build_element_query(key, value):
    """Translate and parse search parameters (key, value) to an
    elasticSearch element query
    """

    element_query = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    numeric_prefix = re.search(r"^(gt|lt|ge|le)([0-9].*)$", f"{value}")
    eq_prefix = re.search(r"^(eq)([0-9].*)$", f"{value}")
    special_prefix = re.search(r"^(ne|sa|eb|ap)([0-9].*)$", f"{value}")

    string_modif = re.search(
        r"^(.*):(contains|exact|above|below|not|in|not-in|of-type|identifier)$", key
    )
    if string_modif:
        string_modifier = string_modif.group(2)
        string_field = string_modif.group(1)
        if string_modifier == "contains":
            element_query["query_string"]["query"] = f"*{value}*"
            element_query["query_string"]["default_field"] = string_field

        elif string_modifier == "exact":
            element_query["query_string"]["query"] = value
            element_query["query_string"]["fields"] = [string_field]

        elif string_modifier == "not":
            element_query["bool"]["must_not"]["match"][string_field] = f"{value}"

        elif string_modifier == "not-in":
            element_query["simple_query_string"]["query"] = f"-{value}"
            element_query["simple_query_string"]["fields"] = [string_field]

        elif string_modifier == "in":
            element_query["query_string"]["query"] = f"{value}"

        elif string_modifier == "below":
            element_query["simple_query_string"]["query"] = f"({value})*"
            element_query["simple_query_string"]["fields"] = [string_field]
        elif string_modifier == "identifier":
            element_query["simple_query_string"]["query"] = f"{value}"
            element_query["simple_query_string"]["fields"] = [
                f"{string_field}.identifier.value"
            ]

    elif numeric_prefix:
        element_query["range"][key] = {
            number_prefix_matching[numeric_prefix.group(1)]: numeric_prefix.group(2)
        }
    elif eq_prefix:
        element_query["match"][key] = eq_prefix.group(2)

    elif special_prefix:
        if special_prefix.group(1) == "ne":
            element_query["simple_query_string"][
                "query"
            ] = f"-{special_prefix.group(2)}"
            element_query["simple_query_string"]["fields"] = [key]

    elif isinstance(value, str):
        element_query["simple_query_string"]["query"] = f"({value})*"
        element_query["simple_query_string"]["fields"] = [key]
    elif isinstance(value, int) or isinstance(value, float):
        element_query["match"][key] = value
    return element_query


def build_simple_query(sub_param):
    """Translates a dictionary of length 1 to
    a simple elasticsearch query
    """
    validate_parameters(sub_param)
    validate_sub_parameters(sub_param)
    sub_query = {}
    if sub_param.get("multiple"):
        multiple_key = list(sub_param["multiple"])[0]
        multiple_values = sub_param["multiple"][multiple_key]
        content = [{"match": {multiple_key: element}} for element in multiple_values]
        sub_query = {"bool": {"should": content}}
    else:
        key = list(sub_param)[0]
        values = sub_param[key]
        if len(values) == 1:
            sub_query = build_element_query(key, values[0])
        else:
            content = [build_element_query(key, element) for element in values]
            sub_query = {"bool": {"must": content}}
    return sub_query


def build_core_query(params):
    """Translates a full JSON body to
    the core of an elasticsearch query
    """
    validate_parameters(params)

    core_query = defaultdict(lambda: defaultdict(dict))
    if len(params) == 0:
        core_query = {"match_all": {}}
    elif len(params) == 1:
        core_query = build_simple_query(params)
    elif len(params) > 1:
        inter_query = [
            build_simple_query({sub_key: sub_value})
            for sub_key, sub_value in params.items()
        ]
        core_query = {"bool": {"must": inter_query}}

    return core_query
