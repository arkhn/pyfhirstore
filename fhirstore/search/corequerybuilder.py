import sys
import re
import json

from collections import defaultdict
from collections.abc import Mapping
from fhirstore.search import SearchArguments, ReverseChain

number_prefix_matching = {"gt": "gt", "ge": "gte", "lt": "lt", "le": "lte"}

# TODO handle sa|eb|ap|ne
def is_numeric_type(argument):
    is_numeric = re.search(
        r"^([0]|[-+]?[1-9][0-9]*)$|^(-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?)$|^(([0-9]([0-9]([0-9][1-9]|[1-9]0)|[1-9]00)|[1-9]000)(-(0[1-9]|1[0-2])(-(0[1-9]|[1-2][0-9]|3[0-1])(T([01][0-9]|2[0-3]):[0-5][0-9]:([0-5][0-9]|60)(\.[0-9]+)?(Z|(\+|-)((0[0-9]|1[0-3]):[0-5][0-9]|14:00)))?)?)?)$",
        argument,
    )
    return is_numeric


def check_prefix(value):
    num_prefix = re.search(r"^(ne|eq|gt|lt|ge|le)(.*)$", value)
    if num_prefix and is_numeric_type(num_prefix.group(2)):
        prefix = num_prefix.group(1)
        argument = num_prefix.group(2)
        return prefix, argument
    else:
        return None, value


def build_element_query(key, value):
    """Translate and parse search parameters (key, value) to an
    elasticSearch element query
    """
    element_query = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    if isinstance(value, int) or isinstance(value, float):
        element_query["match"][key] = value

    else:
        prefix, value = check_prefix(value)
        pipe_suffix = re.search(r"(.*)\|(.*)", value)

        string_modif = re.search(
            r"^(.*):(contains|exact|above|below|not|in|not-in|of-type|identifier)$", key
        )
        if key == "_text" or key == "_content":
            element_query["simple_query_string"]["query"] = value

        elif string_modif:
            string_modifier = string_modif.group(2)
            string_field = string_modif.group(1)
            if string_modifier == "contains":
                element_query["query_string"]["query"] = f"*{value}*"
                element_query["query_string"]["fields"] = [string_field]

            elif string_modifier == "exact":
                element_query["simple_query_string"]["query"] = f'"{value}"'
                element_query["simple_query_string"]["fields"] = [string_field]
                element_query["simple_query_string"]["flags"] = "PHRASE"

            elif string_modifier == "not":
                element_query["simple_query_string"]["query"] = f"-{value}"
                element_query["simple_query_string"]["fields"] = [string_field]

            elif string_modifier == "not-in":
                element_query["simple_query_string"]["query"] = f"-{value}"
                element_query["simple_query_string"]["fields"] = [string_field]

            elif string_modifier == "in":
                element_query["simple_query_string"]["query"] = f'"{value}"'
                element_query["simple_query_string"]["fields"] = [string_field]
                element_query["simple_query_string"]["flags"] = "PHRASE"

            elif string_modifier == "below":
                element_query["simple_query_string"]["query"] = f"({value})*"
                element_query["simple_query_string"]["fields"] = [string_field]

            elif string_modifier == "identifier":
                element_query["simple_query_string"]["query"] = f'"{value}"'
                element_query["simple_query_string"]["fields"] = [
                    f"{string_field}.identifier.value"
                ]
                element_query["simple_query_string"]["flags"] = "PHRASE"

        elif prefix:
            if prefix in number_prefix_matching:
                element_query["range"][key] = {number_prefix_matching[prefix]: value}
            elif prefix == "eq":
                element_query["match"][key] = value
            elif prefix == "ne":
                element_query["simple_query_string"]["query"] = f"-{value}"
                element_query["simple_query_string"]["fields"] = [key]

        elif pipe_suffix:
            system, code = re.split(r"\|", value)
            element_query["bool"]["must"] = [
                {
                    "simple_query_string": {
                        "query": f'"{system}"',
                        "fields": [f"{key}.system"],
                        "flags": "PHRASE",
                    }
                }
            ]
            element_query["bool"]["must"].append(
                {
                    "simple_query_string": {
                        "query": f'"{code}"',
                        "fields": [f"{key}.code", f"{key}.value"],
                        "flags": "PHRASE",
                    }
                }
            )

        elif isinstance(value, str):
            element_query["simple_query_string"]["query"] = f'"{value}"'
            element_query["simple_query_string"]["fields"] = [key]
            element_query["simple_query_string"]["flags"] = "PHRASE"

    return element_query


def validate_parameters(core_args):
    """Validates that parameters is in dictionary form
    """
    assert isinstance(core_args, Mapping), "parameters must be a dictionary"


def build_simple_query(dict_args):
    """Translates a dictionary of length 1 to
    a simple elasticsearch query
    """
    validate_parameters(dict_args)

    if dict_args.get("multiple"):
        multiple_key = list(dict_args["multiple"])[0]
        multiple_values = dict_args["multiple"][multiple_key]
        content = [build_element_query(multiple_key, element) for element in multiple_values]
        sub_query = {"bool": {"should": content}}
    else:
        key = list(dict_args)[0]
        values = dict_args.get(key)
        if len(values) == 1:
            sub_query = build_element_query(key, values[0])
        else:
            content = [build_element_query(key, element) for element in values]
            sub_query = {"bool": {"must": content}}
    return sub_query


def build_core_query(core_args):
    """Translates a full JSON body to
    the core of an elasticsearch query
    """
    validate_parameters(core_args)

    if core_args == {}:
        query = {"match_all": {}}
    elif len(core_args) == 1:
        query = build_simple_query(core_args)
    elif len(core_args) > 1:
        inter_query = [build_simple_query({sub_key: core_args[sub_key]}) for sub_key in core_args]
        query = {"bool": {"must": inter_query}}
    return query
