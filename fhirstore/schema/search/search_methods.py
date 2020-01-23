import sys
import re
import json

from collections import defaultdict
from elasticsearch import Elasticsearch

number_prefix_matching = {"gt": "gt", "ge": "gte", "lt": "lt", "le": "lte"}

def build_element_query(key, value):
    """ Translates the a single JSON body search to an
    elasticSearch query
    """

    element_query = defaultdict(lambda: defaultdict(dict))

    numeric_modif = re.search(r"^(gt|lt|ge|le)([0-9].*)$", f"{value}")
    eq_modif = re.search(r"^(eq)([0-9].*)$", f"{value}")
    str_modif = re.search(
        r"^(.*):(contains|exact|above|below|not|in|not-in|of-type)$", key
    )
    if str_modif:
        if str_modif.group(2) == "contains":
            element_query["query_string"]["query"] = f"*{value}*"
            element_query["query_string"]["default_field"] = str_modif.group(1)

        if str_modif.group(2) == "exact":
            element_query["query_string"]["query"] = value
            element_query["query_string"]["fields"] = [str_modif.group(1)]

        if str_modif.group(2) == "not":
            element_query["bool"]["must_not"]["match"][str_modif.group(1)] = f"{value}"

        if str_modif.group(2) == "not-in":
            element_query["simple_query_string"]["query"] = f"-{value}"
            element_query["simple_query_string"]["fields"] = [str_modif.group(1)]

        if str_modif.group(2) == "in":
            element_query["query_string"]["query"] = f"{value}"

        if str_modif.group(2) == "below":
            element_query["simple_query_string"]["query"] = f"({value})*"
            element_query["simple_query_string"]["fields"] = [str_modif.group(1)]

    elif numeric_modif:
        element_query["range"][key] = {
            number_prefix_matching[numeric_modif.group(1)]: numeric_modif.group(2)
        }
    elif eq_modif:
        element_query["match"][key] = eq_modif.group(2)
    else:
        element_query["match"][key] = value

    return element_query


def build_simple_query(sub_param):
    """Accepts a dictionary of length 1
    """
    content = []
    sub_query = {}
    if bool(sub_param.get("multiple")):
        multiple_key = list(sub_param["multiple"])[0]
        multiple_values = sub_param["multiple"][multiple_key]
        for element in multiple_values:
            content.append({"match": {multiple_key: element}})
        sub_query = {"bool": {"should": content}}
    else:
        key = list(sub_param)[0]
        values = sub_param[key]
        if len(values) == 1:
            sub_query = build_element_query(key, values[0])
        else:
            for element in values:
                content.append({"match": {key: element}})
            sub_query = {"bool": {"must": content}}
    return sub_query
