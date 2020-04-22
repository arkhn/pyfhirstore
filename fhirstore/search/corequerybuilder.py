import sys
import re
import json

from collections import defaultdict
from collections.abc import Mapping

from elasticsearch import Elasticsearch

number_prefix_matching = {"gt": "gt", "ge": "gte", "lt": "lt", "le": "lte"}


def build_element_query(key, value):
    """Translate and parse search parameters (key, value) to an
    elasticSearch element query
    """
    element_query = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    if isinstance(value, int) or isinstance(value, float):
        element_query["match"][key] = value

    else : 
        numeric_prefix = re.search(r"^(gt|lt|ge|le)([0-9].*)$", f"{value}")
        eq_prefix = re.search(r"^(eq)([0-9].*)$", f"{value}")
        special_prefix = re.search(r"^(ne|sa|eb|ap)([0-9].*)$", f"{value}")
        pipe_suffix = re.search(r"(.*)\|(.*)", value)

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
                element_query["simple_query_string"]["fields"] = [f"{string_field}.identifier.value"]

        elif numeric_prefix:
            element_query["range"][key] = {
                number_prefix_matching[numeric_prefix.group(1)]: numeric_prefix.group(2)
            }
        elif eq_prefix:
            element_query["match"][key] = eq_prefix.group(2)

        elif special_prefix:
            if special_prefix.group(1) == "ne":
                element_query["simple_query_string"]["query"] = f"-{special_prefix.group(2)}"
                element_query["simple_query_string"]["fields"] = [key]

        elif pipe_suffix:
            system, code = re.split(r"\|", value)
            element_query["bool"]["must"] = [{"match": {f"{key}.system": system}}]
            element_query["bool"]["must"].append(
                {"simple_query_string": {"query": code, "fields": [f"{key}.code", f"{key}.value"],}}
            )

        elif isinstance(value, str):
            element_query["simple_query_string"]["query"] = f"({value})*"
            element_query["simple_query_string"]["fields"] = [key]
        
    return element_query


# This class transforms a dictionary of fields, and values into a dictionary
# which can be the body of an elasticsearch query


class CoreQueryBuilder:
    def __init__(self, core_args):
        self.args = core_args
        self.query = {}

    def validate_parameters(self):
        """Validates that parameters is in dictionary form
        """
        assert isinstance(self.args, Mapping), "parameters must be a dictionary"


    def build_simple_query(self, dict_args):
        """Translates a dictionary of length 1 to
        a simple elasticsearch query
        """
        self.validate_parameters()

        if dict_args.get("multiple"):
            multiple_key = list(dict_args["multiple"])[0]
            multiple_values = dict_args["multiple"][multiple_key]
            content = [{"match": {multiple_key: element}} for element in multiple_values]
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

    def build_core_query(self):
        """Translates a full JSON body to
        the core of an elasticsearch query
        """
        self.validate_parameters

        if self.args == {}:
            self.query = {"match_all": {}}
        elif len(self.args) == 1:
            self.query = self.build_simple_query(self.args)
        elif len(self.args) > 1:
            inter_query = [
                self.build_simple_query({sub_key: self.args[sub_key]}) for sub_key in self.args
            ]
            self.query = {"bool": {"must": inter_query}}
        return self.query
