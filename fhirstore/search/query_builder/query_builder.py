import sys
import re
import json

from collections import defaultdict
from collections.abc import Mapping
import parser


from elasticsearch import Elasticsearch

number_prefix_matching = {"gt": "gt", "ge": "gte", "lt": "lt", "le": "lte"}


class Query_builder:
    def __init__(self, parameters):
        self.search_parameters = parameters
        self.sub_query = {}
        self.core_query = defaultdict(lambda: defaultdict(dict))
        self.full_query = {}

    def validate_parameters(self):
        """Validates that parameters is in dictionary form
        """
        assert isinstance(self.search_parameters, Mapping), "parameters must be a dictionary"

    def validate_sub_parameters(self):
        """Validates that sub-parameters have length 1
        """
        assert all(
            len(item) == 1 for item in self.search_parameters.items()
        ), "sub-parameters must be of length 1"

    @staticmethod
    def build_element_query(key, value):
        """Translate and parse search parameters (key, value) to an
        elasticSearch element query
        """

        element_query = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

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
        elif isinstance(value, int) or isinstance(value, float):
            element_query["match"][key] = value
        return element_query

    def build_simple_query(self, sub_param):
        """Translates a dictionary of length 1 to
        a simple elasticsearch query
        """
        self.validate_parameters()
        self.validate_sub_parameters()
        if sub_param.get("multiple"):
            multiple_key = list(sub_param["multiple"])[0]
            multiple_values = sub_param["multiple"][multiple_key]
            content = [{"match": {multiple_key: element}} for element in multiple_values]
            self.sub_query = {"bool": {"should": content}}
        else:
            key = list(sub_param)[0]
            values = sub_param[key]
            if len(values) == 1:
                self.sub_query = build_element_query(key, values[0])
            else:
                content = [build_element_query(key, element) for element in values]
                self.sub_query = {"bool": {"must": content}}

    def build_core_query(self):
        """Translates a full JSON body to
        the core of an elasticsearch query
        """
        validate_parameters(self)

        if len(self.search_parameters) == 0:
            self.core_query = {"match_all": {}}
        elif len(search_parameters) == 1:
            self.core_query = build_simple_query(search_parameters)
        elif len(search_parameters) > 1:
            inter_query = [
                build_simple_query({sub_key: sub_value})
                for sub_key, sub_value in search_parameters.items()
            ]
            self.core_query = {"bool": {"must": inter_query}}
