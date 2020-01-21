import sys
import re
import json

from collections import defaultdict
from elasticsearch import Elasticsearch


def element_search(params):
    """ Translates the a single JSON body search to an 
    elasticSearch query
    """

    # translate the prefix from fhir standard to elastic search standard
    number_prefix_matching = {"gt": "gt", "ge": "gte", "lt": "lt", "le": "lte"}

    parsed_params = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for key, value in params.items():
        for v in value:
            print(key)
            print(params[key])
            # TODO: handle sa eb ap ne prefixes
            n = re.search(r"^(gt|lt|ge|le)([0-9].*)$", v)
            m = re.search(
                r"^(.*):(contains|exact|above|below|not|in|not-in|of-type)$", key
            )
            p = re.search(r"^(eq)([0-9].*)$", v)
            if m:
                if m.group(2) == "contains":
                    parsed_params["query_string"]["query"] = f"*{v}*"
                    parsed_params["query_string"]["default_field"] = m.group(1)

                if m.group(2) == "exact":
                    parsed_params["query_string"]["query"] = v
                    parsed_params["query_string"]["fields"] = [m.group(1)]

                # Term does not go in depth
                if m.group(2) == "not":
                    parsed_params["bool"]["must_not"]["match"][m.group(1)] = f"{v}"
                # TODO: improve method for FHIR tokens
                if m.group(2) == "not-in":
                    parsed_params["simple_query_string"]["query"] = f"-{v}"
                    parsed_params["simple_query_string"]["fields"] = [m.group(1)]
                # TODO: improve method for FHIR tokens
                if m.group(2) == "in":
                    parsed_params["query_string"]["query"] = f"{v}"

                # URI examples
                # # Above method for URI, not used
                # if m.group(2) == "above":
                #     parsed_params = defaultdict(lambda: defaultdict(dict))
                #     parsed_params["regexp"][f"{m.group(1)}"]["value"] = f"\b[{value}].*"

                # For URI
                if m.group(2) == "below":
                    parsed_params["simple_query_string"]["query"] = f"({v})*"
                    parsed_params["simple_query_string"]["fields"] = [m.group(1)]
            elif n:
                parsed_params["range"][key] = {
                    number_prefix_matching[n.group(1)]: n.group(2)
                }
            elif p:
                parsed_params["match"][key] = p.group(2)

            elif not (m) or not (n):
                parsed_params["match"][key] = v

    return parsed_params
