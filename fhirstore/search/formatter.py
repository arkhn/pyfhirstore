from fhirstore.search.urlparser import SearchArguments
import elasticsearch
import logging


class Bundle:
    def __init__(self):
        self.bundle = {}


class Formatter:
    def __init__(self):
        self.hits = {}
        self.included_hits = {}

    def initiate_bundle(self, parsed_args: SearchArguments, resource_type, bundle: Bundle):
        if parsed_args.is_summary_count:
            bundle = {"resource_type": "Bundle", "tag": {"code": "SUBSETTED"}, "total": 0}
        else:
            bundle = {"resource_type": "Bundle", "entry": [], "total": 0}

            if parsed_args.elements or parsed_args.summary:
                bundle["tag"] = {"code": "SUBSETTED"}
        return bundle

    # fill a bundle that has already been initiated, with ES hits
    def fill_bundle(self, parsed_args: SearchArguments, bundle: Bundle, hits):
        if hits != {}:
            if parsed_args.is_summary_count:
                bundle["total"] += hits["count"]
            else:
                for h in hits["hits"]["hits"]:
                    bundle["entry"].append({"resource": h["_source"], "search": {"mode": "match"}})
                bundle["total"] += hits["hits"]["total"]["value"]
        return bundle

    # Complete an existing bundle with another bundle in a search mode
    def complete_bundle(
        self, parsed_args: SearchArguments, current_bundle: Bundle, new_bundle: Bundle
    ):
        current_bundle["total"] += new_bundle["total"]
        if not parsed_args.is_summary_count:
            current_bundle["entry"].extend(new_bundle["entry"])
        return current_bundle

    # Adds included results to an existing bundle in an include mode
    def add_included_bundle(
        self, parsed_args: SearchArguments, current_bundle: Bundle, included_hits
    ):
        if parsed_args.include and "hits" in included_hits:
            for h in included_hits["hits"]["hits"]:
                current_bundle["entry"].append(
                    {"resource": h["_source"], "search": {"mode": "include"}}
                )
        return current_bundle
