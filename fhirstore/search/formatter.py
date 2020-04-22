from fhirstore.search.url_parser import URL_Parser
import elasticsearch
import logging


class Formatter:
    def __init__(self):
        self.hits = {}
        self.included_hits = {}
        self.bundle = {}

    def parse_format_arguments(self, url_args, resource_type):
        parsed_args = URL_Parser(url_args, resource_type)
        parsed_args.process_params()
        self.resource_type = parsed_args.resource_type
        self.elements = parsed_args.elements
        self.include = parsed_args.include
        self.summary = parsed_args.summary
        self.is_summary_count = parsed_args.is_summary_count

    def initiate_bundle(self, url_args, resource_type):
        self.parse_format_arguments(url_args, resource_type)
        if self.is_summary_count:
            self.bundle = {
                "resource_type": "Bundle",
                "tag": {"code": "SUBSETTED"},
                "total": 0,
            }

        else:
            self.bundle = {
                "resource_type": "Bundle",
                "entry": [],
                "total": 0,
            }

            if self.elements or self.summary:
                self.bundle["tag"] = {"code": "SUBSETTED"}

    # fill a bundle that has already been initiated, with ES hits
    def fill_bundle(self, hits):
        if hits != {}:
            if self.is_summary_count:
                self.bundle["total"] += hits["count"]
            else:
                for h in hits["hits"]["hits"]:
                    self.bundle["entry"].append(
                        {"resource": h["_source"], "search": {"mode": "match"}}
                    )
                self.bundle["total"] += hits["hits"]["total"]["value"]

    # Complete an existing bundle with another bundle in a search mode
    def complete_bundle(self, new_bundle):
        self.bundle["total"] += new_bundle["total"]
        if not self.is_summary_count:
            self.bundle["entry"].extend(new_bundle["entry"])

    # Adds included results to an existing bundle in an include mode
    def add_included_bundle(self, included_hits):
        if self.include and "hits" in included_hits:
            for h in included_hits["hits"]["hits"]:
                self.bundle["entry"].append(
                    {"resource": h["_source"], "search": {"mode": "include"}}
                )
