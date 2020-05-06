from fhirstore.search import SearchArguments


class Bundle:
    def __init__(self):
        self.content = {}

    def initiate_bundle(self, formatting_args, resource_type):
        self.content = {"resource_type": "Bundle", "total": 0}

        if formatting_args["is_summary_count"]:
            self.content["tag"] = {"code": "SUBSETTED"}
        else:
            self.content["entry"] = []

            if formatting_args["elements"] or formatting_args["summary"]:
                self.content["tag"] = {"code": "SUBSETTED"}

    def fill(self, formatting_args, hits):
        if hits != {}:
            if formatting_args["is_summary_count"]:
                self.content["total"] += hits["count"]
            else:
                for h in hits["hits"]["hits"]:
                    self.content["entry"].append(
                        {"resource": h["_source"], "search": {"mode": "match"}}
                    )
                self.content["total"] += hits["hits"]["total"]["value"]

    def complete(self, formatting_args, new_bundle):
        self.content["total"] += new_bundle.content["total"]
        if not formatting_args["is_summary_count"]:
            self.content["entry"].extend(new_bundle.content["entry"])

    def append_bundle(self, formatting_args, included_hits):
        if formatting_args["include"] and "hits" in included_hits:
            for h in included_hits["hits"]["hits"]:
                self.content["entry"].append(
                    {"resource": h["_source"], "search": {"mode": "include"}}
                )

    def fill_error(
        self, severity="error", code="invalid", details=None, diagnostic=None
    ):
        self.content["resource_type"] = "OperationOutcome"
        self.content.pop("entry")
        self.content.pop("total")
        self.content["issue"] = {"severity": severity, "code": code}

        if details:
            self.content["issue"]["details"] = details
        if diagnostic:
            self.content["issue"]["diagnostic"] = diagnostic
