from fhirstore.search import SearchArguments


class Bundle:
    def __init__(self):
        self.content = {"resource_type": "Bundle", "total": 0, "entry": []}

    def fill(self, hits, formatting_args):
        if formatting_args["is_summary_count"] == True:
            self.content["tag"] = {"code": "SUBSETTED"}
            self.content.pop("entry")
            if len(hits):
                self.content["total"] += hits["count"]

        elif formatting_args["is_summary_count"] == False and len(hits):
            for h in hits["hits"]["hits"]:
                self.content["entry"].append(
                    {"resource": h["_source"], "search": {"mode": "match"}}
                )
            self.content["total"] += hits["hits"]["total"]["value"]
        if formatting_args["elements"] or formatting_args["summary"]:
            self.content["tag"] = {"code": "SUBSETTED"}

    def complete(self, new_bundle, formatting_args):
        if new_bundle.content["resource_type"] == "OperationOutcome":
            self.content = new_bundle.content
        else:
            self.content["total"] += new_bundle.content["total"]
            if not formatting_args["is_summary_count"]:
                self.content["entry"].extend(new_bundle.content["entry"])

    def append(self, included_hits, formatting_args):
        if formatting_args["include"] and "hits" in included_hits:
            for h in included_hits["hits"]["hits"]:
                self.content["entry"].append(
                    {"resource": h["_source"], "search": {"mode": "include"}}
                )

    def fill_error(self, severity="error", code="invalid", details=None, diagnostic=None):
        self.content["resource_type"] = "OperationOutcome"
        self.content["issue"] = {"severity": severity, "code": code}
        self.content.pop("total", None)
        self.content.pop("entry", None)
        self.content.pop("tag", None)

        if details:
            self.content["issue"]["details"] = details
        if diagnostic:
            self.content["issue"]["diagnostic"] = diagnostic
