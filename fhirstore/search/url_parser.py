from collections import defaultdict
import re

# Format the url args to a dict
def url_to_dict(url_args):
    search_args = {key: url_args.getlist(key) for key in url_args.keys()}
    return search_args


# Parse commas in a (key,value)
def parse_comma(key, value):
    has_comma = "," in value
    if has_comma:
        return "multiple", {key: value.split(",")}
    else:
        return key, [value]
    #     search_args["multiple"] = {key: value.split(",")}
    # else:
    #     search_args[key] = [value]
    # return element_


# Process all the dict for possible commas
def process_params(url_args):
    search_args = url_to_dict(url_args)
    processed_params = defaultdict(list)
    if search_args == {}:
        processed_params = {}

    for key, value in search_args.items():
        if len(value) == 1:
            parsed_key, parsed_value = parse_comma(key, value[0])
            processed_params.update({parsed_key: parsed_value})
        else:
            for element in value:
                parsed_key, parsed_value = parse_comma(key, element)
                if parsed_key=="multiple":
                    processed_params[parsed_key] = parsed_value
# /                    processed_params["multiple"][sub_key].append(parsed_dict["multiple"][sub_key])
                else:
                    processed_params[parsed_key].append(parsed_value[0])
    return processed_params


class URL_Parser:
    def __init__(self, url_args: dict, resource_type):
        # core args
        self.resource_type = resource_type
        self.core_args = {}
        self.sort = None

        # formatting args
        self.elements = None
        self.include = False

        # meta args
        self.summary = False
        self.is_summary_count = False
        self.offset = 0
        self.result_size = 100
        
        self.processed_params = process_params(url_args)


    def sort_params(self):
        has_sort = None
        if "_sort" in self.processed_params:
            has_sort = self.processed_params["_sort"]
        elif "multiple" in self.processed_params:
            has_sort = self.processed_params["multiple"].get("_sort", None)
        # if there is a sorting argument, process it to handle a change of sorting order
        if has_sort:
            sorting_params = []
            for argument in has_sort:
                # find a "-" before the argument. It indicates a sorting order different from default
                has_minus = re.search(r"^-(.*)", argument)
                if has_minus:
                    # if the argument is -score, sort by ascending order (i.e. ascending relevance)
                    if has_minus.group(1) == "_score":
                        sorting_params.append({has_minus.group(1): {"order": "asc"}})
                    # for any other argument, sort by descending order
                    else:
                        sorting_params.append({has_minus.group(1): {"order": "desc"}})
                # if there is no "-", use order defaults defined in elasticsearch
                else:
                    sorting_params.append(argument)
            self.sort = sorting_params

    def include_params(self):
        if "_include" in self.processed_params:
            self.include = [re.search(r"(.*):(.*)", self.processed_params["_include"][0]).group(2)]

        elif (
            "multiple" in self.processed_params and "_include" in self.processed_params["multiple"]
        ):
            attributes = self.processed_params["multiple"].get("_include", None)
            self.include = [re.search(r"(.*):(.*)", elem).group(2) for elem in attributes]

    # Removes result formatting arguments from the search fields
    def clean_params(self):
        if "multiple" in self.processed_params:
            self.processed_params["multiple"].pop("_element", None)
            self.processed_params["multiple"].pop("_sort", None)
            self.processed_params["multiple"].pop("_include", None)
            self.processed_params["multiple"].pop("_type", None)

        if self.processed_params.get("multiple") == {}:
            self.processed_params = {}

        self.processed_params.pop("_summary", None)
        self.processed_params.pop("_element", None)
        self.processed_params.pop("_sort", None)
        self.processed_params.pop("_include", None)
        self.processed_params.pop("_count", None)

        # Probably not the best way to proceed
        self.core_args = self.processed_params

    def process_params(self):
        parsed_params = self.processed_params
        # TODO: handle offset
        self.sort_params()
        self.include_params()
        has_result_size = parsed_params.get("_count", None)
        self.result_size = int(has_result_size[0]) if has_result_size else 100
        self.summary = "_summary" in parsed_params
        self.is_summary_count = (
            "_summary" in parsed_params and parsed_params["_summary"][0] == "count"
        )

        if "_summary" in parsed_params and parsed_params["_summary"][0] == "text":
            self.elements = ["text", "id", "meta"]
        elif "_element" in parsed_params:
            self.elements = parsed_params["_element"]
        elif "multiple" in parsed_params:
            self.elements = parsed_params["multiple"].get("_element", None)

        self.clean_params()
