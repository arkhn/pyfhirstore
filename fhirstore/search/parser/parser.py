from collections import defaultdict
from errors.operation_outcome import OperationOutcome
import elasticsearch
import re
from preprocess import parse_params


class Parser:
    def __init__(self,url_args):
        self.processed_params = process_params(url_args)
        self.query_params = {}
        self.result_size = 100
        self.elements = None
        self.is_summary_count = False
        self.offset = 0
        self.sort = None
        self.include = False


    # Parse commas in a (key,value)
    @staticmethod
    def parse_comma(key, value):
        search_args = {}
        has_comma = "," in value
        if has_comma:
            search_args["multiple"] = {key: value.split(",")}
        else:
            search_args[key] = [value]
        return search_args
    
    # Process all the dict for possible commas
    @staticmethod
    def process_params(url_args):
        processed_params = defaultdict(list)
        
        if url_args == {}:
            processed_params = {}

        for key, value in url_args.items():
            if len(value) == 1:
                element_key, parsed_element = parse_comma(key, value[0])
                processed_params[element_key] = parsed_element
            else:
                for element in value:
                    element_key, parsed_element = parse_comma(key, element)
                    if element_key == "multiple":
                        processed_params[element_key] = parsed_element
                    else:
                        processed_params[element_key].append(parsed_element[0])
        return processed_params

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
            self.include = re.search(r"(.*):(.*)", self.processed_params["_include"][0]).group(2)

        elif "multiple" in self.processed_params and "_include" in self.processed_params["multiple"]:
            attributes = self.processed_params["multiple"].get("_include", None)
            self.include = [re.search(r"(.*):(.*)", elem).group(2) for elem in attributes]

    # Removes result formatting arguments from the search fields
    def clean_params(self):
        if "multiple" in self.processed_params:
            self.processed_params["multiple"].pop("_element", None)
            self.processed_params["multiple"].pop("_sort", None)
            self.processed_params["multiple"].pop("_include", None)

        if self.processed_params.get("multiple") == {}:
            self.processed_params = {}

        self.processed_params.pop("_summary", None)
        self.processed_params.pop("_element", None)
        self.processed_params.pop("_sort", None)
        self.processed_params.pop("_include", None)
        
        # Probably not the best way to proceed
        self.query_params = self.processed_params

    def process_params(self):
        parsed_params = self.processed_params
        # TODO: handle offset
        self.sort_params()
        self.include_params()
        has_result_size = parsed_params.pop("_count", None)
        self.result_size = int(has_result_size[0]) if has_result_size
        self.is_summary_count = "_summary" in parsed_params and parsed_params["_summary"][0] == "count"

        if "_summary" in parsed_params and parsed_params["_summary"][0] == "text":
            self.elements = ["text", "id", "meta"]
        elif "_element" in parsed_params:
            self.elements = parsed_params["_element"]
        elif "multiple" in parsed_params:
            self.elements = parsed_params["multiple"].get("_element", None)

        self.clean_params()
