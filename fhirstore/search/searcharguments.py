from collections import defaultdict
import re


def url_to_dict(url_args):
    search_args = {key: url_args.getlist(key) for key in url_args.keys()}
    return search_args


def parse_comma(key, value):
    if "," in value:
        return "multiple", {key: value.split(",")}
    else:
        return key, [value]


def pre_process_params(url_args):
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
                if parsed_key == "multiple":
                    processed_params[parsed_key] = parsed_value
                # /                    processed_params["multiple"][sub_key].append(parsed_dict["multiple"][sub_key])
                else:
                    processed_params[parsed_key].append(parsed_value[0])
    return processed_params


class SearchArguments:
    def __init__(self):
        self.core_args = {}
        self.sort = None

        self.formatting_args = {
            "include": False,
            "is_summary_count": False,
            "summary": False,
            "elements": None,
        }
        self.meta_args = {"offset": 0, "result_size": 100}

    def sort_params(self, args):
        has_sort = None
        if "_sort" in args:
            has_sort = args["_sort"]
        elif "multiple" in args:
            has_sort = args["multiple"].get("_sort", None)
        # if there is a sorting argument, process it to handle a change of sorting order
        if has_sort:
            sorting_params = []
            for argument in has_sort:
                # find a "-" before the argument. It indicates a sorting order different from default
                has_minus = argument.startswith("-")
                if has_minus:
                    # if the argument is -score, sort by ascending order (i.e. ascending relevance)
                    if has_minus.group(1) == "_score":
                        sorting_params.append({argument[1:]: {"order": "asc"}})
                    # for any other argument, sort by descending order
                    else:
                        sorting_params.append({argument[1:]: {"order": "desc"}})
                # if there is no "-", use order defaults defined in elasticsearch
                else:
                    sorting_params.append(argument)
            return sorting_params

    def include_params(self, args):
        include = None
        if "_include" in args:
            include = [re.search(r"(.*):(.*)", args["_include"][0]).group(2)]

        elif "multiple" in args and "_include" in args["multiple"]:
            attributes = args["multiple"].get("_include", None)
            include = [re.search(r"(.*):(.*)", elem).group(2) for elem in attributes]
        return include

    # def has_params(self,args):
    #     has = None
    #     for key, value in args.items():
    #         has = re.search(r"^(_has):(.*)", key)
    #         .group(2) if re.search(r"^(_has):(.*)", key) else None
    #     return has

    def clean_params(self, args):
        if "multiple" in args:
            args["multiple"].pop("_element", None)
            args["multiple"].pop("_sort", None)
            args["multiple"].pop("_include", None)
            args["multiple"].pop("_type", None)

        if args.get("multiple") == {}:
            args.pop("multiple")

        args.pop("_summary", None)
        args.pop("_element", None)
        args.pop("_sort", None)
        args.pop("_include", None)
        args.pop("_count", None)

        return args

    def parse_arguments(self, url_args, resource_type):
        self.resource_type = resource_type
        args = pre_process_params(url_args)
        self.sort = self.sort_params(args)
        self.formatting_args["include"] = self.include_params(args)
        has_result_size = args.get("_count", None)
        self.meta_args["result_size"] = (
            int(has_result_size[0]) if has_result_size else 100
        )
        self.formatting_args["summary"] = "_summary" in args and args["_summary"] != [
            "false"
        ]
        self.formatting_args["is_summary_count"] = (
            "_summary" in args and args["_summary"][0] == "count"
        )

        if "_summary" in args and args["_summary"][0] == "text":
            self.formatting_args["elements"] = ["text", "id", "meta"]
        elif "_element" in args:
            self.formatting_args["elements"] = args["_element"]
        elif "multiple" in args:
            self.formatting_args["elements"] = args["multiple"].get("_element", None)

        self.core_args = self.clean_params(args)
