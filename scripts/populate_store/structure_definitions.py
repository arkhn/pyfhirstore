import sys


def upload_structure_definitions(bundle_files):

    resource_types = ["StructureDefinition", "ConceptMap"]

    for bundle_file in bundle_files:
        if bundle_file.endswith(".json"):
            with open(bundle_file) as f:
                bundle = json.load(f)

            assert (
                "resourceType" in data and bundle["resourceType"] == "Bundle"
            ), f"{bundle_file} must be a FHIR Bundle resource"

            for entry in bundle["entry"]:
                if "resource" not in entry:
                    raise Exception("Bundle entry is missing a resource.")

                if entry["resource"]["resourceType"] not in resource_types:
                    continue

                resource_type = entry["resource"]["resourceType"]
                response = requests.post(f"{api}/{resource_type}", json=entry["resource"])
                if response.status_code != 200:
                    print(response.text)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("USAGE: python structure_definitions.py <bundleFile.json> [<bundle2.json>...].")
        sys.exit(1)

    upload_structure_definitions(sys.argv[1:])
