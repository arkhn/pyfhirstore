import requests
from bs4 import BeautifulSoup
import json


def download_code_systems():
    base_url = "https://www.hl7.org/fhir"
    code_systems_url = "https://www.hl7.org/fhir/terminologies-systems.html"

    api = "https://fhir.staging.arkhn.org/api/CodeSystem"

    # Get html from hl7.org
    response = requests.get(code_systems_url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Find hypertext links in table
    links = soup.find("div", {"id": "tabs-fhir"}).findAll("a")
    # Remove normative flags
    links = [link for link in links if link.get("class") != ["normative-flag"]]

    for link in links:
        url = link.get("href").replace(".html", ".json")
        code_system = requests.get(f"{base_url}/{url}")
        yield code_system


def upload_code_systems():
    for code_system in download_code_systems():
        response = requests.post(f"{api}", json=code_system.json())
        if response.status_code != 200:
            print("Error while uploading code system:", response.text)


def build_bundle_of_code_systems():
    bundle = {"resourceType": "Bundle", "id": "codesystems", "type": "collection", "entry": []}
    for code_system in download_code_systems():
        code_system = code_system.json()
        new_entry = {"fullUrl": code_system["url"], "resource": code_system}
        bundle["entry"].append(new_entry)

    with open("codesystems.json", "w+") as f:
        json.dump(bundle, f, indent=2)


if __name__ == "__main__":
    build_bundle_of_code_systems()
