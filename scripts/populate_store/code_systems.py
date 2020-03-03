import requests
from bs4 import BeautifulSoup


def upload_code_systems():

    base_url = "https://www.hl7.org/fhir"
    code_systems_url = "https://www.hl7.org/fhir/terminologies-systems.html"

    api = "https://fhir.staging.arkhn.org/api/CodeSystem"

    # Get html from hl7.org
    response = requests.get(code_systems_url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Find hypertext links in table
    links = soup.find("div", {"id": "tabs-fhir"}).findAll("a")
    # Remove normative flags
    links = [r for r in res if r.get("class") != ["normative-flag"]]

    for link in links[:2]:
        url = r.get("href").replace(".html", ".json")
        code_system = requests.get(f"{base_url}/{url}")
        response = requests.post(f"{api}", json=code_system.json())
        if response.status_code != 200:
            print("Error while uploading code system:", response.text)


if __name__ == "__main__":
    upload_code_systems()
