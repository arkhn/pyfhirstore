import requests
import io
import zipfile
import json
from os import path
from timeit import default_timer as timer
import statistics
from uuid import uuid4

from tqdm import tqdm
import fhirbase
import psycopg2

example_blacklist = [
    "package-min-ver.json",
    "profiles-resources.json",
    "questionnaireresponse-extensions-QuestionnaireResponse-item-subject.json",
    "binary-example.json",
    "binary-f006.json",
    "bundle-example.json",
    "bundle-references.json",
    "bundle-request-medsallergies.json",
    "bundle-request-simplesummary.json",
    "bundle-response-medsallergies.json",
    "bundle-response-simplesummary.json",
    "bundle-response.json",
    "bundle-search-warning.json",
    "catalogentry-example.json",
    "chargeitemdefinition-device-example.json",
    "chargeitemdefinition-ebm-example.json",
    "codesystem-extensions-CodeSystem-author.json",
    "codesystem-extensions-CodeSystem-effective.json",
    "chargeitemdefinition-ebm-example.json",
    "codesystem-extensions-CodeSystem-end.json",
    "codesystem-extensions-CodeSystem-keyword.json",
    "conceptmaps.json",
    "coord-0base-example.json",
    "coord-1base-example.json",
    "coverageeligibilityrequest-example-2.json",
    "coverageeligibilityrequest-example.json",
    "coverageeligibilityresponse-example-benefits-2.json",
    "dataelements.json",
    "device-extensions-Device-din.json",
    "devicedefinition-example.json",
    "diagnosticreport-example-f001-bloodexam.json",
    "diagnosticreport-example-f202-bloodculture.json",
    "document-example-dischargesummary.json",
    "effectevidencesynthesis-example.json",
    "endpoint-examples-general-template.json",
    "evidence-example.json",
    "evidencevariable-example.json",
    "extension-definitions.json",
    "external-resources.json",
    "group-example-herd1.json",
    "graphdefinition-questionnaire.json",
    "group-example-member.json",
    "group-example-patientlist.json",
    "group-example.json",
    "insuranceplan-example.json",
    "location-examples-general.json",
    "medicationknowledge-example.json",
    "medicinalproductcontraindication-example.json",
    "medicinalproductindication-example.json",
    "medicinalproductinteraction-example.json",
    "medicinalproductmanufactured-example.json",
    "medicinalproductundesirableeffect-example.json",
    "message-request-link.json",
    "message-response-link.json",
    "molecularsequence-example.json",
    "namingsystem-registry.json",
    "namingsystem-terminologies.json",
    "observation-genetic-Observation-amino-acid-change.json",
    "observation-genetic-Observation-dna-variant.json",
    "observation-genetic-Observation-gene-amino-acid-change.json",
    "observation-genetic-Observation-gene-dnavariant.json",
    "observation-genetic-Observation-gene-identifier.json",
    "organizationaffiliation-example.json",
    "orgrole-example-hie.json",
    "orgrole-example-services.json",
    "patient-examples-cypress-template.json",
    "patient-examples-general.json",
    "patient-extensions-Patient-age.json",
    "patient-extensions-Patient-birthOrderBoolean.json",
    "patient-extensions-Patient-mothersMaidenName.json",
    "practitioner-examples-general.json",
    "practitionerrole-examples-general.json",
    "profiles-others.json",
    "profiles-types.json",
    "questionnaire-profile-example-ussg-fht.json",
    "researchdefinition-example.json",
    "researchelementdefinition-example.json",
    "riskevidencesynthesis-example.json",
    "search-parameters.json",
    "searchparameter-example-extension.json",
    "searchparameter-example-reference.json",
    "searchparameter-example.json",
    "searchparameter-filter.json",
    "sequence-complex-variant.json",
    "sequence-example-fda-comparisons.json",
    "sequence-example-fda-vcfeval.json",
    "sequence-example-fda.json",
    "sequence-example-pgx-1.json",
    "sequence-example-pgx-2.json",
    "sequence-example-TPMT-one.json",
    "sequence-example-TPMT-two.json",
    "sequence-genetics-example-breastcancer.json",
    "sequence-graphic-example-1.json",
    "sequence-graphic-example-2.json",
    "sequence-graphic-example-3.json",
    "sequence-graphic-example-4.json",
    "sequence-graphic-example-5.json",
    "v2-tables.json",
    "v3-codesystems.json",
    "valueset-extensions-ValueSet-author.json",
    "valueset-extensions-ValueSet-effective.json",
    "valueset-extensions-ValueSet-end.json",
    "valueset-extensions-ValueSet-keyword.json",
    "valueset-extensions-ValueSet-workflow.json",
    "valuesets.json",
    "xds-example.json",
    "bundle-transaction.json",
    "codesystem-extensions-CodeSystem-workflow.json",
    "coverageeligibilityresponse-example-benefits.json",
    "coverageeligibilityresponse-example-error.json",
    "coverageeligibilityresponse-example.json",
    "diagnosticreport-example-ghp.json",
    "diagnosticreport-example-lipids.json",
    "diagnosticreport-example-lri.json",
    "diagnosticreport-example.json",
    "diagnosticreport-examples-general.json",
    "diagnosticreport-genetic-DiagnosticReport-assessed-condition.json",
    "diagnosticreport-genetics-comprehensive-bone-marrow-report.json",
    "diagnosticreport-genetics-example-2-familyhistory.json",
    "diagnosticreport-hla-genetics-results-example.json",
    "diagnosticreport-micro1.json",
]


def count_examples():
    with zipfile.ZipFile("benchmark/examples.zip") as archive:
        return len(
            [f for f in archive.infolist() if f.filename not in example_blacklist]
        )


def iter_examples():
    with zipfile.ZipFile("benchmark/examples.zip") as archive:
        for zipinfo in archive.infolist():
            with archive.open(zipinfo) as thefile:
                if zipinfo.filename not in example_blacklist:
                    yield zipinfo.filename, json.load(thefile)


def download_resources():
    """
    Downloads examples from HL7 website.
    """
    if not path.exists("benchmark/examples.zip"):
        url = "http://www.hl7.org/fhir/examples-json.zip"
        r = requests.get(url, stream=True)
        total_size = int(r.headers.get("content-length", 0))
        block_size = 1024
        t = tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc="Downloading example resources",
        )
        with open("benchmark/examples.zip", "wb") as f:
            for data in r.iter_content(block_size):
                t.update(len(data))
                f.write(data)
        t.close()
    else:
        print("Using cached resources")


download_resources()
connection = psycopg2.connect(
    dbname="fb", user="postgres", host="localhost", port="5432"
)
fb = fhirbase.FHIRBase(connection)

examples = tqdm(iter_examples(), total=count_examples(),
                desc="Running write benchmark")
stats = {}
inserted = []
for example, data in examples:
    if not data.get("id"):
        data["id"] = str(uuid4())
    start = timer()
    res = fb.create(data)
    end = timer()
    stats[example] = end - start
    inserted.append(res)

values = stats.values()
print(f"insertions per second (on average): {1/statistics.mean(values):.2f}")
print(f"average: {statistics.mean(values)*1000:.2f} milliseconds")
print(f"median: {statistics.median(values)*1000:.2f} milliseconds")
print(f"min: {min(values)*1000:.2f} milliseconds")
print(f"max: {max(values)*1000:.2f} milliseconds")
print(f"spread: {statistics.variance(values)}")

examples = tqdm(inserted, desc="Running read benchmark")
stats = {}
for doc in examples:
    start = timer()
    fb.read(doc["resourceType"], doc["id"])
    end = timer()
    stats[doc["id"]] = end - start

values = stats.values()
print(f"reads per second (on average): {1/statistics.mean(values):.2f}")
print(f"average: {statistics.mean(values)*1000:.2f} milliseconds")
print(f"median: {statistics.median(values)*1000:.2f} milliseconds")
print(f"min: {min(values)*1000:.2f} milliseconds")
print(f"max: {max(values)*1000:.2f} milliseconds")
print(f"spread: {statistics.variance(values)}")
