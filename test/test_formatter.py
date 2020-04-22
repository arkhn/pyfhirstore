import pytest
import json
from pytest import raises
from fhirstore.search.url_parser import URL_Parser
from fhirstore.search.formatter import Formatter
from collections.abc import Mapping
from fhirstore import FHIRStore, NotFoundError
from Werkzeug.datastructures import ImmutableMultiDict


def test_parse_format_args():
    formatter = Formatter()
    formatter.parse_format_arguments(ImmutableMultiDict([]), "Patient")
    assert formatter.resource_type == "Patient"
    assert formatter.elements is None
    assert formatter.include is False
    assert formatter.elements is None
    assert formatter.summary is False
    assert formatter.is_summary_count is False


def test_initiate_search_bundle():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([]), "Patient")
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "entry": [],
        "total": 0,
    }


def test_initiate_count_bundle():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([("_summary", "count")]), "Patient")
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "tag": {"code": "SUBSETTED"},
        "total": 0,
    }


def test_initiate_tag_search_bundle():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([("_summary", "text")]), "Patient")
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "entry": [],
        "tag": {"code": "SUBSETTED"},
        "total": 0,
    }


def test_initiate_no_tag_search_bundle():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([("_summary", "false")]), "Patient")
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "entry": [],
        "total": 0,
    }


def test_fill_bundle():
    formatter = Formatter()
    formatter.fill_bundle({})
    assert formatter.bundle == {}


def test_fill_bundle_count_empty():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([("_summary", "count")]), "Patient")
    formatter.fill_bundle({})
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "tag": {"code": "SUBSETTED"},
        "total": 0,
    }


def test_fill_bundle_search_empty():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([]), "Patient")
    formatter.fill_bundle({})
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "entry": [],
        "total": 0,
    }


def test_fill_bundle_count():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([("_summary", "count")]), "Patient")
    formatter.fill_bundle(
        {"count": 1, "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0}}
    )
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "tag": {"code": "SUBSETTED"},
        "total": 1,
    }


def test_fill_bundle_search():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([]), "Patient")
    formatter.fill_bundle(
        {
            "took": 2,
            "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "max_score": 1.0925692,
                "hits": [
                    {
                        "_index": "fhirstore.patient",
                        "_type": "_doc",
                        "_id": "5e96c51977d71dc87644653b",
                        "_score": 1.0925692,
                        "_source": {
                            "active": True,
                            "contact": [
                                {
                                    "organization": {
                                        "display": "Walt Disney Corporation",
                                        "reference": "Organization/1",
                                    }
                                }
                            ],
                        },
                    }
                ],
            },
        }
    )
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "entry": [
            {
                "resource": {
                    "active": True,
                    "contact": [
                        {
                            "organization": {
                                "display": "Walt Disney Corporation",
                                "reference": "Organization/1",
                            }
                        }
                    ],
                },
                "search": {"mode": "match"},
            }
        ],
        "total": 1,
    }


def test_complete_bundle():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([]), "Patient")
    formatter.fill_bundle(
        {
            "took": 2,
            "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "max_score": 1.0925692,
                "hits": [
                    {
                        "_index": "fhirstore.patient",
                        "_type": "_doc",
                        "_id": "5e96c51977d71dc87644653b",
                        "_score": 1.0925692,
                        "_source": {
                            "active": True,
                            "contact": [
                                {
                                    "organization": {
                                        "display": "Walt Disney Corporation",
                                        "reference": "Organization/1",
                                    }
                                }
                            ],
                        },
                    }
                ],
            },
        }
    )
    new_bundle = {
        "entry": [
            {
                "resource": {
                    "active": True,
                    "birthDate": "1932-09-24",
                    "gender": "male",
                    "id": "xcda",
                    "name": [{"family": "Levin", "given": ["Henry"]}],
                    "resourceType": "Patient",
                },
                "search": {"mode": "match"},
            }
        ],
        "resource_type": "Bundle",
        "total": 1,
    }
    formatter.complete_bundle(new_bundle)
    assert formatter.bundle["total"] == 2
    assert formatter.bundle["entry"] == [
        {
            "resource": {
                "active": True,
                "contact": [
                    {
                        "organization": {
                            "display": "Walt Disney Corporation",
                            "reference": "Organization/1",
                        }
                    }
                ],
            },
            "search": {"mode": "match"},
        },
        {
            "resource": {
                "active": True,
                "birthDate": "1932-09-24",
                "gender": "male",
                "id": "xcda",
                "name": [{"family": "Levin", "given": ["Henry"]}],
                "resourceType": "Patient",
            },
            "search": {"mode": "match"},
        },
    ]


def test_complete_bundle_count():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([("_summary", "count")]), "Patient")
    formatter.fill_bundle(
        {"count": 1, "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0}}
    )
    formatter.complete_bundle(
        {"resource_type": "Bundle", "tag": {"code": "SUBSETTED"}, "total": 1,}
    )
    assert formatter.bundle["total"] == 2
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "tag": {"code": "SUBSETTED"},
        "total": 2,
    }


def test_add_included_bundle_no_include():
    formatter = Formatter()
    formatter.initiate_bundle(ImmutableMultiDict([]), "Patient")
    formatter.add_included_bundle(
        {
            "took": 2,
            "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "max_score": 1.0925692,
                "hits": [
                    {
                        "_index": "fhirstore.patient",
                        "_type": "_doc",
                        "_id": "5e96c51977d71dc87644653b",
                        "_score": 1.0925692,
                        "_source": {
                            "active": True,
                            "contact": [
                                {
                                    "organization": {
                                        "display": "Walt Disney Corporation",
                                        "reference": "Organization/1",
                                    }
                                }
                            ],
                        },
                    }
                ],
            },
        }
    )
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "entry": [],
        "total": 0,
    }


def test_add_included_bundle():
    formatter = Formatter()
    formatter.initiate_bundle(
        ImmutableMultiDict([("_include", "MedicationRequest:subject")]), "Patient"
    )
    formatter.add_included_bundle(
        {
            "took": 2,
            "timed_out": False,
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "max_score": 1.0925692,
                "hits": [
                    {
                        "_index": "fhirstore.patient",
                        "_type": "_doc",
                        "_id": "5e96c51977d71dc87644653b",
                        "_score": 1.0925692,
                        "_source": {
                            "active": True,
                            "contact": [
                                {
                                    "organization": {
                                        "display": "Walt Disney Corporation",
                                        "reference": "Organization/1",
                                    }
                                }
                            ],
                        },
                    }
                ],
            },
        }
    )
    assert formatter.bundle == {
        "resource_type": "Bundle",
        "entry": [
            {
                "resource": {
                    "active": True,
                    "contact": [
                        {
                            "organization": {
                                "display": "Walt Disney Corporation",
                                "reference": "Organization/1",
                            }
                        }
                    ],
                },
                "search": {"mode": "include"},
            }
        ],
        "total": 0,
    }
