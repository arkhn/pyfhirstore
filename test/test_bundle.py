import pytest
import json
from pytest import raises
from fhirstore.search import SearchArguments
from fhirstore.search import Bundle
from collections.abc import Mapping
from fhirstore import FHIRStore, NotFoundError
from werkzeug.datastructures import ImmutableMultiDict


def test_fill_search_bundle():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([]), "Patient")
    bundle = Bundle()
    bundle.fill({}, search_args.formatting_args)
    assert bundle.content == {
        "resource_type": "Bundle",
        "entry": [],
        "total": 0,
    }


def test_fill_count_bundle():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_summary", "count")]), "Patient")
    bundle = Bundle()
    bundle.fill({}, search_args.formatting_args)
    assert bundle.content == {
        "resource_type": "Bundle",
        "tag": {"code": "SUBSETTED"},
        "total": 0,
    }


def test_fill_tag_search_bundle():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_summary", "text")]), "Patient")
    bundle = Bundle()
    bundle.fill({}, search_args.formatting_args)
    assert bundle.content == {
        "resource_type": "Bundle",
        "entry": [],
        "tag": {"code": "SUBSETTED"},
        "total": 0,
    }


def test_fill_no_tag_search_bundle():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_summary", "false")]), "Patient")
    bundle = Bundle()
    bundle.fill({}, search_args.formatting_args)
    assert bundle.content == {
        "resource_type": "Bundle",
        "entry": [],
        "total": 0,
    }


def test_fill_bundle_search():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([]), "Patient")
    bundle = Bundle()
    bundle.fill(
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
        },
        search_args.formatting_args,
    )
    assert bundle.content == {
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
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([]), "Patient")
    bundle = Bundle()
    bundle.fill(
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
        },
        search_args.formatting_args,
    )
    new_bundle = Bundle()
    new_bundle.content = {
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
    bundle.complete(new_bundle, search_args.formatting_args)
    assert bundle.content["total"] == 2
    assert bundle.content["entry"] == [
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
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_summary", "count")]), "Patient")
    bundle = Bundle()
    bundle.fill(
        {"count": 1, "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0}},
        search_args.formatting_args,
    )
    other_bundle = Bundle()
    other_bundle.content = {"resource_type": "Bundle", "tag": {"code": "SUBSETTED"}, "total": 1}
    bundle.complete(other_bundle, search_args.formatting_args)
    assert bundle.content["total"] == 2
    assert bundle.content == {
        "resource_type": "Bundle",
        "tag": {"code": "SUBSETTED"},
        "total": 2,
    }


def test_add_included_bundle_no_include():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([]), "Patient")
    bundle = Bundle()
    bundle.append(
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
        },
        search_args.formatting_args,
    )
    assert bundle.content == {
        "resource_type": "Bundle",
        "entry": [],
        "total": 0,
    }


def test_add_included_bundle():
    search_args = SearchArguments()
    search_args.parse(ImmutableMultiDict([("_include", "MedicationRequest:subject")]), "Patient")
    bundle = Bundle()
    bundle.append(
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
        },
        search_args.formatting_args,
    )
    assert bundle.content == {
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
