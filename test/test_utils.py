from fhirstore import utils
from fhirstore.search import Bundle


def test_get_from_path():
    obj = {"a": [{"b": 1}, {"b": 2}], "c": 3}

    assert utils.get_from_path(obj, "a.b") == [1, 2]

    # nested
    obj = {"a": [{"b": 1, "c": [2]}, {"b": 3, "c": [4, 5]}]}

    assert utils.get_from_path(obj, "a.c") == [2, 4, 5]


def test_get_reference_ids_from_bundle():
    bundle = Bundle()
    bundle.content = {
        "entry": [
            {
                "resource": {
                    "a": [{"b": {"reference": "resource/1"}}, {"b": {"reference": "resource/2"}}],
                    "c": "3",
                }
            },
            {
                "resource": {
                    "a": [{"b": {"reference": "resource/11"}}, {"b": {"reference": "resource/22"}}],
                    "c": "33",
                }
            },
        ]
    }

    assert utils.get_reference_ids_from_bundle(bundle, "a.b") == ["1", "2", "11", "22"]
