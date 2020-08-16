from fhirstore import utils


def test_get_from_path():
    obj = {"a": [{"b": 1}, {"b": 2}], "c": 3}

    assert utils.get_from_path(obj, "a.b") == [1, 2]

    # nested
    obj = {"a": [{"b": 1, "c": [2]}, {"b": 3, "c": [4, 5]}]}

    assert utils.get_from_path(obj, "a.c") == [2, 4, 5]

