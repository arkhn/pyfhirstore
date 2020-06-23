def get_from_path(obj, path):
    return _get_from_steps(obj, path.split("."))


def _get_from_steps(obj, steps):
    if steps == []:
        return obj
    if isinstance(obj, list):
        res = []
        for el in obj:
            res_el = _get_from_steps(el, steps)
            if isinstance(res_el, list):
                res.extend(res_el)
            else:
                res.append(res_el)
        return res
    else:
        return _get_from_steps(obj[steps[0]], steps[1:])


def get_reference_ids_from_bundle(bundle, path):
    ids = []
    for item in bundle.content["entry"]:
        reference = get_from_path(item["resource"], path)
        if isinstance(reference, list):
            ids.extend([r["reference"].split(sep="/", maxsplit=1)[1] for r in reference])
        else:
            ids.append(reference["reference"].split(sep="/", maxsplit=1)[1])

    return ids
