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
