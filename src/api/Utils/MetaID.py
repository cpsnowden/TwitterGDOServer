import uuid


def get_id(candidate, candidate_length=10, full_length=15, prefix=""):
    long_id = str(uuid.uuid4())
    conv_desc = (prefix + ''.join(x for x in candidate if x.isalnum()))[:candidate_length + len(prefix)] + "_"
    conv_desc += long_id
    return conv_desc[:full_length], long_id


def get_cammel_id(candidate, candidate_length=10, full_length=15, prefix=""):
    return get_id(candidate.title(), candidate_length, full_length, prefix)
