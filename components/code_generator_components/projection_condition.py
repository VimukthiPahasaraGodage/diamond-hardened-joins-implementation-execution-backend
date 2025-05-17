projection_condition = r"""
def bindable_to_pandas_proj(attr_str):
    # 1. find all `name=[$idx]` pairs
    pairs = re.findall(r'(\w+)=\[\$(\d+)\]', attr_str)
    if not pairs:
        raise ValueError("No projection clauses found")

    # 2. split into names and numeric indices
    names = [fld for fld, idx in pairs]
    idxs = [int(idx) for fld, idx in pairs]

    return idxs, names
"""
