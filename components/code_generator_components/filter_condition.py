filter_condition = r"""
def parse_atom(op, col_idx, arg, df):
    col = df.iloc[:, col_idx].astype(str)
    if op == '=':
        return col == str(arg)
    if op == '<>':
        return col != str(arg)
    if op == '>':
        return pd.to_numeric(col, errors='coerce') > arg
    if op == '<':
        return pd.to_numeric(col, errors='coerce') < arg
    if op == 'LIKE':
        pattern = '^' + re.escape(arg).replace('%', '.*') + '$'
        return col.str.match(pattern)
    if op == 'SEARCH':
        # numeric‐range as before
        if isinstance(arg, tuple) and len(arg) == 2:
            start, end = arg
            num = pd.to_numeric(col, errors='coerce')
            return num.between(start, end)

        # list of exact terms:
        elif isinstance(arg, list):
            mask = pd.Series(False, index=df.index)
            for term in arg:
                mask |= (col == term)  # ← direct equality, not substring
            return mask

        # single‐value fallback
        else:
            return col == str(arg)
    raise ValueError(f"Unknown op: {op}")


def eval_condition(cond, df):
    op = cond[0]
    if op == 'AND':
        masks = [eval_condition(c, df) for c in cond[1:]]
        m = masks[0]
        for mm in masks[1:]:
            m &= mm
        return m
    if op == 'OR':
        masks = [eval_condition(c, df) for c in cond[1:]]
        m = masks[0]
        for mm in masks[1:]:
            m |= mm
        return m
    # atomic
    _, col_idx, arg = cond
    return parse_atom(op, col_idx, arg, df)


def condition_from_text(text):
    text = text.strip()
    # AND/OR as before
    m = re.match(r'^(AND|OR)\((.*)\)$', text, re.IGNORECASE)
    if m:
        op, body = m.groups()
        parts = split_top_level(body)
        return (op.upper(),) + tuple(condition_from_text(p) for p in parts)

    # atomic
    m = re.match(r'^(=|<>|>|<|LIKE|SEARCH)\(\s*\$(\d+)\s*,\s*(.+)\)$', text, re.IGNORECASE)
    if not m:
        raise ValueError("Can't parse: " + text)
    op, col, arg = m.groups()
    op = op.upper()
    col = int(col)
    arg = arg.strip()

    # 1) numeric literal
    if re.fullmatch(r'\d+(\.\d+)?', arg):
        arg = float(arg) if '.' in arg else int(arg)

    # 2) explicit [a..b] range
    elif op == 'SEARCH' and (r := re.fullmatch(r'\[\s*(\d+)\.\.(\d+)\s*\]', arg)):
        arg = (int(r.group(1)), int(r.group(2)))

    # 3) Sarg[[a..b]] numeric‐range
    elif op == 'SEARCH' and (r := re.fullmatch(r'Sarg\[\[\s*(\d+)\.\.(\d+)\s*\]\]', arg)):
        arg = (int(r.group(1)), int(r.group(2)))

    # 4) Sarg list of VARCHAR terms (as before)
    elif op == 'SEARCH' and (r := re.match(r"Sarg\[\s*(.+?)\]\s*:(?:VARCHAR|.+)$", arg)):
        inside = r.group(1)
        items = []
        for part in split_top_level(inside):
            val = part.split(':', 1)[0].strip().strip("'\"")
            items.append(val)
        arg = items

    # 5) everything else: strip quotes
    else:
        arg = arg.strip("'\"")

    return (op, col, arg)


def split_top_level(s):
    parts, depth, cur = [], 0, []
    for ch in s:
        if ch == '(':
            depth += 1
            cur.append(ch)
        elif ch == ')':
            depth -= 1
            cur.append(ch)
        elif ch == ',' and depth == 0:
            parts.append(''.join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    parts.append(''.join(cur).strip())
    return parts
"""
