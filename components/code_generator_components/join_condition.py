join_condition = r"""
class JoinConditionParser:
    eq_pattern = re.compile(r"=\(\$(\d+),\s*\$(\d+)\)")
    and_pattern = re.compile(r"AND\((.*)\)", re.IGNORECASE)

    def __init__(self, condition: str):
        self.condition = condition.strip()

    def parse(self) -> Dict[str, Union[bool, List[Tuple[int, int]]]]:
        if self.condition.lower() == 'true':
            return {'cross': True, 'pairs': []}

        m = self.and_pattern.match(self.condition)
        if m:
            parts = split_top_level(m.group(1))
        else:
            parts = [self.condition]

        pairs = []
        for part in parts:
            part = part.strip()
            em = self.eq_pattern.match(part)
            if not em:
                raise ValueError(f"Unsupported clause: {part}")
            left_idx, right_idx = int(em.group(1)), int(em.group(2))
            pairs.append((left_idx, right_idx))
        return {'cross': False, 'pairs': pairs}


def split_top_level(s: str) -> List[str]:
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
    if cur:
        parts.append(''.join(cur).strip())
    return parts


def build_merge_params(condition: str, join_type: str, left_df_num_cols: int, right_df_num_col: int) -> Dict:
    parser = JoinConditionParser(condition)
    info = parser.parse()
    if info['cross']:
        # Cross join workaround: add temporary key
        return {'cross': True, 'how': join_type}
    # Map index pairs to column names
    left_on = []
    right_on = []
    for pair in info['pairs']:
        l = min(pair)
        r = max(pair) - left_df_num_cols
        if r >= right_df_num_col:
            raise IndexError("Number of columns in right side is not sufficient")
        left_on.append(l)
        right_on.append(r)
    return {'cross': False, 'how': join_type, 'left_on': left_on, 'right_on': right_on}
"""
