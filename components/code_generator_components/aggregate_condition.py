aggregate_condition=r"""
def parse_bindable_aggregate(line, df_num_cols):
    input_cols = [i for i in range(df_num_cols)]
    grp_match = re.search(r'group=\[\{(.*?)\}\]', line)
    group_cols = [c.strip() for c in grp_match.group(1).split(',')] if grp_match and grp_match.group(1).strip() else []
    pairs = re.findall(r'(\w+)=\[\s*(\w+)\s*\(\s*\$(\d+)\)\s*\]', line)
    return group_cols, {out_col: (input_cols[int(idx)], func.lower()) for out_col, func, idx in pairs}
"""

