import re
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor

class PlanCodeGenerator:
    def __init__(self, root):
        self.root = root
        self.code_lines: List[str] = []
        self.visited = set()

    def generate(self) -> str:
        self.code_lines = [
            "import pandas as pd",
            "from concurrent.futures import ThreadPoolExecutor",
            "",
            "# Thread pool and storage for futures/results",
            "executor = ThreadPoolExecutor()",
            "futures = {}     # node_id -> Future",
            "dfs = {}         # node_id -> DataFrame",
            "",
        ]
        self.visited.clear()
        # Recursively emit tasks for every node
        self._emit_all_nodes(self.root)

        # Finally, wait on root and print
        self.code_lines += [
            "#--- wait for the root node and show result ---",
            f"result = futures[{self.root.node_id}].result()",
            "print(result.head())",
            "executor.shutdown()",
        ]
        return "\n".join(self.code_lines)

    def _emit_all_nodes(self, node):
        if node.node_id in self.visited:
            return
        self.visited.add(node.node_id)

        # First emit children
        for child in node.children:
            self._emit_all_nodes(child)

        # Then emit this node’s task
        self._emit_node_task(node)

    def _emit_node_task(self, node):
        nid = node.node_id
        op = node.op_type

        # 1) Define the task function
        lines = [f"def task_{nid}():"]
        # 1a) wait on children
        for c in node.children:
            lines.append(f"    dfs[{c.node_id}] = futures[{c.node_id}].result()")

        # 1b) core logic per operator
        if op == "BindableTableScan":
            table = self._extract_table_name(node.attributes.get("table", ""))
            lines.append(f"    df = pd.read_csv('{table}.csv')")

        elif op == "BindableFilter":
            cond = self._translate_condition(node.attributes.get("condition", ""))
            child = node.children[0].node_id
            lines.append(f"    df = dfs[{child}].query({cond})")

        elif op == "BindableProject":
            cols = self._extract_projection_cols(node.attributes)
            child = node.children[0].node_id
            lines.append(f"    df = dfs[{child}][[{', '.join(cols)}]]")

        elif op == "BindableJoin":
            l, r = node.children[0].node_id, node.children[1].node_id
            jt = node.attributes.get("joinType", "inner").strip("[]")
            on = self._translate_join_condition(node.attributes.get("condition", ""))
            lines.append(f"    df = pd.merge(dfs[{l}], dfs[{r}], how='{jt}', {on})")

        elif op == "BindableAggregate":
            child = node.children[0].node_id
            aggs = self._translate_aggregates(node.attributes)
            lines.append(f"    df = dfs[{child}].agg({aggs}).reset_index(drop=True)")

        elif op == "BindableValues":
            lines.append("    df = pd.DataFrame([[]])")

        else:
            lines.append(f"    # unsupported op: {op}")
            lines.append("    df = pd.DataFrame()")

        lines.append("    return df")
        lines.append("")  # blank line after def

        # 2) submit it
        lines.append(f"futures[{nid}] = executor.submit(task_{nid})")
        lines.append("")  # blank line

        # add to main code
        self.code_lines.extend(lines)

    # ─── Helpers ─────────────────────────────────────────────────────────

    def _extract_table_name(self, table_attr: str) -> str:
        # e.g. "[[company_name]]" → "company_name"
        return table_attr.replace("[[", "").replace("]]", "").split(".")[-1]

    def _extract_projection_cols(self, attrs: Dict[str, str]) -> List[str]:
        # project keys are the output col aliases
        return [f"'{k}'" for k in attrs.keys()]

    def _translate_condition(self, cond: str) -> str:
        # very minimal translator: LIKE → str.contains, = → ==, AND/OR → and/or
        c = cond
        c = c.replace("AND(", "").replace("AND", " and ")
        c = c.replace("OR(", "").replace("OR", " or ")
        c = re.sub(r"LIKE\(\$(\d+),\s*'(.*?)'\)", r"dfs_child.str.contains(r'\2')", c)
        c = c.replace("=", "==").replace("<>", "!=")
        c = c.replace("true", "True").replace("false", "False")
        return f"r'''{c}'''"

    def _translate_join_condition(self, cond: str) -> str:
        # find all $n references
        nums = re.findall(r"\$([0-9]+)", cond)
        if len(nums) == 2:
            return f"left_on='col{nums[0]}', right_on='col{nums[1]}'"
        if len(nums) % 2 == 0:
            lefts = [f"'col{n}'" for n in nums[::2]]
            rights = [f"'col{n}'" for n in nums[1::2]]
            return f"left_on=[{','.join(lefts)}], right_on=[{','.join(rights)}]"
        return "# TODO: complex join condition"

    def _translate_aggregates(self, attrs: Dict[str, str]) -> str:
        agg_map = {}
        for alias, expr in attrs.items():
            m = re.search(r"MIN\(\$(\d+)\)", expr)
            if m:
                col = f"col{m.group(1)}"
                agg_map[alias.lower()] = f"('{col}','min')"
        entries = ", ".join(f"'{k}': {v}" for k, v in agg_map.items())
        return "{" + entries + "}"
