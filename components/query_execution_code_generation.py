import re
from typing import Dict, List


class PlanCodeGenerator:
    def __init__(self, root):
        self.root = root
        self.code_lines = []
        self.visited = set()

    def generate(self) -> str:
        self.code_lines = ["import pandas as pd", "# Load CSVs (you may need to adjust paths)", ]
        self._gen_node_code(self.root)
        self.code_lines.append(f"\nresult = df_{self.root.node_id}")
        self.code_lines.append("print(result.head())")
        return "\n".join(self.code_lines)

    def _gen_node_code(self, node):
        if node.node_id in self.visited:
            return
        self.visited.add(node.node_id)

        # Generate code for children first
        for child in node.children:
            self._gen_node_code(child)

        op = node.op_type
        df_name = f"df_{node.node_id}"

        if op == "BindableTableScan":
            table = self._extract_table_name(node.attributes.get("table", ""))
            self.code_lines.append(f"{df_name} = pd.read_csv('{table}.csv')")

        elif op == "BindableFilter":
            child_df = f"df_{node.children[0].node_id}"
            condition = self._translate_condition(node.attributes.get("condition", ""))
            self.code_lines.append(f"{df_name} = {child_df}.query({condition})")

        elif op == "BindableProject":
            child_df = f"df_{node.children[0].node_id}"
            cols = self._extract_projection_cols(node.attributes)
            self.code_lines.append(f"{df_name} = {child_df}[[{', '.join(cols)}]]")

        elif op == "BindableJoin":
            left = f"df_{node.children[0].node_id}"
            right = f"df_{node.children[1].node_id}"
            join_type = node.attributes.get("joinType", "inner").strip('[]')
            on_cols = self._translate_join_condition(node.attributes.get("condition", ""))
            if on_cols == "cross_join":
                self.code_lines.append(f"{df_name} = {left}.merge({right}, how='cross')")
            else:
                self.code_lines.append(f"{df_name} = pd.merge({left}, {right}, how='{join_type}', {on_cols})")

        elif op == "BindableAggregate":
            child_df = f"df_{node.children[0].node_id}"
            aggs = self._translate_aggregates(node.attributes)
            self.code_lines.append(f"{df_name} = {child_df}.agg({aggs}).reset_index(drop=True)")

        elif op == "BindableValues":
            self.code_lines.append(f"{df_name} = pd.DataFrame([[]])")

        else:
            self.code_lines.append(f"# Unsupported node type: {op}")
            self.code_lines.append(f"{df_name} = pd.DataFrame()  # placeholder")

    def _extract_table_name(self, table_attr):
        # table=[[movie_companies]] => movie_companies
        return table_attr.replace("[[", "").replace("]]", "").split(".")[-1]

    def _extract_projection_cols(self, attrs: Dict[str, str]) -> List[str]:
        # Extract projection columns like name=[$1], title=[$2] -> return ['"name"', '"title"']
        return [f"'{k}'" for k in attrs.keys()]

    def _translate_condition(self, cond: str) -> str:
        # Minimal translator for now; could be improved with regex parsing
        cond = cond.replace("LIKE", "str.contains")
        cond = cond.replace("AND", "and").replace("OR", "or")
        cond = cond.replace("=", "==")
        cond = cond.replace("<>", "!=")
        cond = cond.replace("true", "True").replace("false", "False")
        return f'""" {cond} """'

    def _translate_join_condition(self, cond: str) -> str:
        # Example: =($0, $1) => on=['col0', 'col1']
        matches = re.findall(r"\$([0-9]+)", cond)
        if len(matches) == 2:
            return f"left_on='col{matches[0]}', right_on='col{matches[1]}'"
        elif "AND" in cond:
            cols = re.findall(r"\$([0-9]+)", cond)
            return f"left_on={[f'col{c}' for c in cols[::2]]}, right_on={[f'col{c}' for c in cols[1::2]]}"
        elif "true" in cond:
            return "cross_join"
        else:
            return "# TODO: Complex join condition"

    def _translate_aggregates(self, attrs: Dict[str, str]) -> str:
        agg_map = {}
        for k, v in attrs.items():
            match = re.search(r'MIN\(\$(\d+)\)', v)
            if match:
                col = f"col{match.group(1)}"
                agg_map[
                    k.lower()] = f"('{col}', 'min')"  # You can add other aggregate support here (e.g. MAX, SUM, etc.)

        return "{" + ", ".join(f"'{k}': {v}" for k, v in agg_map.items()) + "}"
