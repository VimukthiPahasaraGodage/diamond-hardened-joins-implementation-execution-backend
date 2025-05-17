from typing import List

from components.code_generator_components.aggregate_condition import aggregate_condition
from components.code_generator_components.filter_condition import filter_condition
from components.code_generator_components.join_condition import join_condition
from components.code_generator_components.projection_condition import projection_condition


class PlanCodeGenerator:
    def __init__(self, root):
        self.root = root
        self.code_lines: List[str] = []

    def generate(self) -> str:
        self.code_lines = []
        all_nodes = self._collect_nodes(self.root)

        self._emit_header()
        self._emit_functions_for_conditions()
        self._emit_dependency_maps(all_nodes)
        self._emit_metadata_maps(all_nodes)
        self._emit_make_task()
        self._emit_scheduler()
        return "\n".join(self.code_lines)

    def _emit_header(self):
        self.code_lines += [
            "import concurrent.futures",
            "import re",
            "from collections import defaultdict",
            "from concurrent.futures import ThreadPoolExecutor",
            "from typing import List, Tuple, Union, Dict",
            "import pandas as pd",
            "",
            "prefix = 'D:/UoM/Semester_8/CS4633_Database_Internals/JOB/JOB_database/final_final'"
        ]

    def _emit_functions_for_conditions(self):
        code_blocks = [filter_condition.splitlines(),
                       aggregate_condition.splitlines(),
                       projection_condition.splitlines(),
                       join_condition.splitlines()]
        for code_block in code_blocks:
            for _, line in enumerate(code_block):
                self.code_lines.append(line)

    def _collect_nodes(self, root):
        nodes = []

        def dfs(n):
            nodes.append(n)
            for c in n.children:
                dfs(c)

        dfs(root)
        return nodes

    def _emit_dependency_maps(self, nodes):
        # children
        self.code_lines.append("# --- dependencies ---")
        self.code_lines.append("children = {")
        for n in nodes:
            cids = [c.node_id for c in n.children]
            self.code_lines.append(f"    {n.node_id}: {cids},")
        self.code_lines.append("}")
        self.code_lines.append("")
        # parents
        self.code_lines += [
            "parents = defaultdict(list)",
            "for p, chs in children.items():",
            "    for c in chs:",
            "        parents[c].append(p)",
            "",
        ]
        # pending_inputs
        self.code_lines.append("pending_inputs = {nid: len(chs) for nid, chs in children.items()}")
        self.code_lines.append("")

    def _emit_metadata_maps(self, nodes):
        # op_type
        self.code_lines.append("op_type = {")
        for n in nodes:
            self.code_lines.append(f"    {n.node_id}: '{n.op_type}',")
        self.code_lines.append("}")
        self.code_lines.append("")
        # attrs
        self.code_lines.append("attrs = {")
        for n in nodes:
            safe = {k: v for k, v in n.attributes.items()}
            self.code_lines.append(f"    {n.node_id}: {safe},")
        self.code_lines.append("}")
        self.code_lines.append("")
        # table_name
        self.code_lines.append("table_name = {}")
        for n in nodes:
            if n.op_type == "TableScan":
                self.code_lines.append(
                    f"table_name[{n.node_id}] = attrs[{n.node_id}].get('table','')"
                )
        self.code_lines.append("")

    def _emit_make_task(self):
        self.code_lines += [
            "# --- task factory (no blocking) ---",
            "def make_task(nid):",
            "    def task():",
            "        op = op_type[nid]",
            "        at = attrs[nid]",
            "        df = None",
            "        if op=='TableScan':",
            "            df = pd.read_csv(f\"{prefix}/{table_name[nid]}.csv\", header=None, low_memory=False)",
            "        elif op=='Values':",
            "            df = pd.DataFrame([[]])",
            "        elif op=='Filter':",
            "            child = children[nid][0]",
            "            dfs_child = dfs[child]",
            "            cond = condition_from_text(at['condition'])",
            "            mask = eval_condition(cond, dfs_child)",
            "            df = dfs_child[mask]",
            "        elif op=='Project':",
            "            child = children[nid][0]",
            "            dfs_child = dfs[child]",
            "            idxs, names = bindable_to_pandas_proj(at['projection'])",
            "            df = dfs_child.iloc[:, idxs]",
            "            df.columns = names",
            "        elif op=='Join':",
            "            l,r = children[nid]",
            "            left_df = dfs[l]",
            "            right_df = dfs[r]",
            "            jt = at['joinType']",
            "            cond = at['condition']",
            "            params = build_merge_params(cond, jt, left_df.shape[1], right_df.shape[1])",
            "            if params['cross']:",
            "                left_df['_tmp'] = 1",
            "                right_df['_tmp'] = 1",
            "                df = left_df.merge(right_df, on='_tmp', how=params['how']).drop('_tmp', axis=1)",
            "            else:",
            "                df = left_df.merge(right_df, how=params['how'], left_on=params['left_on'], right_on=params['right_on'])",
            "            df.columns = list(range(df.shape[1]))",
            "        elif op=='Aggregate':",
            "            child = children[nid][0]",
            "            dfs_child = dfs[child]",
            "            group_cols, agg_map = parse_bindable_aggregate(at['aggregation'], dfs_child.shape[1])",
            "            if group_cols:",
            "                # regular GROUP BY",
            "                df = (dfs_child.groupby(group_cols).agg(**agg_map).reset_index())",
            "            else:",
            "                # no GROUP BY → aggregate entire frame",
            "                df = dfs_child.agg(**agg_map)",
            "        else:",
            "            raise ValueError(f'No such operation type: {op}')",
            "        return df",
            "    return task",
            "",
        ]

    def _emit_scheduler(self):
        self.code_lines += [
            "# --- scheduler ---",
            "executor = ThreadPoolExecutor(max_workers=8)",
            "futures = {}",
            "dfs = {}",
            "",
            "def _make_callback(nid):",
            "    def _cb(fut):",
            "        print(f'[DONE] Node {nid}')",
            "        dfs[nid] = fut.result()",
            "        for p in parents[nid]:",
            "            pending_inputs[p] -= 1",
            "            if pending_inputs[p]==0:",
            "                print(f'[SCHEDULING] Node {p}')",
            "                f = executor.submit(make_task(p))",
            "                futures[p] = f",
            "                f.add_done_callback(_make_callback(p))",
            "    return _cb",
            "",
            "# submit leaves",
            "for nid, cnt in pending_inputs.items():",
            "    if cnt==0:",
            "        f = executor.submit(make_task(nid))",
            "        futures[nid] = f",
            "        f.add_done_callback(_make_callback(nid))",
            "",
            "# Wait for all futures to complete",
            "concurrent.futures.wait(futures.values())",
            "executor.shutdown()",
            "",
            f"# Now dfs[{self.root.node_id}] should exist",
            f"result = dfs[{self.root.node_id}]",
            "print(result.head())"
        ]
