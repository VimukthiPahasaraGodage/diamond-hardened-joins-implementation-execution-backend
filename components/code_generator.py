from typing import List

from components.code_generator_components.aggregate_condition import aggregate_condition
from components.code_generator_components.filter_condition import filter_condition
from components.code_generator_components.helper_functions import fill_nan_with_default_values
from components.code_generator_components.join_algorithms.hash_join_algorithm import partitioned_hash_join_function
from components.code_generator_components.join_algorithms.le_decomposition_join_algorithm import \
    le_decomposition_join_function
from components.code_generator_components.join_condition import join_condition
from components.code_generator_components.projection_condition import projection_condition


class PlanCodeGenerator:
    def __init__(self, root, prefix, join_method: str):
        self.root = root
        self.prefix = prefix
        self.join_method = join_method  # 'auto', 'hash-join', 'nested-loop-join', 'LE-decomp'
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
            "import os",
            "import re",
            "import threading",
            "import time",
            "import psutil",
            "from collections import defaultdict",
            "from concurrent.futures import ThreadPoolExecutor",
            "from typing import List, Tuple, Union, Dict",
            "",
            "import pandas as pd",
            "",
            "process = psutil.Process()",
            "# storage for metrics",
            "timings: Dict[int, float] = {}         # seconds spent in each node",
            "mem_usage: Dict[int, int] = {}         # RSS in bytes after each node completes",
            "row_counts: Dict[int, int] = {}        # number of rows output by each node",
            "",
            "# Will accumulate only join outputs:",
            "total_intermediate_rows = 0",
            "intermediate_lock = threading.Lock()",
            "",
            f"prefix = '{self.prefix}'"
        ]

    def _emit_functions_for_conditions(self):
        code_blocks = [partitioned_hash_join_function.splitlines(),
                       filter_condition.splitlines(),
                       aggregate_condition.splitlines(),
                       projection_condition.splitlines(),
                       join_condition.splitlines(),
                       le_decomposition_join_function.splitlines(),
                       fill_nan_with_default_values.splitlines()]
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

    def _add_join_operation_routine(self):
        if self.join_method == 'auto':
            return [
                "            l,r = children[nid]",
                "            left_df = dfs[l]",
                "            right_df = dfs[r]",
                "            jt = at['joinType']",
                "            cond = at['condition']",
                "            params = build_merge_params(cond, jt, left_df.shape[1], right_df.shape[1])",
                "            if params['cross']:",
                "                left_df = dfs[l].copy()",
                "                right_df = dfs[r].copy()",
                "                left_df['_tmp'] = 1",
                "                right_df['_tmp'] = 1",
                "                df = left_df.merge(right_df, on='_tmp', how=params['how']).drop('_tmp', axis=1)",
                "            else:",
                "                df = left_df.merge(right_df, how=params['how'], left_on=params['left_on'], right_on=params['right_on'])",
                "            df.columns = list(range(df.shape[1]))"
            ]
        elif self.join_method == 'hash-join':
            return [
                "            l,r = children[nid]",
                "            left_df = dfs[l]",
                "            right_df = dfs[r]",
                "            jt = at['joinType']",
                "            cond = at['condition']",
                "            params = build_merge_params(cond, jt, left_df.shape[1], right_df.shape[1])",
                "            if params['cross']:",
                "                left_df = dfs[l].copy()",
                "                right_df = dfs[r].copy()",
                "                left_df['_tmp'] = 1",
                "                right_df['_tmp'] = 1",
                "                df = left_df.merge(right_df, on='_tmp', how=params['how']).drop('_tmp', axis=1)",
                "            else:",
                "                if params['how'] == 'inner':",
                "                    df = partitioned_hash_join(",
                "                        left_df,",
                "                        right_df,",
                "                        params['left_on'],",
                "                        params['right_on'])",
                "                else:",
                "                    df = left_df.merge(right_df, how=params['how'], left_on=params['left_on'], right_on=params['right_on'])",
                "            df.columns = list(range(df.shape[1]))"
            ]
        elif self.join_method == 'nested-loop-join':
            pass
        elif self.join_method == 'LE_decomposition':
            pass
        else:
            raise ValueError(f"No such join method: {self.join_method}")

    def _emit_make_task(self):
        self.code_lines += [
                               "LE_stacks_1 = {}",
                               "LE_stacks_2 = {}",
                               "",
                               "# --- task factory (no blocking) ---",
                               "def make_task(nid):",
                               "    def task():",
                               "        global total_intermediate_rows",
                               "        start = time.perf_counter()",
                               "        before_mem = process.memory_info().rss",
                               "",
                               "        op = op_type[nid]",
                               "        at = attrs[nid]",
                               "        if op=='TableScan':",
                               "            df = pd.read_csv(f\"{prefix}/{table_name[nid]}.csv\", header=None, low_memory=False)",
                               "            df = fill_nan_with_defaults(df)",
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
                               f"            if nid == {self.root.node_id}:",
                               "                df.columns = names",
                               "            else:",
                               "                df.columns = [i for i in range(df.shape[1])]",
                               "        elif op=='Join':"] + \
                           self._add_join_operation_routine() + \
                           [
                               "        elif op=='Aggregate':",
                               "            child = children[nid][0]",
                               "            dfs_child = dfs[child]",
                               "            group_cols, agg_map = parse_bindable_aggregate(at['aggregation'], dfs_child.shape[1])",
                               "            if group_cols:",
                               "                # regular GROUP BY",
                               "                df = (dfs_child.groupby(group_cols).agg(**agg_map).reset_index())",
                               "            else:",
                               "                # no GROUP BY → aggregate entire frame",
                               "                results = {}",
                               "                for out_col, (col_idx, func) in agg_map.items():",
                               "                    series = dfs_child.iloc[:, col_idx]",
                               "                    results[out_col] = getattr(series, func)()",
                               "                df = pd.DataFrame([results])",
                               "        elif op == 'Lookup':",
                               "            l, r = children[nid]",
                               "            left_df = dfs[l]",
                               "            right_df = dfs[r]",
                               "            jt = at['joinType']",
                               "            cond = at['condition']",
                               "            left_num_cols = left_df.shape[1]",
                               "            right_num_cols = right_df.shape[1]",
                               "            if op_type[l] == 'Lookup':",
                               "                l_child_lookup, r_child_lookup = children[l]",
                               "                left_num_cols = dfs[l_child_lookup].shape[1] + dfs[r_child_lookup].shape[1]",
                               "            elif op_type[r] == 'Lookup':",
                               "                l_child_lookup, r_child_lookup = children[2]",
                               "                right_num_cols = dfs[l_child_lookup].shape[1] + dfs[r_child_lookup].shape[1]",
                               "            params = build_merge_params(cond, jt, left_num_cols, right_num_cols)",
                               "            LE = int(at['LE'])",
                               "            if LE in LE_stacks_1:",
                               "                LE_stacks_1[LE].append({'nid': nid, 'l': l, 'r': r, 'jt': jt, 'left_on': params['left_on'], 'right_on': params['right_on']})",
                               "            else:",
                               "                LE_stacks_1[LE] = [{'nid': nid, 'l': l, 'r': r, 'jt': jt, 'left_on': params['left_on'], 'right_on': params['right_on']}]",
                               "            df = pd.DataFrame()",
                               "        elif op == 'Expand':",
                               "            lookup_id = int(at['lookup_id'])",
                               "            LE = int(at['LE'])",
                               "            info = LE_stacks_1[LE].pop(0)",
                               "            if info['nid'] == lookup_id:",
                               "                if LE in LE_stacks_2:",
                               "                    LE_stacks_2[LE].insert(0, info)",
                               "                else:",
                               "                    LE_stacks_2[LE] = [info]",
                               "            else:",
                               "                raise ValueError('lookup_id does not match with the id of first element in LE_stack_1')",
                               "            df = None",
                               "            if len(LE_stacks_1[LE]) == 0:",
                               "                df = le_decomposition_join(LE)",
                               "                df.columns = list(range(df.shape[1]))",
                               "            else:",
                               "                df = pd.DataFrame()",
                               "        else:",
                               "            raise ValueError(f'No such operation type: {op}')",
                               "",
                               "        elapsed = time.perf_counter() - start",
                               "        timings[nid] = elapsed",
                               "        row_counts[nid] = df.shape[0]",
                               "        mem_usage[nid] = process.memory_info().rss - before_mem",
                               "",
                               "        if op == 'Join' or op == 'Lookup' or op == 'Expand':",
                               "            with intermediate_lock:",
                               "                total_intermediate_rows += df.shape[0]",
                               "        return df",
                               "    return task",
                               "",
                           ]

    def _emit_scheduler(self):
        self.code_lines += [
            "# --- scheduler ---",
            f"executor = ThreadPoolExecutor(max_workers=len(op_type))",
            "futures = {}",
            "dfs = {}",
            "",
            "root_finished = threading.Event()",
            "",
            "def _make_callback(nid):",
            "    def _cb(fut):",
            "        print(f'[DONE] Node {nid}')",
            "        dfs[nid] = fut.result()",
            f"        if nid == {self.root.node_id}:",
            "            root_finished.set()",
            "        for p in parents[nid]:",
            "            pending_inputs[p] -= 1",
            "            if pending_inputs[p]==0:",
            "                print(f'[SCHEDULING] Node {p}')",
            "                f = executor.submit(make_task(p))",
            "                futures[p] = f",
            "                f.add_done_callback(_make_callback(p))",
            "    return _cb",
            "",
            "overall_start = time.perf_counter()",
            "",
            "# submit leaves",
            "for nid, cnt in pending_inputs.items():",
            "    if cnt==0:",
            "        print(f'[SCHEDULING] Node {nid}')",
            "        f = executor.submit(make_task(nid))",
            "        futures[nid] = f",
            "        f.add_done_callback(_make_callback(nid))",
            "",
            "# Wait for all tasks to complete",
            "root_finished.wait()",
            "executor.shutdown()",
            "",
            "overall_time = time.perf_counter() - overall_start",
            "total_node_time = sum(timings.values())",
            "",
            f"# Now dfs[{self.root.node_id}] should exist",
            f"result = dfs[{self.root.node_id}]",
            "print('\\nOutput\\n------------')",
            "print(result.head())",
            "print(f\"\\nMetrics: {{'total_node_time': {total_node_time}, 'overall_time': {overall_time}, 'intermediate_size': {total_intermediate_rows}, 'mem_usage': {mem_usage}, 'op_type': {op_type}}}\")"
        ]
