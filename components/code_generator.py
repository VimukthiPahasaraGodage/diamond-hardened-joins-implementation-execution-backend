import re
from typing import Dict, List

class PlanCodeGenerator:
    def __init__(self, root):
        self.root = root
        self.code_lines: List[str] = []

    def generate(self) -> str:
        self.code_lines = []
        self._emit_header()
        all_nodes = self._collect_nodes(self.root)

        self._emit_dependency_maps(all_nodes)
        self._emit_metadata_maps(all_nodes)
        self._emit_translators()
        self._emit_make_task()
        self._emit_scheduler()
        return "\n".join(self.code_lines)

    def _emit_header(self):
        self.code_lines += [
            "import re",
            "import pandas as pd",
            "from concurrent.futures import ThreadPoolExecutor",
            "from collections import defaultdict",
            "",
        ]

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
        self.code_lines.append("def _extract_table_name(table_attr):")
        self.code_lines.append("    return table_attr.replace('[[','').replace(']]','').split('.')[-1]")
        self.code_lines.append("")
        self.code_lines.append("table_name = {}")
        for n in nodes:
            if n.op_type == "BindableTableScan":
                self.code_lines.append(
                    f"table_name[{n.node_id}] = _extract_table_name(attrs[{n.node_id}].get('table',''))"
                )
        self.code_lines.append("")

    def _emit_translators(self):
        self.code_lines += [
            "# --- translators ---",
            "def translate_condition(cond):",
            "    s = cond",
            r"    s = re.sub(r\"LIKE\(\$(\d+),\s*'(.*?)'\)\",",
            r"                 r\"dfs_child['col\1'].str.contains(r'\2')\", s)",
            "    s = s.replace('AND', ' & ').replace('OR', ' | ')",
            "    s = s.replace('=', '==').replace('<>', '!=')",
            "    return s",
            "",
            "def translate_join(cond):",
            "    nums = re.findall(r\"\\$([0-9]+)\", cond)",
            "    if len(nums)==2:",
            "        return {'left_on': f'col{nums[0]}', 'right_on': f'col{nums[1]}'}",
            "    if len(nums)%2==0:",
            "        L = [f'col{nums[i]}' for i in range(0,len(nums),2)]",
            "        R = [f'col{nums[i]}' for i in range(1,len(nums),2)]",
            "        return {'left_on': L, 'right_on': R}",
            "    return {}",
            "",
            "def translate_aggs(at):",
            "    out = {}",
            "    for alias, expr in at.items():",
            "        m = re.search(r'MIN\\(\\$(\\d+)\\)', expr)",
            "        if m:",
            "            out[alias.lower()] = (f'col{m.group(1)}','min')",
            "    return out",
            "",
        ]

    def _emit_make_task(self):
        self.code_lines += [
            "# --- task factory (no blocking) ---",
            "def make_task(nid):",
            "    def task():",
            "        op = op_type[nid]",
            "        at = attrs[nid]",
            "        df = None",
            "        if op=='BindableTableScan':",
            "            df = pd.read_csv(f\"{table_name[nid]}.csv\")",
            "        elif op=='BindableValues':",
            "            df = pd.DataFrame([[]])",
            "        elif op=='BindableFilter':",
            "            child = children[nid][0]",
            "            dfs_child = dfs[child]",
            "            cond = translate_condition(at.get('condition',''))",
            "            df = dfs_child.query(cond)",
            "        elif op=='BindableProject':",
            "            child = children[nid][0]",
            "            df = dfs[child][list(at.keys())]",
            "        elif op=='BindableJoin':",
            "            l,r = children[nid]",
            "            jt = at.get('joinType','inner').strip('[]')",
            "            on = translate_join(at.get('condition',''))",
            "            df = pd.merge(dfs[l], dfs[r], how=jt, **on)",
            "        elif op=='BindableAggregate':",
            "            child = children[nid][0]",
            "            df = dfs[child].agg(translate_aggs(at)).reset_index(drop=True)",
            "        else:",
            "            df = pd.DataFrame()",
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
            "        dfs[nid] = fut.result()",
            "        for p in parents[nid]:",
            "            pending_inputs[p] -= 1",
            "            if pending_inputs[p]==0:",
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
            f"# wait for root {self.root.node_id}",
            f"result = futures[{self.root.node_id}].result()",
            "print(result.head())",
            "executor.shutdown()",
        ]
