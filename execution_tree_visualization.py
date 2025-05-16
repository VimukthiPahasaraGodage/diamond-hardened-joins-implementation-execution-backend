import html
from typing import Optional

from graphviz import Digraph

from execution_tree import PlanNode


def escape_label(s):
    return html.escape(str(s))


def visualize_plan_tree(root: PlanNode, filename: Optional[str] = "plan_tree") -> Digraph:
    dot = Digraph(format='png')
    dot.attr(rankdir='TB')  # Top to bottom

    def add_nodes_edges(node: PlanNode):
        node_label = f"{node.op_type}\\n(id={node.node_id})"
        for k, v in node.attributes.items():
            node_label += f"\\n{k}=[{v}]"
        node_label = escape_label(node_label)
        dot.node(str(node.node_id), label=node_label, shape="box", style="filled", fillcolor="lightgray")

        for child in node.children:
            dot.edge(str(node.node_id), str(child.node_id))
            add_nodes_edges(child)

    add_nodes_edges(root)
    dot.render(filename, view=True)
    return dot
