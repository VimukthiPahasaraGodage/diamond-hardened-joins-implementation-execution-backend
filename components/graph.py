import re
from dataclasses import dataclass, field
from typing import List, Dict, Tuple


@dataclass
class PlanNode:
    op_type: str
    attributes: Dict[str, str]
    node_id: int
    children: List["PlanNode"] = field(default_factory=list)


def strip_condition_brackets(text):
    if text.startswith("condition=[") and text.endswith("]"):
        return text[len("condition=["):-1]
    raise ValueError("The attributes does not match the pattern")


def extract_table_name(line):
    match = re.search(r'table=\[\[(.*?)\]\]', line)
    if match:
        return match.group(1)
    else:
        raise ValueError("The attributes does not match the pattern")


def remove_bindable_prefix(s):
    prefix = "Bindable"
    if s.startswith(prefix):
        return s[len(prefix):]
    raise ValueError("The attributes does not match the pattern")


def parse_plan(plan_text: str) -> PlanNode:
    lines = [line.rstrip() for line in plan_text.strip().split("\n") if line.strip()]
    stack: List[Tuple[int, PlanNode]] = []

    node_pattern = re.compile(r"(\w+)\((.*?)\), id\s*=\s*(\d+)")
    attr_pattern = re.compile(r"(\w+)=\[(.*?)]")

    for line in lines:
        indent = len(line) - len(line.lstrip())
        match = node_pattern.match(line.strip())
        if not match:
            continue

        op_type, raw_attrs, node_id = match.groups()
        node_id = int(node_id)

        if op_type == "BindableFilter":
            attributes = {'condition': strip_condition_brackets(raw_attrs)}
        elif op_type == "BindableAggregate":
            attributes = {'aggregation': raw_attrs}
        elif op_type == "BindableJoin":
            attributes = {k: v for k, v in attr_pattern.findall(raw_attrs)}
        elif op_type == "BindableProject":
            attributes = {'projection': raw_attrs}
        elif op_type == "BindableTableScan":
            attributes = {'table': extract_table_name(raw_attrs)}
        elif op_type == "BindableValues":
            attributes = {'values': []}
        else:
            raise ValueError(f"No such operation type: {op_type}")

        node = PlanNode(remove_bindable_prefix(op_type), attributes, node_id)

        # Attach to parent
        while stack and stack[-1][0] >= indent:
            stack.pop()
        if stack:
            stack[-1][1].children.append(node)

        stack.append((indent, node))

    # Root node is the first one in the stack
    return stack[0][1] if stack else None
