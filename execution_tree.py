import re
from dataclasses import dataclass, field
from typing import List, Dict, Tuple


@dataclass
class PlanNode:
    op_type: str
    attributes: Dict[str, str]
    node_id: int
    children: List["PlanNode"] = field(default_factory=list)


def parse_plan(plan_text: str) -> PlanNode:
    lines = [line.rstrip() for line in plan_text.strip().split("\n") if line.strip()]
    stack: List[Tuple[int, PlanNode]] = []

    node_pattern = re.compile(r"(\w+)\((.*?)\), id\s*=\s*(\d+)")
    attr_pattern = re.compile(r"(\w+)=\[+(.*?)\]+")

    for line in lines:
        indent = len(line) - len(line.lstrip())
        match = node_pattern.match(line.strip())
        if not match:
            continue

        op_type, raw_attrs, node_id = match.groups()
        node_id = int(node_id)

        # Parse attributes
        attributes = {k: v for k, v in attr_pattern.findall(raw_attrs)}

        node = PlanNode(op_type, attributes, node_id)

        # Attach to parent
        while stack and stack[-1][0] >= indent:
            stack.pop()
        if stack:
            stack[-1][1].children.append(node)

        stack.append((indent, node))

    # Root node is the first one in the stack
    return stack[0][1] if stack else None
