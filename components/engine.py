import os
import shutil
import copy
import psutil
import pandas as pd
import time
import math
import threading
from collections import defaultdict
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Union, Dict
from pathlib import Path

from components.code_generator import PlanCodeGenerator
from components.graph import PlanNode
from components.graph import parse_plan
from components.graph_visualization import visualize_plan_tree

LE = 1
expand_node_id = 5_000_000_000
finalized = False
optimized = False


class OptimizationEngine:
    def __init__(self, root_node):
        self.tree = root_node
        self.opt_tree = None
        self.opt_tree_text = None

    @staticmethod
    def generate_physical_plan_text(node: PlanNode, indent: int = 0) -> str:
        indent_space = "  " * indent
        op_type = node.op_type

        # Re-add the "Bindable" prefix to match the original plan output
        bindable_op_type = f"Bindable{op_type}"

        # Format attributes based on the type
        if op_type == "Filter":
            attr_text = f"condition=[{node.attributes['condition']}]"
        elif op_type == "Aggregate":
            attr_text = node.attributes['aggregation']
        elif op_type == "Join":
            attr_text = ', '.join(f"{k}=[{v}]" for k, v in node.attributes.items())
        elif op_type == "Project":
            attr_text = node.attributes['projection']
        elif op_type == "TableScan":
            attr_text = f"table=[[{node.attributes['table']}]]"
        elif op_type == "Values":
            attr_text = ""
        elif op_type == "Lookup":
            attr_text = ', '.join(f"{k}=[{v}]" for k, v in node.attributes.items())
        elif op_type == "Expand":
            attr_text = ', '.join(f"{k}=[{v}]" for k, v in node.attributes.items())
        else:
            raise ValueError(f"Unknown operation type: {op_type}")

        line = f"{indent_space}{bindable_op_type}({attr_text}), id = {node.node_id}"
        child_lines = [OptimizationEngine.generate_physical_plan_text(child, indent + 1) for child in node.children]
        return "\n".join([line] + child_lines)

    @staticmethod
    def swap_joins_with_lookup_and_expand(node):
        global LE
        global expand_node_id
        if node.op_type != "Join" or node.attributes['joinType'] != 'inner':
            return None
        if node.children[0].op_type == "Join" and node.children[0].attributes['joinType'] == 'inner':
            new_node = PlanNode(op_type="Expand", attributes={'lookup_id': node.node_id, 'LE': LE},
                                node_id=expand_node_id)
            new_node.children = [
                PlanNode(op_type="Expand", attributes={'lookup_id': node.children[0].node_id, 'LE': LE},
                         node_id=expand_node_id + 1)]
            _node = copy.deepcopy(node)
            _node.op_type = "Lookup"
            _node.attributes['LE'] = LE
            _node.children[0].op_type = "Lookup"
            _node.children[0].attributes['LE'] = LE
            new_node.children[0].children = [_node]
            LE += 1
            expand_node_id += 2
            return new_node
        elif node.children[1].op_type == "Join" and node.children[1].attributes['joinType'] == 'inner':
            new_node = PlanNode(op_type="Expand", attributes={'lookup_id': node.node_id, 'LE': LE}, node_id=expand_node_id)
            new_node.children = [
                PlanNode(op_type="Expand", attributes={'lookup_id': node.children[1].node_id, 'LE': LE}, node_id=expand_node_id + 1)]
            _node = copy.deepcopy(node)
            _node.op_type = "Lookup"
            _node.attributes['LE'] = LE
            _node.children[1].op_type = "Lookup"
            _node.children[1].attributes['LE'] = LE
            new_node.children[0].children = [_node]
            LE += 1
            expand_node_id += 2
            return new_node
        else:
            return None

    @staticmethod
    def transform(node, root_node_id):
        global finalized
        global optimized
        for i, child in enumerate(node.children):
            new_node = OptimizationEngine.transform(child, root_node_id)
            if new_node:
                node.children[i] = new_node
                finalized = True
                optimized = True

        chain = None
        if not finalized:
            chain = OptimizationEngine.swap_joins_with_lookup_and_expand(node)

        if node.node_id == root_node_id:
            chain = node
        return chain

    def get_optimized_tree_text(self):
        if self.opt_tree:
            return OptimizationEngine.generate_physical_plan_text(self.opt_tree)
        else:
            self.get_optimized_tree()
            return OptimizationEngine.generate_physical_plan_text(self.opt_tree)

    def get_optimized_tree(self):
        global finalized
        global optimized
        while True:
            finalized = False
            optimized = False
            self.opt_tree = OptimizationEngine.transform(self.tree, self.tree.node_id)

            if not optimized:
                break
        return self.opt_tree

class ExecutionEngine:
    def __init__(self, csv_dataset_path: str, calcite_output_file: str, execution_tree_visualizations_folder: str,
                 save_folder: str):
        self.csv_dataset_path = csv_dataset_path
        self.calcite_output_file = calcite_output_file
        self.execution_tree_visualizations_folder = execution_tree_visualizations_folder
        self.save_folder = save_folder
        self.calcite_output_filename = Path(calcite_output_file).stem

        os.makedirs(self.save_folder, exist_ok=True)

        if os.path.exists(execution_tree_visualizations_folder):
            shutil.rmtree(execution_tree_visualizations_folder)

        os.makedirs(execution_tree_visualizations_folder, exist_ok=True)

    def extract_sql_query_and_physical_plan_from_calcite_output(self):
        result = []
        with open(self.calcite_output_file, 'r', encoding='utf-8') as file:
            content = file.read()

        # Split the content by the separator
        entries = content.split(
            '------------------------------------------------------------------------------------------------------------------')

        for entry in entries:
            current_entry = {}
            capture_mode = None
            buffer = []

            lines = entry.splitlines()

            for line in lines:
                stripped_line = line.strip()

                if stripped_line.startswith("[SQL Query]"):
                    # Save previous entry if complete
                    if 'SQL Query' in current_entry and 'Physical plan' in current_entry:
                        result.append(current_entry)
                    current_entry = {}
                    capture_mode = 'SQL Query'
                    buffer = []
                    continue
                elif stripped_line.startswith("[Physical plan]"):
                    if capture_mode and buffer:
                        current_entry[capture_mode] = '\n'.join(buffer).strip()
                    capture_mode = 'Physical plan'
                    buffer = []
                    continue
                elif stripped_line.startswith("[") and stripped_line.endswith("]"):
                    # Save current buffer if switching to a new section
                    if capture_mode and buffer:
                        current_entry[capture_mode] = '\n'.join(buffer).strip()
                    capture_mode = None
                    buffer = []
                    continue

                if capture_mode:
                    buffer.append(line.rstrip())

            # Catch the last entry if needed
            if capture_mode and buffer:
                current_entry[capture_mode] = '\n'.join(buffer).strip()
            if 'SQL Query' in current_entry and 'Physical plan' in current_entry:
                result.append(current_entry)

        return result

    def parse_physical_plan_to_execution_tree(self):
        execution_trees = []
        extracted_physical_plans = self.extract_sql_query_and_physical_plan_from_calcite_output()
        for physical_plan in extracted_physical_plans:
            root_node = parse_plan(physical_plan['Physical plan'])
            execution_trees.append(root_node)
        return execution_trees

    def visualize_execution_tree(self, root_node: PlanNode, identifier: str):
        visualization_filename = f"{self.execution_tree_visualizations_folder}/{identifier}"
        visualize_plan_tree(root_node, filename=visualization_filename)

    def generate_execution_code_for_execution_tree(self, root_node: PlanNode, join_method: str):
        generated_code = PlanCodeGenerator(root_node, self.csv_dataset_path, join_method).generate()
        return generated_code

    def execute_queries(self, join_method: str = 'auto', opt: str = 'normal', visualize: bool = True, std_out_code: bool = False):
        execution_trees = self.parse_physical_plan_to_execution_tree()
        index = 0
        generated_code_paths = []
        for root_node in execution_trees:
            if visualize:
                self.visualize_execution_tree(root_node, Path(self.calcite_output_file).stem + "_normal_opt")

            if opt == 'LE-decomposition':
                opt_engine = OptimizationEngine(root_node)
                root_node = opt_engine.get_optimized_tree()
                if not root_node:
                    print("Lookup and Expand decomposition optimization failed!")
                else:
                    opt_tree_text = f"\n[Physical Plan - Lookup & Expand Optimized]\n{opt_engine.get_optimized_tree_text()}"
                    print(opt_tree_text)

                    with open(self.calcite_output_file, "a", encoding="utf-8") as file:
                        file.write(opt_tree_text)

                    if visualize:
                        self.visualize_execution_tree(root_node, Path(self.calcite_output_file).stem + "_LE_opt")

            code = self.generate_execution_code_for_execution_tree(root_node, join_method)

            if std_out_code:
                print(
                    "\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print(code)
                print(
                    "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

            file_path = os.path.join(self.save_folder, f"{Path(self.calcite_output_file).stem}.py")
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(code)
            generated_code_paths.append(file_path)
            index += 1
        return generated_code_paths
