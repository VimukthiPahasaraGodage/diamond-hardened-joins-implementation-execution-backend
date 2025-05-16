import os
from pathlib import Path

from graph import PlanNode
from graph import parse_plan
from graph_visualization import visualize_plan_tree
from code_generator import PlanCodeGenerator


class ExecutionEngine:
    def __init__(self, calcite_output_file: str, execution_tree_visualizations_folder: str):
        self.calcite_output_file = calcite_output_file
        self.execution_tree_visualizations_folder = execution_tree_visualizations_folder
        os.makedirs(execution_tree_visualizations_folder, exist_ok=True)
        self.calcite_output_filename = Path(calcite_output_file).stem

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

    def visualize_execution_tree(self, root_node: PlanNode, index: int):
        visualization_filename = f"{self.execution_tree_visualizations_folder}/{self.calcite_output_filename}_{index}"
        visualize_plan_tree(root_node, filename=visualization_filename)

    @staticmethod
    def generate_execution_code_for_execution_tree(root_node: PlanNode):
        generated_code = PlanCodeGenerator(root_node).generate()
        return generated_code

    def execute_queries(self, visualize=False):
        execution_trees = self.parse_physical_plan_to_execution_tree()
        index = 0
        for root_node in execution_trees:
            if visualize:
                self.visualize_execution_tree(root_node, index)
            code = self.generate_execution_code_for_execution_tree(root_node)
            print(code)
