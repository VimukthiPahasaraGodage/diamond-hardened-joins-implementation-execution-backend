import os

from execution_engine import ExecutionEngine

if __name__ == '__main__':
    calcite_output_file = f'{os.getcwd()}/calcite_outputs/physical_plans_1.txt'
    execution_tree_visualizations_folder = f'{os.getcwd()}/visualization_outputs'

    engine = ExecutionEngine(calcite_output_file, execution_tree_visualizations_folder)
    engine.execute_queries()
