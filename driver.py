import os
import shutil
import runpy
import sys

from components.engine import ExecutionEngine

class TeeOutput:
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
        self.flush()

    def flush(self):
        for stream in self.streams:
            stream.flush()

if __name__ == '__main__':
    calcite_output_file = f'{os.getcwd()}/calcite_outputs/test_plan.txt'.replace("\\", '/')
    execution_tree_visualizations_folder = f'{os.getcwd()}/visualization_outputs'.replace("\\", '/')
    generated_codes_folder = f'{os.getcwd()}/generated_codes'.replace("\\", '/')
    logs_folder = f'{os.getcwd()}/query_outputs'.replace("\\", '/')
    csv_dataset_path = 'C:/JOB_dataset'

    if os.path.exists(execution_tree_visualizations_folder):
        shutil.rmtree(execution_tree_visualizations_folder)
    os.makedirs(execution_tree_visualizations_folder)

    if os.path.exists(generated_codes_folder):
        shutil.rmtree(generated_codes_folder)
    os.makedirs(generated_codes_folder)

    if os.path.exists(logs_folder):
        shutil.rmtree(logs_folder)
    os.makedirs(logs_folder)

    engine = ExecutionEngine(csv_dataset_path, calcite_output_file, execution_tree_visualizations_folder, generated_codes_folder)
    generated_code_paths = engine.execute_queries(join_method='auto', visualize=True, std_out_code=False)

    for i, generated_code_path in enumerate(generated_code_paths):
        log_path = os.path.join(logs_folder, f"query_{i}.log")
        print(f"Executing query {i} ...")

        with open(log_path, "w", encoding="utf-8") as log_file:
            tee = TeeOutput(sys.stdout, log_file)
            original_stdout = sys.stdout
            sys.stdout = tee
            try:
                runpy.run_path(generated_code_path, run_name="__main__")
            finally:
                sys.stdout = original_stdout
