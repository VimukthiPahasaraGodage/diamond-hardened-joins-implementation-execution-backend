import os
import shutil
import runpy
import sys
import argparse
from pathlib import Path

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

def parse_args():
    parser = argparse.ArgumentParser(description="Execute queries from generated Calcite plans.")

    parser.add_argument('--calcite_output_file', type=str,
                        required=True,
                        help='Path to the Calcite output file')

    parser.add_argument('--execution_tree_visualizations_folder', type=str,
                        required=True,
                        help='Folder to store execution tree visualizations')

    parser.add_argument('--generated_codes_folder', type=str,
                        required=True,
                        help='Folder to store generated Python codes')

    parser.add_argument('--logs_folder', type=str,
                        required=True,
                        help='Folder to store logs of executed queries')

    parser.add_argument('--csv_dataset_path', type=str,
                        required=True,
                        help='Path to the CSV dataset folder')

    parser.add_argument('--join_method', type=str,
                        default='hash-join',
                        help='auto, hash-join')

    parser.add_argument('--visualize', type=int, default=1, help='visualize the execution ready relational tree (0 or 1)')
    parser.add_argument('--std_out_code', type=int, default=0, help='Whether to print the code generated for execution of the query (0 or 1)')
    parser.add_argument('--opt', type=str, default='normal', help='Optimization method to use (norma/ LE-decomposition')

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    if os.path.exists(args.execution_tree_visualizations_folder):
        shutil.rmtree(args.execution_tree_visualizations_folder)
    os.makedirs(args.execution_tree_visualizations_folder)

    if os.path.exists(args.generated_codes_folder):
        shutil.rmtree(args.generated_codes_folder)
    os.makedirs(args.generated_codes_folder)

    if os.path.exists(args.logs_folder):
        shutil.rmtree(args.logs_folder)
    os.makedirs(args.logs_folder)

    engine = ExecutionEngine(
        args.csv_dataset_path,
        args.calcite_output_file,
        args.execution_tree_visualizations_folder,
        args.generated_codes_folder
    )
    visualize = True if args.visualize == 1 else False
    std_out_code = True if args.std_out_code == 1 else False
    generated_code_paths = engine.execute_queries(join_method=args.join_method, opt=args.opt, visualize=args.visualize, std_out_code=args.std_out_code)

    if generated_code_paths:
        for i, generated_code_path in enumerate(generated_code_paths):
            log_path = os.path.join(args.logs_folder, f"{Path(args.calcite_output_file).stem}.log")
            print(f"\nExecuting query ...")

            with open(log_path, "w", encoding="utf-8") as log_file:
                tee = TeeOutput(sys.stdout, log_file)
                original_stdout = sys.stdout
                sys.stdout = tee
                try:
                    runpy.run_path(generated_code_path, run_name="__main__")
                finally:
                    sys.stdout = original_stdout
