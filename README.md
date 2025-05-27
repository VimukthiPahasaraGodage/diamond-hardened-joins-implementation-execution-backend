This repo contains the Python Query Execution Engine developed for the project in the repo: https://github.com/VimukthiPahasaraGodage/diamond-hardened-joins-implementation-on-apache-calcite

# Usage

1. Stand-alone usage

    Given a query plan(a physical plan in the format that Apache Calcite outputs), ```driver.py``` is able to execute that query using code generation.
    
    ```
    python driver.py
        --calcite_output_file Path to the Calcite output file
        --execution_tree_visualizations_folder Folder to store execution tree visualizations
        --generated_codes_folder Folder to store generated Python codes
        --logs_folder Folder to store logs of executed queries
        --csv_dataset_path Path to the CSV dataset folder
        --join_method 'auto' or 'hash-join'
        --visualize '1' r '0'
        --std_out_code '1' or '0'
        --opt 'LE-decomposition' or 'normal'
    ```
2. Usage with Database Engine in the repo: https://github.com/VimukthiPahasaraGodage/diamond-hardened-joins-implementation-on-apache-calcite

    Use the following commands to generate an executable file(Windows) and place the executable file in a folder and notedown the path for the executable to use in Database engine.

    ```
    pip install PyInstaller
   pyinstaller --onefile driver.py
   ```

