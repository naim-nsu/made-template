import os
import shutil

def check_config_file():
    # Path to the config directory
    directory_path = "./project/config"

    # Name of the file you're checking for
    file_to_check = "config_var.py"

    # Name of the file to duplicate
    file_to_duplicate = "config_var.example.py"

    # Check if the file exists in the specified directory
    if os.path.isfile(os.path.join(directory_path, file_to_check)):
        print(f"File '{file_to_check}' exists.")
    else:
        if os.path.isfile(os.path.join(directory_path, file_to_duplicate)):
            # Duplicate the file if it exists and rename it
            shutil.copy(os.path.join(directory_path, file_to_duplicate), os.path.join(directory_path, file_to_check))
            print(f"File '{file_to_check}' created by duplicating '{file_to_duplicate}'.")
        else:
            print(f"File '{file_to_duplicate}' does not exist to duplicate.")
            exit()

if __name__ == "__main__":
    check_config_file()
    
    #run the data pipeline
    from pipeline import DataPipeline
    data_pipeline = DataPipeline()
    data_pipeline.run_pipeline()

    