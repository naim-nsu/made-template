#!/bin/bash


# Path to the directory where you want to check for the file
directory_path="/project/config"

# Name of the file you're checking for
file_to_check="config_var.py"

# Name of the file to duplicate
file_to_duplicate="config_var.example.py"


# Check if the file exists in the specified directory
if [ -f "${directory_path}/${file_to_check}" ]; then
    echo "File '${file_to_check}' exists."
else
    if [ -f "${directory_path}/${file_to_duplicate}" ]; then
        # Duplicate the file if it exists and rename it
        cp "${directory_path}/${file_to_duplicate}" "${directory_path}/${file_to_check}"
        echo "File '${file_to_check}' created by duplicating '${file_to_duplicate}'."
    else
        echo "File '${file_to_duplicate}' does not exist to duplicate."
    fi
fi

python ./project/tests.py