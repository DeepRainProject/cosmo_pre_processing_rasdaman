#!/usr/bin/env python3

"""
Script for registering the preprocessed data in rasdaman Enterprise.

The files are already preprocessed and only need to be inserted to rasdaman.

Execution: ./register.py /<VAR>/<day> where <VAR> is the actual variable that should be imported
"""

# ==== imports    ======================================================================================================

import glob
import os
import shutil
import subprocess
import sys


# ==== constants  ======================================================================================================
COMPRESS_LEVEL = " -z zip_2 "  # compress the data for faster import and lower memory usage
# COMPRESS_LEVEL = " "                                  # if no compression is wished

script_dir = os.getcwd()  # actual directory
ingest_dir = script_dir + "/ingest"  # directory including ingest-files (.json)
import_script = "/opt/rasdaman/bin/wcst_import.sh"  # import script (rasdaman)
rasdaman_endpoint = "http://134.94.199.213:8080/rasdaman/ows"
source_path = ""  # will be set through input parameter
ingest_template_file = ""  # file out of which the ingest-file is created, defined in main()
ingest_file = ""  # ingest_file name which is used to import data

if not os.path.exists(import_script):
    import_script = "/home/dimitar/rasdaman/enterprise/src-install/bin/wcst_import.sh"
    rasdaman_endpoint = "http://localhost:8080/rasdaman/ows"


# ==== functions =======================================================================================================

def exit_fail(msg):
    """
    Printing error message and stops execution.

    Using this function to print an error message before the execution of this script is stopped.
    It's just used for a better overview of the code.

    Args:
        msg (str): error message that is printed before stopping the execution
    """
    print(msg)
    sys.exit(1)


def term_shell(shell_args, err_message):
    """
    Execution of shell arguments.

    This function gets arguments "shell_args" what should be executed as a shell command.
    If this does not work "err_message" is printed and the program will exit.

    Args:
        shell_args (str): arguments that should be executed in shell
        err_message (str): message that should be printed if the execution fails
    """
    return_code = subprocess.call(shell_args, shell=True)
    if return_code != 0:
        exit_fail("Command failed: " + shell_args + "\nReason: " + err_message)


def create_ingest(import_files):
    """
    Creates the ingest-file that is needed to import the data.

    First the ingest-template file will be copied.
    The copy will be our ingest-file. Therefore the placeholder "FILE_PATH" must be replaced.
    We use the parameter "files" to replace the placeholder.

    Args:
        import_files (str): files that should be imported (replace the placeholder)

    Returns:
        str: path to created ingest-file
    """
    shutil.copyfile(ingest_dir + "/" + ingest_template_file, source_path + "/" + ingest_file)
    path_ingest_file = glob.glob(source_path + "/" + ingest_file)[0]
    shell_args = "sed -i -e 's@FILE_PATH@" + import_files + "@g' '" + path_ingest_file + "'"
    term_shell(shell_args, "Replacement failed")
    return path_ingest_file


def main():
    """
    Register the files of the given directory.

    1. Check the given input parameter, constants and files
    2. Define paths needed for execution
    3. Create ingest file
    4. Register data (using ingest file)
    5. Clean things up
    """
    # ==== 1. Step === Checking input ==================================================================================
    if len(sys.argv) > 1:
        if os.path.isdir(sys.argv[1]):
            source_path = os.path.abspath(sys.argv[1])  # use absolute path just to avoid unexpected behaviour
        else:
            exit_fail("Path did not exist: " + sys.argv[1])
    else:
        exit_fail("Please specify a path.")

    if not os.path.isdir(ingest_dir):
        exit_fail("Invalid ingest_dir: " + ingest_dir)
    if not os.path.isfile(import_script):
        exit_fail("Path to import_script does not exist: " + import_script)
    if not os.access(import_script, os.X_OK):
        exit_fail("Import_script is not executable: " + import_script)

    # ==== 2. Step === Define paths needed for execution ===============================================================

    variable = str(source_path.split("/")[-2])  # get variable (source path should be /<VAR>/<day>)
    ingest_template_file = "ingest-" + variable + ".json.template"  # save the needed ingest-file (template)
    ingest_file = "ingest-" + variable + ".json"  # save name of used ingest-file

    # ==== 3. Step === Create ingest file ==============================================================================
    files = "{0}/processed:*.*.nc".format(source_path)
    ingest_file_path = create_ingest(files)

    # ==== 4. Step === Register data ===================================================================================
    args = import_script + " " + ingest_file_path
    term_shell(args, "Import failed")

    # ==== 5. Step === Clean things up =================================================================================
    os.remove(ingest_file_path)
        for datafile in glob.glob(files):

    print("Files of {0} registered.".format(source_path))


if __name__ == "__main__":
    # execute only if run as a script
    main()
