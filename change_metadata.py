import os
import shutil
import glob
import sys
import subprocess
from datetime import datetime, timedelta


def exit_fail(msg: str):
    """
    Using this function to print an error message before the execution of this script is stopped.
    It's just used for a better overview of the code.
    @param msg: The error message will be printed before stopping the execution
    """
    print(msg)
    sys.exit(1)


def term_shell(shell_args: str, err_message: str):
    """
    This function gets arguments "shell_args" what should be executed as a shell command.
    If this does not work "err_message" is printed and the program will exit.
    @param shell_args: arguments that should be executed in shell
    @param err_message: message that should be printed if the execution fails
    @param clean: specifies weather a cleanup should be executed
    """
    return_code = subprocess.call(shell_args, shell=True)
    if return_code != 0:
        print("term_shell is failed in progress for command: {shell_args}".format(shell_args = shell_args))
        exit_fail("Command failed: " + shell_args + "\nReason: " + err_message)


def delete_steps(step):
    os.remove(step)


def main():
    # ==== Checking input ==============================================================================================
    source_path = ""
    if len(sys.argv) > 1:
        if os.path.isdir(sys.argv[1]):
            source_path = os.path.abspath(sys.argv[1])  # use absolute path just to avoid unexpected behaviour
            print(source_path)
        else:
            exit_fail("Path did not exist: " + sys.argv[1])
    else:
        exit_fail("Please specify a path.")

    # definition of temporary directories
    act_dir = os.getcwd()
    step1 = act_dir + "/step1.nc"
    step2 = act_dir + "/step2.nc"

    # files to process
    files = glob.glob("{0}/*".format(source_path))
    files.sort()

    for file in files:
        print(file)

        # change variable names (rlat->lat and rlon->lon)
        shell_args = "cdo chname,rlat,lat,rlon,lon {in_file} {out_file}".format(in_file=file, out_file=step1)
        term_shell(shell_args, "Failed change variable names")
        print("Variable changed  ...ok")

        # change lat and lon long_names
        args = "ncap2 -s 'lon@long_name=\"longitude\"' -s 'lon@units=\"degrees_east\"' -s 'lon@axis=\"X\"'" \
               "-s 'lat@long_name=\"latitude\"' -s 'lat@units=\"degrees_north\"' -s 'lat@axis=\"Y\"' " \
               "{in_file} {out_file}".format(in_file=step1, out_file=step2)
        term_shell(args, "Change long_names failed")
        print("Long name changed ...ok")

        # delete original file and use it as output of next step
        delete_steps(file)

        # change history
        shell_args = "ncatted -h -a history,global,d,, {in_file} {out_file}".format(text="", in_file=step2, out_file=file)
        term_shell(shell_args, "Failed rewriting history")
        print("History changed   ...ok")

        # delete temporary files
        delete_steps(step1)
        delete_steps(step2)

if __name__ == "__main__":
    # execute only if run as a script
    main()
