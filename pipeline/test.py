import os
import shutil
import glob
import sys
import subprocess
from datetime import datetime, timedelta

script_dir = os.getcwd()                                # actual directory
ingest_dir = script_dir+"/ingest"                       # directory including ingest-files
import_script = "/opt/rasdaman/bin/wcst_import.sh"      # import script (rasdaman)
source_path = ""                                        # will be set through input parameter


def exit_fail(msg: str):
    """
    Using this function to print an error message before the execution of this script is stopped.
    It's just used for a better overview of the code.
    @param msg: The error message will be printed before stopping the execution
    """
    print(msg)
    sys.exit(1)


def term_shell(shell_args: str, err_message: str, clean: bool):
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


def convert_time(path):
    """
    Extracts time information from given path.

    This is needed to get the start and end time from the source directory.

    Args:
        path (str): the first or last element in the directory that is needed to get the time information

    Returns:
        datetime.datetime: time information
    """
    # b'/p/largedata/deeprain/<year>/<month>/<var>/<day>/
    time = path.split("/") # [p][largedata][deeprain][year][month][var][day]
    time = "{y}{m}{d}".format(y=time[-4], m=time[-3], d=time[-1])
    return time


def needs_to_create_missing(h, t):
    """
    Checks weather missing values are needed or not.

    This function checks weather a missing value must be inserted or not. Until the 5th of March 2013 (2013-03-05) only
    the forecast hours 00-21 where stored. Since we want to import forecast hours 00-24 this missing hours (22-24) must
    be filled up with missing values. So this function checks if the given time and given hour lead to a missing file.
    If an hour before 22 or a time after this 'border time' is given, no missing value needs to be created. Something
    went than wrong in the preprocessing since the data is available but not given here.

    Args:
        h (int): forecast hour that should be there but is not
        t (datetime): time for which the given forecast hour is not given

    Returns:
        bool: True if a missing value needs to be created. Otherwise False.
    """
    border_time = datetime.strptime("2013-03-05", "%Y-%m-%d")
    if h <= 21:
        return False
    if t > border_time:
        return False
    return True


def move_files(a_time, e_mem, tempdir, source_path):
    """
    Moves specified files to tempdir.

    Moves all files with the specified time (a_time) of the specified member to tempdir and renames it in the same
    step to <forecast_hour>.nc. All forecast_hours that exist in our data set will then stored in 'existing'.

    Args:
        a_time (datetime): specifies the time when the model run started
        e_mem (str): specifies the member

    Returns:
        list: all forecast hours that exist for this model run
    """
    os.makedirs(tempdir, exist_ok=True)
    # move input files to tempdir
    exis = []
    files_to_process = glob.glob(source_path + "/time:" + a_time.strftime("%Y%m%d-%H") + ".*.m" + e_mem + ".nc")
    files_to_process.sort()
    for act_file in files_to_process:
        outfile = tempdir + "/" + act_file.split(".")[-3] + ".nc"
        shutil.copy(act_file, outfile)
        exis.append(outfile.split("/")[-1])
    print(exis)
    return exis


def build_data(a_time, e_mem, exis, missing, tempdir, source_path, COMPRESS_LEVEL):
    """
    Builds one datafile out of the 25 forecast hours.

    Checks for each forecast hour if this exists in the data.
    If so this is added to the list. If not missing data will be inserted.
    After all hours are checked the defined datafiles will be merged together.

    Args:
        a_time (datetime): specifying the model run we are looking at
        e_mem (str): member we are looking at
        exis (list): describes all forecast hours that exist in our data set
    """
    in_files = " "
    hours = [str(h).zfill(2) + ".nc" for h in range(0, 25)]  # ["01.nc", "02.nc", ..]
    for hour in hours:
        if hour not in exis:  # create missing datafile to use as placeholder
            if needs_to_create_missing(hour, a_time):
                shell_args = "ncap2 -s 'time@units=\"hours since {0}\"' {1} {2}/{3}" \
                    .format(a_time.strftime("%Y-%m-%d %H:00:00"), missing, tempdir, hour)
                term_shell(shell_args, "changing time@units in missing file failed.")
                print("Added " + hour + " as missing value")
                print("changed time@units in missing file")
            else:
                exit_fail("Forcast hour {} is missing from time {}".format(hour, datetime.strftime("%Y%m%d")))
        in_files = in_files + tempdir + "/" + hour + " "
    # create the name of the datafile that holds all information from before
    out_file = "{0}/processed:{1}.m{2}.nc".format(source_path, a_time.strftime("%Y%m%d%H"), e_mem)
    shell_args = "cdo{0}-s -mergetime {1} {2}".format(COMPRESS_LEVEL, in_files, out_file)  # merge the given files
    term_shell(shell_args, "Failed merging time steps")
    print("Merging       ...ok")


def remove_data(a_time: datetime, e_mem, source_path, tempdir):
    """
    All specified files where removed.

    All datafiles of the given time (a_time) with the given member will be removed.
    Afterwards the temporary directory will be removed.
    This is done after the datafiles where merged together.

    Args:
        a_time (datetime): specifies the time attribute of the files that should be deleted
        e_mem (str): specifies the member, which files should be removed
    """
    for data_file in glob.glob("{0}/time:{1}.*.m{2}.nc".format(source_path, a_time.strftime("%Y%m%d-%H"), e_mem)):
        os.remove(data_file)
    shutil.rmtree(tempdir)


def main():
    # ==== Checking input ==============================================================================================
    if len(sys.argv) > 1:
        if os.path.isdir(sys.argv[1]):
            source_path = os.path.abspath(sys.argv[1])  # use absolute path just to avoid unexpected behaviour
        else:
            exit_fail("Path did not exist: " + sys.argv[1])
    else:
        exit_fail("Please specify a path.")

    if not os.path.isdir(ingest_dir):
        exit_fail("Invalid ingest_idr: " + ingest_dir)

    relative_tempdir = "{dir}/tempdir".format(dir=source_path)
    # ==== extract start and end_time ==============================================================
    start_time = convert_time(source_path)
    end_time = datetime.strptime("{date}-{hour}".format(date=start_time, hour="21"), "%Y%m%d-%H")
    start_time = datetime.strptime("{date}-{hour}".format(date=start_time, hour="00"), "%Y%m%d-%H")

    print("Import files from {0} to {1}".format(start_time.strftime("%Y-%m-%d:%H"),
                                                end_time.strftime("%Y-%m-%d:%H")))

    actual_time = start_time

    # ==== Build Data to import ====================================================================
    while actual_time <= end_time:
        members = [str(m).zfill(2) for m in range(1, 21)]  # ["01", "02", ..]
        for member in members:
            existing = move_files(actual_time, member)  # move all founded files to tempdir
            build_data(actual_time, member, existing)  # build one datafile for actual_time for that member
            remove_data(actual_time, member)  # remove all datafiles that where used to build the file above

        actual_time = actual_time + timedelta(0, 10800)  # add three hours (go to next model run)

if __name__ == "__main__":
    # execute only if run as a script
    main()