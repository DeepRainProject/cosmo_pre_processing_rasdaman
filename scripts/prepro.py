#!/usr/bin/env python3

"""
This script converts the data given from DWD to netCdf and splits it per variable and after this into the 8 available
time steps since they will get the later ansi-axis and importing data in between existing data in rasdaman is not
possible. The splitting into the different variables is necessary since we import every variable separately
Also the grid of the datafiles is wrong so we change from 'rotated pole grid' to an equidistant grid

Execution: ./01-prepro.py <source path>, where <source path> containing the 'cdeYYYYMMDD.HH.mEE.grib-files'
"""


# ===== imports    =====================================================================================================
import sys
import os
import subprocess
import shutil
import glob
import time as tm
from datetime import datetime, timedelta

# ===== constants  =====================================================================================================
script_dir = os.getcwd()                        # the actual directory containing all necessary files
rot_grid = script_dir + "/gridneu.dat"          # this file describes how to change the grids
destination = script_dir + "/preproc"           # our working directory
ingest_dir = script_dir + "/ingest"             # this directory contains all ingest=files
# TODO
# change for parallel execution
split_dir = script_dir + "/split"               # temporary directory for splitting into variables
filter_file = script_dir + "/split_filter.txt"  # needed file for splitting the data into variables
source_path = ""                                # will be set through input parameter

MAX_HOUR = 24                                   # the maximum hour we want to import
# COMPRESS_LEVEL = " -z zip_2 "                   # compress the data for faster import and lower memory usage
COMPRESS_LEVEL = " "                          # if no compression is wished

ingestions = []                                 # using extract_ingestions we get all possible ingestion files

log_dir = script_dir+"/log"                      # this is the directory the error files will be stored in
err_file_name = ""                              # this is the filename, where the errors will be logged
LOGGING = True                                  # decides weather it should be logged or not


# ===== functions  =====================================================================================================
def cleanup():
    """
    If an error occurs and the script stops it's execution, first it will call this function to delete the temporary
    splitting directory and the split file. This is only in case, the script is already splitting something. All other
    files (processed or not) will stay at their actual location and will not be deleted.
    """
    shutil.rmtree(split_dir)
    os.remove(filter_file)
    print("Cleaned up")


def exit_fail(msg: str):
    """
    Using this function to print an error message before the execution of this script is stopped.
    It's just used for a better overview of the code.
    If LOGGING is true, this also will be written to the logging file 'err_file_name'
    @param msg: The error message will be printed before stopping the execution
    """
    print(msg)
    if LOGGING:
        writer = open(err_file_name, "w")
        writer.write(msg)
        writer.close()
    sys.exit(1)


def get_forecast_hour(filename):
    """
    This function returns the forecast hour of the given file using its name.
    The hour is stored in the third last component:
    cdeYYYYMMDD.HH.mEE.grib2
    @param filename: specifies the filename where the hour should be extracted from
    @return: the hour extracted of the filename (forecast hour)
    """
    input_file_name = os.path.basename(filename)
    try:
        return int(os.path.basename(filename).split(".")[-3])
    except ValueError:
        # sample data on b2drop has the hour in the second component
        return int(os.path.basename(filename).split(".")[1])


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
        if clean:
            cleanup()
        exit_fail("Command failed: " + shell_args + "\nReason: " + err_message)


def extract_ingestions():
    """
    This function extracts all variable names that can be imported later since we have the ingest files in the
    ingest_dir.
    @return: a list containing all ingestion files that are given in the ingest_dir
    """
    inges = []
    for var in glob.glob(ingest_dir + "/ingest-*.json.template"):
        parts = os.path.basename(var)               # ingest-<VAR>.json.template
        parts = parts.split('.')[0]                 # ingest-<VAR>
        parts = parts.split('-')[1]                 # <VAR>
        inges.append(parts)
    if not inges:
        # since no ingest files are given we can not import something later. So we do not need to preprocess something.
        exit_fail("No ingestion scripts given.")
    else:
        print("Split data into: " + ' '.join(inges))
    return inges


def create_split_dir():
    """
    If the specified split directory already exists, it will be deleted and newly created
    """
    if os.path.isdir(split_dir):
        shutil.rmtree(split_dir)
    os.mkdir(split_dir)


def create_filter_file():
    """
    The filter file is needed to split the data form DWD into its variables.
    If the file already exists it will be deleted and newly written.
    """
    if os.path.isfile(filter_file):
        os.remove(filter_file)
    f = open(filter_file, "w")
    f.write('write "{0}/[shortName].grib[editionNumber]";'.format(split_dir))
    f.close()


def split_to_variable(in_file):
    """
    Splits the in_file into its variables using the filter_file
    The outgoing files are named <VAR>.<EXTENSION> and are stored in the split_dir
    @param in_file: file-name of the file that should be splitted into its variables
    """
    timer = Timer()
    ret_code = subprocess.call(["grib_filter", filter_file, in_file])
    if ret_code != 0:
        cleanup()
        exit_fail("Failed splitting " + in_file)
    else:
        print("{0:20} ...ok, {1} s.".format("Splitting", timer.elapsed()))


def define_step_file(in_file):
    """
    use the name of the original data file (before it was spitted) (input_file) to create the step_file name
    this name is just used for this step and contains useful information for the next preprocessing step
    @param in_file: file name of the original data file (before it was splitted into the variables)
    @return: name for the step_file
    """
    out_name = os.path.basename(in_file)
    extension = os.path.splitext(out_name)   # extract the file-name ending ('.grib1', '.grib2')
    if extension[1] != ".m*":                # '.m*' corresponds to the file name and is no file-ending
        # if this is extracted as extension, the datafile had no file-ending. If not, than an extension exists
        # and must be remove before continuing
        out_name = extension[0]  # Datafile had extension like '.grib2' (this gets removed with this)
    out_name = split_dir + "/preproc-" + out_name + ".nc"
    return out_name


def define_out_file_path(v_name):
    """
    defines the path where the file should be stored at the end. (after all preprocessing is done)
    Default: 'preproc/<VAR>'
    @param v_name: the variable name of the actual file
    @return: specified path
    """
    out_name = destination + "/" + v_name  # specified path
    os.makedirs(out_name, exist_ok=True)     # create this directory if it does not exist now
    return out_name


def change_grid(a_file, s_file):
    """
    change coordinate grid from rotated pole grid to an equidistant coordinate grid using the rot_grid file
    (gridneu.dat) and write result to specified location. In this step the file will be also converted to netCdf and
    compressed if COMPRESS_LEVEL is defined.
    @param a_file: actual file name. This is the file whose coordinate grid will be changed
    @param s_file: out file name (step_file)
    """
    timer = Timer()
    args = "cdo -O -s -f nc4{0}setgrid,'{1}' '{2}' '{3}' > /dev/null 2>&1"\
           .format(COMPRESS_LEVEL, rot_grid, a_file, s_file)
    term_shell(args, "Failed converting '{0}' to '{1}'".format(a_file, s_file), True)
    print("{0:20} ...ok, {1} s.".format("Grib2 -> NetCDF", timer.elapsed()))


def split_time_steps(s_file):
    """
    automatically splits the data in the 8 time steps defined in the file (step_file). The outgoing files where stored
    in split_dir named output00000X
    Each of this eight outgoing files contain only one time step
    @param s_file: file name of the file that should be splitted in the different time_steps.
    """
    timer = Timer()
    args = "cdo{0}-s splitsel,1 {1} {2}/output > /dev/null 2>&1".format(COMPRESS_LEVEL, s_file, split_dir)
    term_shell(args, "Failed splitting '{0}'".format(s_file), False)
    print("{0:20} ...ok, {1} s.".format("Split time steps", timer.elapsed()))


def rename_splitted_data(o_file_path, s_file):
    """
    The previous step created 8 files named 'output00000X'. This function renames this files to meaningful names.
    The new file name contains the model run start time and the 'forecast hour' and 'ensemble' of the old data file
    (the parent file s_file)
    Also the content of the new file needs to be changed. The time variable gets the forecast hour as time value, the
    units of this variable will be changed to 'hours since model run start'.
    @param o_file_path: the path where the data should be stored at
    @param s_file: the file name before it got split into the time steps
    """
    timer = Timer()
    for datafile in glob.glob(split_dir + "/out*"):
        # check the value of the 'time'-variable in the given file. This values is stored in "time_step"
        time_step = datetime.strptime(subprocess.check_output(["cdo", "-s", "-showtimestamp", datafile])
                                      .decode(sys.stdout.encoding).strip(), "%Y-%m-%dT%H:%M:%S")
        store = os.path.basename(s_file)  # use name of datafile that got splitted ("parent file")
        store = store.split("-")[1]  # cdeYYYYMMDD.FF.mEE.nc
        store = store.split(".")  # [cdeYYYYMMDD][FF][mEE][nc]
        forecast_hour = store[-3]  # FF
        ensemble = store[-2]  # mEE
        model_start = time_step - timedelta(hours=int(forecast_hour))  # time the model run starts (stored in ansi)
        hours_since = model_start.strftime("%Y-%m-%d %H:%M:%S")  # just convert it to a string
        model_start = model_start.strftime("%Y%m%d-%H")  # the same, just an other format

        args = "ncap2 -s 'time += {0} - time' -s 'time@units=\"hours since {1} \"' {2} {3}/time:{4}.{5}.{6}.nc" \
            .format(forecast_hour, hours_since, datafile, o_file_path, model_start, forecast_hour, ensemble)
        term_shell(args, "Time change failed", False)
        os.remove(datafile)  # the old file with the "wrong" content named "output00000X" can be removed
    print("{0:20} ...ok, {1} s.".format("Time change + mv", timer.elapsed()))


def create_err_file_name():
    """
    This function creates the name of the logging file. This consists of the path to the logging directory. The actual
    time and the extension '.err".
    @return: the file name
    """
    file_name = log_dir + "/" + datetime.strftime(datetime.today(), "%Y-%m-%d-%H-%M-%s") + ".err"
    return file_name



def float_to_str(f):
    return '%.2f' % f


class Timer:
    def __init__(self):
        self.start_time = tm.time()

    def elapsed(self):
        return float_to_str(tm.time() - self.start_time)


# ===== 0. Step === Checking ===========================================================================================
totalTimer = Timer()

# to start the execution the script needs a source path working with. This needs to contain the data. If no path is
# given the program returns a message.
if len(sys.argv) > 1:
    if os.path.isdir(sys.argv[1]):
        source_path = os.path.abspath(sys.argv[1])  # use absolute path just to avoid unexpected behaviour
    else:
        exit_fail("Path does not exist: " + sys.argv[1])
else:
    exit_fail("No path is given")

# TODO
# for parallel execution this needs to be in the source directories, otherwise things will be overwritten
# split_dir = source_path + "/split"
# filter_file = source_path + "/split_filter.txt"

# check rot_grid data (needed to change the coordinate system)
if not os.path.isfile(rot_grid):
    exit_fail(rot_grid + " is not given or not readable")

# check ingest_dir (needed to figure out which variables we want to import, since this files are given for the import
# later in 03-import.py
if os.path.isdir(ingest_dir):
    ingest_dir = os.path.abspath(ingest_dir)  # use absolute path just to avoid unexpected behaviour
else:
    exit_fail("Ingestion directory does not exist: " + ingest_dir)

os.makedirs(log_dir, exist_ok=True)
err_file_name = create_err_file_name()
print(err_file_name)
# change to working directory (also this is no good behaviour, it makes the execution more easy)
os.makedirs(destination, exist_ok=True)
os.chdir(destination)

# ===== 1. Step === preparation ========================================================================================
ingestions = extract_ingestions()
create_split_dir()
create_filter_file()

# define all files that should be preprocessed (laying in the given source path)
input_files = glob.glob("{0}/cde*".format(source_path))
if not input_files:
    exit_fail("No files of the form 'cde*' found in {0}".format(source_path))

# every file in the directory will be processed
input_files.sort()
for input_file in input_files:
    # only files with forecast_hour between 0 and 24 where processed. (We do not need the rest)
    dat_timer = Timer()
    print(input_file)
    if get_forecast_hour(input_file) > MAX_HOUR:
        print("skipped {0}".format(input_file))
        continue

# ===== 2. Step === split into the variables using filter file =========================================================
    split_to_variable(input_file)

# ===== 3. Step === Grib -> NetCDF (and change coordinate grid) ========================================================
    for var_file in os.listdir(split_dir):
        var_name = var_file.split(".")[0]       # defines the variable name of the given file
        if var_name not in ingestions:
            # remove unneeded variable-datafiles (no ingest file was given)
            os.remove(split_dir + "/" + var_file)
            continue

        step_file = define_step_file(input_file)                    # name after changing the grid
        out_file_path = define_out_file_path(var_name)              # specify location the file will be stored finally
        actual_file = glob.glob(split_dir + "/" + var_file)[0]      # specify actual datafile

        change_grid(actual_file, step_file)                         # changes the grid of the actual file

# ==== 5. Step === Split time steps ====================================================================================
        split_time_steps(step_file)

# ==== 6. Step === Rename data =========================================================================================
        rename_splitted_data(out_file_path, step_file)

# ==== 7. Step === Delete (old) netCdf ("parent file") =================================================================
        os.remove(step_file)
        print("{0:20} ...ok".format("Remove"))
    print("{0:35} {1} s.".format("", dat_timer.elapsed()))

cleanup()
print("\nExecution completed, {0} s.".format(totalTimer.elapsed()))
