#!/usr/bin/env python3

# ===== description ====================================================================================================
# This script converts the data given from DWD to netCdf and splits it per variable
# This is necessary since we import every variable separately
# Also the grid of the datafiles is wrong so we change from 'rotated pole grid' to an equidistant grid
# Execution: ./01-prepro.py <source path>, where <source path> containing the 'cdeYYYYMMDD.HH.mEE.grib-files'
# ======================================================================================================================

# ===== imports    =====================================================================================================
import sys
import os
import subprocess
import shutil
import glob
import time

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
COMPRESS_LEVEL = " -z zip_2 "                   # compress the data for faster import and lower memory usage
# COMPRESS_LEVEL = " "                          # if no compression is wished


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
    @param msg: The error message will be printed before stopping the execution
    """
    print(msg)
    sys.exit(1)


def get_hour(filename):
    """
    This function returns the hour of the given file using its name. The hour is stored in the third last component:
    cdeYYYYMMDD.HH.mEE.grib2
    @param filename: specifies the filename where the hour should be extracted from
    @return: the hour extracted of the filename (forecast hour)
    """
    input_file_name = os.path.basename(filename)
    print(input_file_name)
    try:
        return int(os.path.basename(filename).split(".")[-3])
    except ValueError:
        # sample data on b2drop has the hour in the second component
        return int(os.path.basename(filename).split(".")[1])


class Timer:
    def __init__(self):
        self.start_time = time.time()

    def elapsed(self):
        return '%.2f' % (time.time() - self.start_time)


# ===== 0. Step === Checking ===========================================================================================
totalTimer = Timer()

# to start the execution the script needs a source path working with. This needs to contain the data. If no path is
# given the program returns a message.
if len(sys.argv) > 1:
    if os.path.isdir(sys.argv[1]):
        source_path = os.path.abspath(sys.argv[1])  # use absolute path just to avoid unexpected behaviour
        print(source_path + "\n")
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

# change to working directory (also this is no good behaviour, it makes the execution more easy)
os.makedirs(destination, exist_ok=True)
os.chdir(destination)

# ===== 1. Step === preparation ========================================================================================
# extract all ingestion file names, since this are the variables we have an files that explain how to import
# them later
ingestions = []
for var in glob.glob(ingest_dir + "/ingest-*.json.template"):
    parts = os.path.basename(var)
    parts = parts.split('.')
    parts = parts[0].split('-')
    ingestions.append(parts[1])
if not ingestions:
    exit_fail("No ingestion scripts given.")
else:
    print("Split data into: " + ' '.join(ingestions))

# if the split directory exists, it will be deleted and newly created
if os.path.isdir(split_dir):
    shutil.rmtree(split_dir)
os.mkdir(split_dir)

# write filter_file for split-step
if os.path.isfile(filter_file):
    os.remove(filter_file)
f = open(filter_file, "w")
f.write('write "{0}/[shortName].grib[editionNumber]";'.format(split_dir))
f.close()

input_files = glob.glob("{0}/cde*".format(source_path))
if not input_files:
    exit_fail("No files of the form 'cde*' found in {0}".format(source_path))

# every file in the directory will be processed
input_files.sort()
for input_file in input_files:
    if get_hour(input_file) > MAX_HOUR:
        print("skipped {0}".format(input_file))
        continue

# ===== 2. Step === split into the variables using filter file =========================================================
    timer = Timer()
    return_code = subprocess.call(["grib_filter", filter_file, input_file])
    if return_code != 0:
        cleanup()
        exit_fail("Failed splitting " + input_file)
    else:
        print("Splitting            ...ok, {0} s.".format(timer.elapsed()))

# ===== 3. Step === remove unneeded variable-datafiles (no ingest file was given) ======================================

    for var_file in os.listdir(split_dir):
        var_name = var_file.split(".")[0]
        if var_name not in ingestions:
            os.remove(split_dir + "/" + var_file)

# ===== 4. Step === Grib -> NetCDF (and change coordinate grid) ========================================================
    for file_path in os.listdir(split_dir):  # Each file that later should be imported (ingest file given)
        # use the file name before it was splitted, without the data-extension (=> remove .grib1 or .grib2 extension)
        out_file = os.path.basename(input_file)
        extension = os.path.splitext(out_file)
        if extension[1] != ".m*":
            out_file = extension[0]  # Datafile had extension like '.grib2'
        out_file_path = destination + "/" + os.path.splitext(file_path)[0]  # specify new location (including var.name)
        out_file = out_file_path + "/preproc-" + out_file + ".nc"  # creating new file name (using old name)
        os.makedirs(out_file_path, exist_ok=True)
        in_file = glob.glob(split_dir + "/" + file_path)[0]  # specify actual datafile
        timer = Timer()

        # change coordinate grid and write result to specified location
        args = "cdo -O -s -f nc4{0}setgrid,'{1}' '{2}' '{3}' > /dev/null 2>&1"\
            .format(COMPRESS_LEVEL, rot_grid, in_file, out_file)
        return_code = subprocess.call(args, shell=True)
        if return_code != 0:
            cleanup()
            exit_fail("Failed converting '{0}' to '{1}'".format(file_path, out_file))
        print("Grib2 -> NetCDF      ...ok, {0} s.".format(timer.elapsed()))
cleanup()
print("\nExecution completed, {0} s.".format(totalTimer.elapsed()))
