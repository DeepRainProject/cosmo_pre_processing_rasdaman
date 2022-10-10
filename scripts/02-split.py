#!/usr/bin/env python3

# ==== description =====================================================================================================
# This script splits the preprocessed (01-preproc.py) data into the 8 available time steps since they will get the
# later ansi-axis and importing data in between existing data in rasdaman is not possible.
# Execution: ./02-prepro.py preproc/<VAR>/, where <VAR> containing is the actual variable that should be imported
# ======================================================================================================================

# ==== imports =========================================================================================================

import sys
import os
import subprocess
import glob
import time as tm
from datetime import datetime, timedelta

# ==== constants  ======================================================================================================
COMPRESS_LEVEL = " -z zip_2 "               # compress the data for faster import and lower memory usage
# COMPRESS_LEVEL = " "                      # if no compression is wished
source_path = ""                            # will be set through input parameter


# ==== functions  ======================================================================================================
def float_to_str(f):
    return '%.2f' % f


def exit_fail(msg: str):
    """
    Using this function to print an error message before the execution of this script is stopped.
    It's just used for a better overview of the code.
    @param msg: The error message will be printed before stopping the execution
    """
    print(msg)
    sys.exit(1)


class Timer:
    def __init__(self):
        self.start_time = tm.time()

    def elapsed(self):
        return float_to_str(tm.time() - self.start_time)


# ==== 0. Step === Checking         ====================================================================================
totalTimer = Timer()

# is the source as a parameter given this will be set otherwise the script will be stopped
if len(sys.argv) > 1:
    if os.path.isdir(sys.argv[1]):
        source_path = os.path.abspath(sys.argv[1])      # use absolute path just to avoid unexpected behaviour
    else:
        exit_fail("Path did not exist")
else:
    exit_fail("No path is given")

# change to working directory (also this is no good behaviour, it makes the execution more easy)
os.chdir(source_path)

# check if data is splitted in variables
if not glob.glob(source_path+"/preproc-cde*"):
    exit_fail("No files found; data needs to be preprocessed by 01-preproc.py")

index = 0
files_to_process = glob.glob(source_path+"/preproc-cde*")
files_to_process.sort()

# This is just an output for the user, such that he/she knows how far the preprocessing got
total = len(files_to_process)
for data in files_to_process:
    index += 1
    print("processing " + data)
    print(str(index) + " / " + str(total) + " ---> " + float_to_str(index*100/float(total)) + "%")

# ==== 1. Step === Split time steps ====================================================================================
    timer = Timer()
    # automatically splits the data in the 8 time steps
    args = "cdo{0}-s splitsel,1 {1} output > /dev/null 2>&1".format(COMPRESS_LEVEL, data)
    return_code = subprocess.call(args, shell=True)
    if return_code != 0:
        exit_fail("Failed splitting '{0}'".format(data))
    print("Split time steps ...ok, {0} s.".format(timer.elapsed()))

    hour = 0

# ==== 2. Step === Rename data =========================================================================================
    timer = Timer()
    # the resulting data from the previous step is called "output00000X". This must be changed to some meaningful
    # data name
    for var in glob.glob("out*"):
        # check the value of the 'time'-variable in the given file. This values is stored in "time"
        time = datetime.strptime(subprocess.check_output(["cdo", "-s", "-showtimestamp", var])
                                 .decode(sys.stdout.encoding).strip(), "%Y-%m-%dT%H:%M:%S")
        store = os.path.basename(data)                    # use name of the datafile that got splitted ("parent file")
        store = store.split("-")[1]                       # cdeYYYYMMDD.FF.mEE.nc
        store = store.split(".")                          # [cdeYYYYMMDD][FF][mEE][nc]
        forecast_hour = store[-3]                         # FF
        ensemble = store[-2]                              # mEE
        time = time-timedelta(hours=int(forecast_hour))   # this is the time the model run starts (stored in ansi)
        hours_since = time.strftime("%Y-%m-%d %H:%M:%S")  # just convert it to a string
        var_time = time.strftime("%Y%m%d-%H")             # the same, just an other format
        # In the following the value of the 'time'-variable in the datafile is set to the value of 'forecast_hour'
        # Also the reference is changed, to the start of the model run (later: ansi axis), using variable 'hours_since'
        # The new name of the data file contains the model run start time (other format of the string) and the
        # 'forecast_hour' and 'ensemble' of the "paren file"
        args = "ncap2 -s 'time += {0} - time' -s 'time@units=\"hours since {1} \"' {2} time:{3}.{4}.{5}.nc"\
               .format(forecast_hour, hours_since, var, var_time, forecast_hour, ensemble)
        return_code = subprocess.call(args, shell=True)
        if return_code != 0:
            exit_fail("Time change failed")
        os.remove(var)  # the old file with the "wrong" content named "output00000X" can be removed
    print("Time change + mv ...ok, {0} s.".format(timer.elapsed()))

# ==== 3. Step === Delete (old) netCdf ("parent file") =================================================================
    os.remove(data)
    print("Remove           ...ok\n")
print("Execution completed, {0} s.".format(totalTimer.elapsed()))
