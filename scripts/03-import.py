#!/usr/bin/env python3

# ==== description =====================================================================================================
# This script merges all available datafiles of one model_start to make the import faster. That means that all 25
# forecast hours (0-24) are merged together. For each model start we than have to import 20 files (the different
# ensemble members). For each day we than import the eight model start times. (This is done for each variable
# individually)
# Execution: ./03-import.py preproc/<VAR>/, where <VAR> containing is the actual variable that should be imported
# ======================================================================================================================

# ==== imports    ======================================================================================================

import glob
import os
import shutil
import subprocess
import sys
import time as tm
from datetime import datetime, timedelta

# ==== constants  ======================================================================================================

COMPRESS_LEVEL = " -z zip_2 "                           # compress the data for faster import and lower memory usage
# COMPRESS_LEVEL = " "                                  # if no compression is wished

script_dir = os.getcwd()                                # actual directory
ingest_dir = script_dir+"/ingest"                       # directory including ingest-files
import_script = "/opt/rasdaman/bin/wcst_import.sh"      # import script (rasdaman)
rasdaman_endpoint = "http://134.94.199.129:8080/rasdaman/ows"
source_path = ""                                        # will be set through input parameter

if not os.path.exists(import_script):
    import_script = "/home/dimitar/rasdaman/enterprise/src-install/bin/wcst_import.sh"
    rasdaman_endpoint = "http://localhost:8080/rasdaman/ows"

# ==== functions =======================================================================================================
def float_to_str(f):
    return '%.2f' % f


def convert_time(path: str) -> datetime:
    """
    Extracts time information from given path.
    This is needed to get the start and end time from the source directory
    @return: time information
    @rtype: datetime
    @param path: the first or last element in the directory that is needed to get the time information
    """
    # b'/mnt/rasdaman/DeepRain/playground/preproc/t/time:YYYYMMDD-HH.FF.mEE.nc
    time = path.split(":")[-1]      # YYYYMMDD-HH.FF.mEE.nc
    time = time.split(".")[0]		# YYYYMMDD-HH
    return datetime.strptime(time, "%Y%m%d-%H")


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


def term_shell(shell_args: str, err_message: str):
    """
    This function gets arguments "shell_args" what should be executed as a shell command.
    If this does not work "err_message" is printed and the program will exit.
    @param shell_args: arguments that should be executed in shell
    @param err_message: message that should be printed if the execution fails
    """
    return_code = subprocess.call(shell_args, shell=True)
    if return_code != 0:
        exit_fail("Command failed: " + shell_args + "\nReason: " + err_message)


def move_files(a_time: datetime, e_mem: str) -> list:
    """
    Moves all files with the specified time (a_time) of the specified member to tempdir and renames it in the same
    step to <forecast_hour>.nc. All forecast_hours that exist in our data set will then stored in 'existing'
    @param a_time: specifies the time when the model run started
    @param e_mem: specifies the member
    @return: array that contains all forecast hours that exist for this model run
    @rtype: array
    """
    os.makedirs(tempdir, exist_ok=True)
    # move input files to tempdir
    exis = []
    files_to_process = glob.glob(source_path+"/time:"+a_time.strftime("%Y%m%d-%H")+".*.m"+e_mem+".nc")
    files_to_process.sort()
    for act_file in files_to_process:
        outfile = tempdir + "/" + act_file.split(".")[-3] + ".nc"
        shutil.move(act_file, outfile)
        exis.append(outfile.split("/")[-1])
    print(exis)
    return exis


def build_data(a_time: datetime, e_mem: str, exis: list):
    """
    Checks for each forecast hour if this exists in the data.
    If so this is added to the list. If not missing data will be inserted.
    After all hours are checked the defined datafiles will be merged together
    @param a_time: datetime object specifying the model run we are looking at
    @param e_mem:specifying the member we are looking at
    @param exis: describes all forecast hours that exist in our data set
    """
    in_files = " "
    hours = [str(h).zfill(2)+".nc" for h in range(0, 25)]  # ["01.nc", "02.nc", ..]
    for hour in hours:
        if hour not in exis:    # create missing datafile to use as placeholder
            timer = Timer()
            shell_args = "ncap2 -s 'time@units=\"hours since {0}\"' {1} {2}/{3}"\
                         .format(a_time.strftime("%Y-%m-%d %H:00:00"), missing, tempdir, hour)
            term_shell(shell_args, "changing time@units in missing file failed.")
            print("Added "+hour+" as missing value")
            print("changed time@units in missing file: " + missing + " " + timer.elapsed() + " s.")
        in_files = in_files + tempdir + "/" + hour + " "
    timer = Timer()
    # create the name of the datafile that holds all information from before
    out_file = "{0}/processed:{1}.m{2}.nc".format(source_path, a_time.strftime("%Y%m%d%H"), e_mem)
    shell_args = "cdo{0}-s -mergetime {1} {2}".format(COMPRESS_LEVEL, in_files, out_file)  # merge the given files
    term_shell(shell_args, "Failed merging time steps")
    print("Merging       ...ok, {0} s({1})".format(timer.elapsed(), out_file))


def remove_data(a_time: datetime, e_mem: str):
    """
    All datafiles of the given time (a_time) with the given member will be removed
    Afterwards the temporary directory will be removed
    This is done after the datafiles where merged together
    @param a_time: specifies the time attribute of the files that should be deleted
    @param e_mem: specifies the member, which files should be removed
    """
    for data_file in glob.glob("{0}/time:{1}.*.m{2}.nc".format(source_path, a_time.strftime("%Y%m%d-%H"), e_mem)):
        os.remove(data_file)
    shutil.rmtree(tempdir)


def create_ingest(import_files: str) -> str:
    """
    First the ingest-template file will be copied.
    The copy will be out ingest-file. Therefore the placeholder "FILE_PATH" must be replaced.
    We use the parameter "files" to replace the placeholder.
    @param import_files: specifies the files that should be imported. This is used to replace the placeholder "
                         FILE_PATH" in the ingest-template file
    @return: returns the path to the created ingest file, which must be used to import the specified data
    """
    shutil.copyfile(ingest_dir+"/"+ingest_template_file, source_path+"/"+ingest_file)
    path_ingest_file = glob.glob(source_path + "/" + ingest_file)[0]
    shell_args = "sed -i -e 's@FILE_PATH@" + import_files + "@g' '" + path_ingest_file + "'"
    term_shell(shell_args, "Replacement failed")
    return path_ingest_file


def check_time(processed_files: str) -> datetime:
    """
    If datafiles that where merged together where already in the directory before this script started, this script
    must have stop the execution because of an error before. In this case we check whether we can fix the problem
    automatically. This function checks whether all files that are already merged together belong to one model run start
    value. If this is the case, this mistake can be automatically fixed. Otherwise it needs to be done manually.
    @param processed_files: specifies the files that where already processed with this script in a run before
    @return: returns a datetime as a symbol that it needs to be fixed manually
    """
    a_time = datetime.strptime(processed_files[0].split(":")[-1].split(".")[0], "%Y%m%d%H")
    for data_file in processed_files:
        if a_time != datetime.strptime(data_file.split(":")[-1].split(".")[0], "%Y%m%d%H"):
            return None
    return a_time


def get_member(data_file: str) -> str:
    """
    Returns the ensemble member of the given datafile. (Can be extracted from the file name)
    @param data_file: file where the ensemble member should be extracted from
    @return: the ensemble member
    """
    e_mem = data_file.split("m")[-1].split(".")[0]
    return e_mem


def check_files(a_time: datetime, processed_files: str) -> list:
    """
    Checks which members where already merged and removes the files that belong to this members that are not merged now
    (files where merged, but old files not deleted)
    The founded members where returned
    @type processed_files: str
    @type a_time: datetime
    @param a_time: specifies the model run of the files
    @param processed_files: specifies the already processed files
    @return: returns an array of all members that where processed
    """
    e_mems = []
    for data_file in processed_files:
        e_mems.append(get_member(data_file))
    for data_file in glob.glob(source_path+"/time:"+a_time.strftime("%Y%m%d-%H")+"*.nc"):
        if get_member(data_file) in e_mems:
            os.remove(datafile)
    return e_mems


def get_position(a_time, times):
    """
    This function is used as a sub function of 'checkImport()'. It returns the position of the actual checked time step.
    @type times: str
    @type a_time: datetime
    @rtype: int
    @param a_time: time of the actual checked file
    @param times: all times that are already imported
    @return: position of the time, if it is not included it returns -1
    """
    i = 0
    for index in times:
        if a_time.strftime("%Y-%m-%dT%H:00:00.000Z") == index:
            return i
        else:
            i = i+1
    return -1


def check_import(a_time, ingest_file_name):
    """
    This function checks whether the given time step (a_time) is already imported and if so it checks
    whether it is possible to automatically continue or if the user has to check things manually.
    @param a_time: datetime of actual checked files
    @param ingest_file_name: contains the coverageId what is needed for getting the information
    @return:
    """
    # extract coverageId out of the ingest file
    shell_args = "sed -n '/coverage_id/p' " + ingest_file_name
    coverage_id = str(subprocess.Popen(shell_args, shell=True, stdout=subprocess.PIPE, stderr=None)
                      .communicate()[0]).split('"')[3]
    # use the extracted coverageId to get the CoverageDescription
    shell_args = 'wget -q "{0}?&SERVICE=WCS&VERSION=2.0.1&' \
                 'REQUEST=DescribeCoverage&COVERAGEID={1}" -O out.file'.format(rasdaman_endpoint, coverage_id)
    term_shell(shell_args, "Could not Get the Coverage Description")
    # use the CoverageDescription to get all imported time steps (array times)
    shell_args = "sed -n '/gmlrgrid:coefficients/p' out.file"
    times = str(subprocess.Popen(shell_args, shell=True, stdout=subprocess.PIPE, stderr=None).communicate()[0])
    times.split('<gmlrgrid:coefficients>"')[1].split('"</gmlrgrid:coefficients>')[0].split('" "')
    # check if the given time step (of the datafiles) is already imported but not as last one
    pos = get_position(a_time, times)
    if pos == -1:
        # Time step is not included
        return 
    elif pos != (len(times)-1):
        exit_fail("Manual check needed. The given datafiles are (partially) included but at position {0} there are "
                  "already {1} other time steps imported".format(str(pos), str(len(times)-pos-1)))
        return


def check_directory():
    """
    This function checks if this script was executed before but stopped its execution.
    This can be seen, if one or more "processed:..." datafiles exist in the directory.
    It is possible that this datafiles where already imported but not removed (not really likely)
    The more realistic option is that the script broke while merging the time steps (less than 20 files in directory)
    or that it broke while importing the datafiles (exactly 20 files in directory).
    If it are more than 20 files something went wrong, since every time 20 of this files exist (not more)
    """
    processed_files = glob.glob(source_path+"/processed:*")
    if len(processed_files) == 0:
        return
    elif len(processed_files) > 20:
        exit_fail("First clean up directory! To many files for automatic import.")
    # At this point we have either 20 or less files that where already processed.
    # But we need to find out if they belong together
    a_time = check_time(processed_files)
    e_mems = check_files(a_time, processed_files)
       
    if len(processed_files) < 20:
        pos_members = [str(m).zfill(2) for m in range(1, 21)]  # ["01", "02", ..]
        for mem in pos_members:
            if mem not in e_mems:
                exis = move_files(a_time, mem)
                build_data(a_time, mem, exis)
                remove_data(a_time, mem)

    # import the files if possible
    check_import(a_time, ingest_dir+"/"+ingest_template_file)
    processed_files = glob.glob(source_path+"/processed:*")
    if len(processed_files) > 0:
        file_name = source_path + "/" + "processed:*"
        path_ingest_file = create_ingest(file_name)
        shell_args = import_script + " " + path_ingest_file
        term_shell(shell_args, "Import failed")
        os.remove(path_ingest_file)
        for data_file in glob.glob(file_name):
            os.remove(data_file)


# ==== Checking input ==================================================================================================
if len(sys.argv) > 1:
    if os.path.isdir(sys.argv[1]):
        source_path = os.path.abspath(sys.argv[1])          # use absolute path just to avoid unexpected behaviour
    else:
        exit_fail("Path did not exist: "+sys.argv[1])
else:
    exit_fail("Please specify a path.")

if not os.path.isdir(ingest_dir):
    exit_fail("Invalid ingest_idr: "+ingest_dir)
if not os.path.isfile(import_script):
    exit_fail("Path to import_script does not exist: "+import_script)
if not os.access(import_script, os.X_OK):
    exit_fail("Import_script is not executable: "+import_script)

missing = source_path+"/missing.nc"                            # path for missing time steps
if not os.path.isfile(missing):
    exit_fail("No file for missing values given!")

# ==== Create "constants" out of checked input =========================================================================

# working directory - needed to create one datafile out of all time steps given and inserting missing values if needed
tempdir = source_path+"/tempdir"
variable = str(source_path.split("/")[-1])                      # get variable (source path should be /preproc/<VAR>)
ingest_template_file = "ingest-" + variable + ".json.template"  # save the needed ingest-file (template)
ingest_file = "ingest-" + variable + ".json"                    # save name of used ingest-file

# ==== Checking again ==================================================================================================
check_directory()  # check if old datafiles that where preprocessed but not imported still remain in the directory

# ==== 1. Step === extract start and end_time ==========================================================================
args = 'ls {0}/time:* | head -n 1'.format(source_path)
start_time = convert_time(str(subprocess.Popen(args, shell=True, stdout=subprocess.PIPE, stderr=None).communicate()[0]))
args = 'ls -r {0}/time:* | head -n 1'.format(source_path)
end_time = convert_time(str(subprocess.Popen(args, shell=True, stdout=subprocess.PIPE, stderr=None).communicate()[0]))

print("Import files from {0} to {1}".format(start_time.strftime("%Y-%m-%d:%H"), end_time.strftime("%Y-%m-%d:%H")))

actual_time = start_time

# ==== 2. Step === Build Data to import ================================================================================

while actual_time <= end_time:
    members = [str(m).zfill(2) for m in range(1, 21)]  # ["01", "02", ..]
    for member in members:
        existing = move_files(actual_time, member)     # move all founded files to tempdir
        build_data(actual_time, member, existing)      # build one datafile for actual_time for that member
        remove_data(actual_time, member)               # remove all datafiles that where used to build the file above
    # all created files (20, one per member) are added to import list
    files = "{0}/processed:{1}.*.nc".format(source_path, datetime.strftime(actual_time, "%Y%m%d%H"))
    
# ==== 3. Step === Create ingest_file ==================================================================================
    ingest_file_path = create_ingest(files)
# ==== 4. Step === Import data =========================================================================================
    args = import_script+" "+ingest_file_path
    term_shell(args, "Import failed")
# ==== 5. Step === Remove ingest_file and datafiles ====================================================================
    os.remove(ingest_file_path)
    for datafile in glob.glob(files):
        os.remove(datafile)

    actual_time = actual_time+timedelta(0, 10800)  # add three hours (go to next model run)
print("{0} until {1} imported".format(start_time.strftime("%Y-%m-%d:%H"), end_time.strftime("%Y-%m-%d:%H")))
