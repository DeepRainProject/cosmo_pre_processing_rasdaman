import os
import shutil
import glob
from datetime import datetime

from prepros import term_shell
from prepros import remap_data, modify_native_data


def convert_time(path):
    """
    Extracts time information from given path.

    This is needed to get the start and end time from the source directory.

    Args:
        path (str): the first or last element in the directory that is needed to get the time information

    Returns:
        datetime.datetime: time information
    """
    # b'/p/largedata/deeprain/<year>/<month>/<day>/
    time = path.split("/") # [p][largedata][deeprain][year][month][day]
    time = "{y}{m}{d}".format(y=time[-3], m=time[-2], d=time[-1])
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
    if int(h) <= 21:
        return False
    if t > border_time:
        return False
    return True


def move_files(model_run, member, tempdir, source_path):
    """
    Moves specified files to a specified tempdir.

    Moves all files that correspond to the given model_run and specified member to a defined tempdir and renames it in the same
    step to <forecast_hour>.nc. All forecast_hours that exist in our data set will then be stored in 'existing'.

    Args:
        model_run (datetime): specifies the time when the model run started
        member (str): specifies the member

    Returns:
        list: all forecast hours that exist for this model run
    """
    os.makedirs(tempdir, exist_ok=True)
    print("DEBUG: Temporary directory created: {path}".format(path = tempdir))
    # move input files to tempdir
    exis = []
    files_to_process = glob.glob(source_path + "/time:" + model_run.strftime("%Y%m%d-%H") + ".*.m" + member + ".nc")
    files_to_process.sort()
    print("DEBUG: Number of files to process: {num}".format(num = len(files_to_process)))
    for act_file in files_to_process:
        outfile = tempdir + "/" + act_file.split(".")[-3] + ".nc"
        shutil.copy(act_file, outfile)
        print("DEBUG: Copied {startfile} to {endfile}".format(startfile = act_file, endfile = outfile))
        exis.append(outfile.split("/")[-1])
    print("DEBUG: Returned files: exis={exis}".format(exis=exis))
    return exis


def deaccumulate_data(hours, max_hour, tempdir):
    in_files = " "
    index = 1
    for hour in hours:
        if index > max_hour:
            out_file = tempdir + "/" + hour
            print("Last hour, no subtraction")
        else:
            high_file = tempdir + "/" + hour
            low_file = tempdir + "/" + hours[index]
            out_file = tempdir + "/" + hour +".out"
            print("t_i+1: {0}\n t_i: {1}\n out: {2}".format(high_file, low_file, out_file))
            shell_args = "cdo sub {0} {1} {2}".format(high_file, low_file, out_file)
            print("Command: '{0}''".format(shell_args))
            term_shell(shell_args, "Failed converting to hourly data", False)
        in_files = in_files + out_file + " "
        index = index + 1
    print("This are the files that will be merged: {0}".format(in_files))
    return in_files


def search_data(hours, tempdir):
    in_files = " "
    for hour in hours:
        out_file = tempdir + "/" + hour
        in_files = in_files + out_file + " "
    print("This are the files that will be merged: {0}".format(in_files))
    return in_files


def build_missing_data(model_run, existing_hours, max_hour, tempdir, missing_file, DEACUMMULATE):
    in_files = " "
    # check if data is available for time > 21, rewrite hours then...
    hours_24 = [str(h).zfill(2)+".nc" for h in range(21, 25)]
    if existing_hours[-1] in hours_24:
        max_hour = 24
        hours = [str(h).zfill(2)+".nc" for h in range(24, -1,-1)]  # ["24.nc", "23.nc", .., 00.nc]
    if DEACUMMULATE:
        # check until which time data is available, starting from zero.
        # Until that time data can be used and deacummulated, afterwards missing data is needed regardles if data is available
        possible_hours = [str(h).zfill(2) for h in range(0, (max_hour+1))] # 00, 01, 02, ..., max_hour
        break_hour = -1
        for hour in possible_hours:
            if hour+".nc" not in existing_hours:
                # hour is the position where data isn't available anymore
                # therefore hour -1 is the position where data is still available
                # break_hour is hour -1
                break
            break_hour = break_hour + 1
        if break_hour > -1:
            available_hours = [str(h).zfill(2)+".nc" for h in range((break_hour), -1,-1)]
            in_files = deaccumulate_data(available_hours, break_hour, tempdir)
        not_available_hours = [str(h).zfill(2) for h in range(max_hour, (break_hour),-1)]
        for hour in not_available_hours:
            shell_args = "ncap2 -s 'time+={3}-time' -s 'time@units=\"hours since {0}\"' {1} {2}/{3}.nc" \
                     .format(model_run.strftime("%Y-%m-%d %H:00:00"), missing_file, tempdir, hour)
            print('MISSING_file_Debug: Shell Arg is:{term_shell_cmd}'.format(term_shell_cmd=shell_args))   
            term_shell(shell_args, "changing time@units in missing file failed.", False)
            #    print("INFO: Added " + hour + " as missing value")
            #    print("DEBUG: changed time@units in missing file")
            out_file = tempdir + "/" + hour+ ".nc"
            in_files = in_files + out_file + " "
            print('MISSING_file_Debug:In_files is:{in_files}'.format(in_files=in_files))   

    else:
        for hour in hours:
            if hour not in existing_hours:
                # create missing datafile to use as placeholder
                shell_args = "ncap2 -s 'time+={3}-time' -s 'time@units=\"hours since {0}\"' {1} {2}/{3}.nc" \
                        .format(model_run.strftime("%Y-%m-%d %H:00:00"), missing_file, tempdir, hour)
                print('MISSING_file_Debug:{term_shell_cmd}'.format(term_shell_cmd=shell_args))  
                term_shell(shell_args, "changing time@units in missing file failed.", False)
                #    print("INFO: Added " + hour + " as missing value")
                #    print("DEBUG: changed time@units in missing file")
            out_file = tempdir + "/" + hour
            in_files = in_files + out_file + " "
    return in_files


def build_data(model_run, member, existing_hours, tempdir, source_path, COMPRESS_LEVEL, cosmo_grid_des, tar_grid_des,
               missing_file, DEACUMMULATE, RENAME_VAR, old_name, new_name, CHANGE_UNITS, units, CHANGE_LONG_NAME,
               long_name, REMAPPED, remapped_dir, NATIVE, native_dir):
    """
    This function first checks if all needed forecast_hours are available. If this is not the case the data will be deleted and
    a file that contains "missing values" is used as placeholder.
    If all needed files are available, the files are merged to one big file containing all forecast_hours for that model_run.
    Dependig on the variable also a conversion to hourly data is done and file-attributes are adjusted.
    Note that intermediate files are stored temporary in tempdir that is cleaned with the remove_data-function
    (see below).
    @param model_run: datetime object specifying the model_run we are looking at
    @param member: specifying the member we are looking at
    @param existing_hours: describes all forecast hours that exist in our data set
    @param tempdir: this is the temporary directory this script is working in
    @param source_path: the path where the data comes from and the created file need to be stored
    @param COMPRESS_LEVEL: ?
    @param cosmo_grid_des: CDO grid decription for data on COSMO's native grid
    @param tar_grid_des: CDO grid description for the target grid (onto which data is remapped)
    """
    max_hour = 24
    clean = False # TODO: check if it is really needed and if so change it to the correct behaviour (needed for term_shell())
    hours = [str(h).zfill(2)+".nc" for h in range(24, -1,-1)]  # ["24.nc", "23.nc", .., 00.nc]
    if set(hours) != set(existing_hours):
        # There are not 24 forecast_hours available. Either only 21 are available or something is wrong
        hours = [str(h).zfill(2)+".nc" for h in range(21, -1,-1)]  # ["21.nc", "20.nc", .., 00.nc]
        max_hour = 21
    if set(hours) != set(existing_hours):
        # Some data is not available. It will be filled with missing values
        in_files = build_missing_data(model_run, existing_hours, max_hour, tempdir, missing_file, DEACUMMULATE)
    else:
        # All needed data is available and will be processed here:
        if DEACUMMULATE:
            in_files = deaccumulate_data(hours, max_hour, tempdir)
        else:
            in_files = search_data(hours, tempdir)

    # Merge all given files
    step_file = "{0}/step_1.nc".format(tempdir)
    shell_args = "cdo{0}-s -mergetime {1} {2}".format(COMPRESS_LEVEL, in_files, step_file)
    term_shell(shell_args, "Failed merging time steps", clean)
    print("INFO: Merging       ...ok")

    variable = old_name

    if RENAME_VAR:
        # rename the variable name
        step_1_file = "{0}/step_2.nc".format(tempdir)
        shell_args = "cdo chname,{old},{new} {in_file} {out_file}"\
                     .format(old = old_name, new=new_name, in_file = step_file, out_file=step_1_file)
        term_shell(shell_args, "Failed renaming variable", clean)
        step_file = step_1_file
        variable = new_name

    if CHANGE_UNITS and CHANGE_LONG_NAME:
        # change both units and long_name of variable
        step_1_file = "{0}/step_3.nc".format(tempdir) # create the name of the datafile that holds all information from before
        shell_args = "ncap2 -s '{variable}@units=\"{units}\"' -s '{variable}@long_name=\"{long_name}\"' {in_file} {out_file}"\
                     .format(units = units, variable = variable, long_name = long_name, in_file = step_file, out_file = step_1_file)
        term_shell(shell_args, "Failed adapting units and long_name", clean)
        step_file = step_1_file
        print("INFO: Adaption       ...ok")
    elif CHANGE_UNITS:
        # change only units of variable
        step_1_file = "{0}/step_3.nc".format(tempdir) # create the name of the datafile that holds all information from before
        shell_args = "ncap2 -s '{variable}@units=\"{units}\"' {in_file} {out_file}"\
                     .format(units = units, variable = variable, in_file = step_file, out_file = step_1_file)
        term_shell(shell_args, "Failed adapting units", clean)
        step_file = step_1_file
        print("INFO: Adaption       ...ok")
    elif CHANGE_LONG_NAME:
        # change only long_name of variable
        step_1_file = "{0}/step_3.nc".format(tempdir) # create the name of the datafile that holds all information from before
        shell_args = "ncap2 -s '{variable}@long_name=\"{long_name}\"' {in_file} {out_file}"\
                     .format(variable = variable, long_name = long_name, in_file = step_file, out_file = step_1_file)
        term_shell(shell_args, "Failed adapting long_name", clean)
        step_file = step_1_file
        print("INFO: Adaption       ...ok")

    # Path for remapped and native

    if REMAPPED:
        # Finally, remap the data (TODO: make compression level flexible, not relying on default of remap_data)
        # remap_method=conservative should be chosen for precipitation data
        path = "{0}/{1}".format(source_path, remapped_dir)
        # TODO: Should be in main.py!
        if not os.path.isdir(path):
            os.mkdir(path)

        outfile = "{0}/processed:{1}.m{2}.nc".format(path, model_run.strftime("%Y%m%d%H"), member)
        _ = remap_data(step_file, cosmo_grid_des, outfile, tar_grid_des, remap_method="conservative")

    if NATIVE:
        path = "{0}/{1}".format(source_path, native_dir)
        # TODO: Should be in main.py! -> Can be replaced by os.makedirs(path, exist_ok=True)
        if not os.path.isdir(path):
            os.mkdir(path)
        outfile = "{0}/processed:{1}.m{2}.nc".format(path, model_run.strftime("%Y%m%d%H"), member)
        _ = modify_native_data(step_file, outfile)


def build_data_old(a_time, e_mem, exis, missing, tempdir, source_path, COMPRESS_LEVEL):
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
    hours = [str(h).zfill(2) + ".nc" for h in range(0, 22)]  # ["01.nc", "02.nc", ..]
    for hour in hours:
        if hour not in exis:  # create missing datafile to use as placeholder
            #if needs_to_create_missing(hour, a_time):
            #    shell_args = "ncap2 -s 'time@units=\"hours since {0}\"' {1} {2}/{3}" \
            #        .format(a_time.strftime("%Y-%m-%d %H:00:00"), missing, tempdir, hour)
            #    term_shell(shell_args, "changing time@units in missing file failed.", clean=false) #TODO
            #    print("INFO: Added " + hour + " as missing value")
            #    print("DEBUG: changed time@units in missing file")
            #else:
            #    raise SlaveError(function="build_data()",
            #                     message="Forcast hour {} is missing from time {}".format(hour, datetime.strftime("%Y%m%d")))
            print("INFO: Hour " + hour + " missing.")
        in_files = in_files + tempdir + "/" + hour + " "
    # create the name of the datafile that holds all information from before
    print("DEBUG: Files specified: {files}".format(files=in_files))
    out_file = "{0}/processed:{1}.m{2}.nc".format(source_path, a_time.strftime("%Y%m%d%H"), e_mem)
    shell_args = "cdo{0}-O -s -mergetime {1} {2}".format(COMPRESS_LEVEL, in_files, out_file)  # merge the given files
    term_shell(shell_args, "Failed merging time steps", clean=False) #TODO
    print("INFO: Merging       ...ok")

#remove_data(actual_time, member, relative_tempdir, relative_var_dir)  # remove all datafiles that where used to build the file above
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
    print("DEBUG: Files removed.")
    shutil.rmtree(tempdir)
    print("INFO: Removing      ...ok")


def get_member(data_file):
    """
    Returns the ensemble member of the given datafile.

    The ensemble member can be extracted from the file name.

    Args:
        data_file (str): file where the ensemble member should be extracted from

    Returns:
        str: the ensemble member
    """
    e_mem = data_file.split("m")[-1].split(".")[0]
    return e_mem
