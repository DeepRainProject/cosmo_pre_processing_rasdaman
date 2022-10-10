from mpi4py import MPI
import sys
import subprocess
import logging
import time
import os
import shutil
import glob
from datetime import datetime, timedelta
from netCDF4 import Dataset
import numpy as np

from exception import MainError
from exception import SlaveError

# ====================== Shared tools across all scripts ========================= #
# ini. MPI
comm = MPI.COMM_WORLD
my_rank = comm.Get_rank()  # rank of the node
p = comm.Get_size()  # number of assigned nods
my_rank = comm.Get_rank()  # rank of the node

if my_rank == 0:  # node is master

    logger = logging.getLogger(__file__)
    logger.addHandler(logging.StreamHandler(sys.stdout))

# ======================= List of functions ====================================== #


def extract_ingestions(input_dir, ingest_file):
    """
    This function extracts all variable names that can be imported later since we have the ingest files in the
    input_dir. The extracted variables names where written in a file to make it available for all slave nodes. They can
    get the ingestions by "read_ingestions()".
    """
    inges = []
    for var in glob.glob(input_dir + "/ingest-*.json.template"):
        parts = os.path.basename(var)               # ingest-<VAR>.json.template
        parts = parts.split('.')[0]                 # ingest-<VAR>
        parts = parts.split('-')[1]                 # <VAR>
        inges.append(parts)
    if not inges:
        # since no ingest files are given we can not import something later. So we do not need to preproces something.
        raise MainError(function="extract_ingestions()",
                        critical="No ingestion scripts given",
                        info='exit status : 1')
        # logger.critical("No ingestion scripts given.")
        # logger.info('exit status : 1')
        # sys.exit(0)
    else:
        #logger.info('"Split data into: " + ' '.join(inges)')
        #print("Split data into: " + ' '.join(inges))
        i_file = Dataset(ingest_file, "w", format="NETCDF4_CLASSIC")#, parallel=True) # @amirpasha
        _ = i_file.createDimension('nchars', 10)
        _ = i_file.createDimension('nstrings', None)
        i = i_file.createVariable('ingestions', 'S1', ('nstrings', 'nchars'))
        inges = np.array(inges, dtype='S10')
        i._Encoding = 'ascii'
        i[:] = inges
        i_file.close()
        logger.info("extract ingetsion is done ")


def read_ingestions(ingest_file):
    """
    This function reads out all variables that can be imported. This information is stored in ghe ingest_file by the
    master using extract_ingestions().
    :return: all variable names that should be preprocessed, since the ingest-files exist
    """
    inges = []
    i_file = Dataset(ingest_file, "r", format="NETCDF4_CLASSIC")#, parallel=True )# @amirpasha turn on the paralelle i/O
    i = i_file.variables["ingestions"]
    for var in i:
        inges.append(str(var))
    i_file.close()
    return inges


def get_forecast_hour(filename):
    """
    This function returns the forecast hour of the given file using its name.
    The hour is stored in the third last component:
    cdeYYYYMMDD.HH.mEE.grib2
    @param filename: specifies the filename where the hour should be extracted from
    @return: the hour extracted of the filename (forecast hour)
    """
    try:
        return int(os.path.basename(filename).split(".")[-3])
    except ValueError:
        # sample data on b2drop has the hour in the second component
        return int(os.path.basename(filename).split(".")[1])


def split_to_variable(in_file, relative_filter_file):
    """
    Splits the in_file into its variables using the relative_filter_file
    The outgoing files are named <VAR>.<EXTENSION> and are stored in the relative_split_dir
    @param in_file: file-name of the file that should be splitted into its variables
    """
    # TODO : @amirpasha : this function does not work like this on the juwels!
    ret_code = subprocess.call(["grib_filter", relative_filter_file, in_file])
    if ret_code != 0:
        raise SlaveError(function="split_to_variable()",
                         message="Something went wrong while splitting into the variables.")


def define_nc_file(relative_split_dir, in_file):
    """
    use the name of the original data file (before it was spitted) (input_file) to create the nc_file name
    this name is just used for this step and contains useful information for the next preprocessing step
    @param in_file: file name of the original data file (before it was splitted into the variables)
    @return: name for the nc_file
    """
    out_name = os.path.basename(in_file)
    extension = os.path.splitext(out_name)   # extract the file-name ending ('.grib1', '.grib2')
    if extension[1] != ".m*":                # '.m*' corresponds to the file name and is no file-ending
        # if this is extracted as extension, the datafile had no file-ending. If not, than an extension exists
        # and must be remove before continuing
        out_name = extension[0]  # Datafile had extension like '.grib2' (this gets removed with this)
    out_name = relative_split_dir + "/preproc-" + out_name + ".nc"
    return out_name


def define_out_file_path(relative_destination_dir, v_name):
    """
    defines the path where the file should be stored at the end. (after all preprocessing is done)
    @param v_name: the variable name of the actual file
    @return: specified path
    """
    out_name = relative_destination_dir + "/" + v_name  # specified path
    os.makedirs(out_name, exist_ok=True)     # create this directory if it does not exist now
    return out_name


def grib_to_netcdf(infile: str, outfile: str, compress_lvl: int = 6):
    """
    Converts grib-files to netCDF-data. If compress_lvl is given, zip-compression is performed as well.
    :param infile: input grib-file
    :param outfile: target netCDF-file (to be created)
    :param compress_lvl: level for zip-compression (must be within 1 and 9)
    """
    method = grib_to_netcdf.__name__

    infile = infile if infile.endswith((".grib", "grib2")) else infile+".grib"
    # outfile = outfile if outfile.endswith(".nc") else outfile + ".nc"

    # sanity checks
    if not os.path.isfile(infile):
        raise FileNotFoundError("%{0}: Could not find input file '{1}'.".format(method, infile))
    if os.path.isfile(outfile):
        raise FileExistsError("%{0}: Output file to be created ('{1}') already exists. Please clean-up first."
                              .format(method, outfile))

    compress_lvl = int(compress_lvl)
    if not 1 <= compress_lvl <= 9:
        raise ValueError("%{0}: Invalid compression level '{1}' chosen. Value must be within 1 and 9."
                         .format(method, compress_lvl))

    # Create cdo-command for conversion..
    args = "cdo -O --reduce_dim -s -f nc4 -z zip_{0:d} copy {1} {2}".format(compress_lvl, infile, outfile)
    # ... run it
    term_shell(args, "%{0}: Failed conversion grib->netCDF for file '{1}'.".format(method, infile), True)

    return True


def remap_data(infile: str, ingrid: str, outfile: str, outgrid: str, compress_lvl: int = 6,
               remap_method: str = "conservative"):
    """
    Remaps data from infile onto a grid defined by a CDO grid description file targrid.
    The method for remapping can be chosen according to the methods provided by CDO
    (see remapfunc_cdo below for details).
    See https://code.mpimet.mpg.de/projects/cdo/embedded/index.html#x1-220001.5.2 for details on
    CDO's grid description files that are required for the remapping.
    Besides, the deflate compression level can be controlled for the final netCDF-file.
    :param infile: input datafile (can be a grib or netCDF-file)
    :param ingrid: a CDO grid description for the input data
    :param outfile: name of output netCDF-file
    :param outgrid: a CDO grid description for the target (output) data
    :param compress_lvl: deflate compression level (must be between 1 and 9)
    :param remap_method: CDO-method for remapping (e.g. "bilinear", "nearest_neighbor", "conservative" etc.)
    :return status: True in case of success
    """
    method = remap_data.__name__

    # sanity checks
    if not os.path.isfile(infile):
        raise FileNotFoundError("%{0}: Could not find input file '{1}'.".format(method, infile))
    if not os.path.isfile(ingrid):
        raise FileNotFoundError("%{0}: Could not find description file '{1}' for input grid.".format(method, ingrid))
    if not os.path.isfile(outgrid):
        raise FileNotFoundError("%{0}: Could not find description file '{1}' for target grid".format(method, outgrid))

    compress_lvl = int(compress_lvl)
    if not 1 <= compress_lvl <= 9:
        raise ValueError("%{0}: Invalid compression level '{1}' chosen. Value must be within 1 and 9."
                         .format(method, compress_lvl))

    # append outfile path by netcdf-extension if required
    outfile = outfile if outfile.endswith(".nc") else outfile+".nc"
    remap_str = remapfunc_cdo(remap_method)

    ### Explanation on the CDO-operators ###
    # * --reduce_dim: Remove singleton dimensions (if any)
    # * -s: silent
    # * -f nc4: nectCDF4-format of target file
    # * -z zip_X: compress netCDF-file on compression level X (X must be within 1 and 9)
    # * <remap_str>,<outgrid>: remapping-method to target grid defined by CDO description
    # * -setgrid,<ingrid>: parse a CDO grid description for the input data
    # * -setctomiss,-999.9: Thread -999.9 as missing values (needed for CIN_ML)
    ### Explanation on the CDO-operators ###
    # generate CDO-command and ...
    #args = "cdo -L -O --reduce_dim -s -f nc4 -z zip_{0:d} {1},{2} -setgrid,{3} -setctomiss,-999.9 {4} {5}"\
    #       .format(compress_lvl, remap_str, outgrid, ingrid, infile, outfile) #TODO original run on 2017/01
    #args = "cdo -L -O --reduce_dim -s -f nc -z zip_{0:d} {1},{2} -setgrid,{3} -setctomiss,-999.9 {4} {5}"\
    #       .format(compress_lvl, remap_str, outgrid, ingrid, infile, outfile) #TODO run on 2017/02
    args = "cdo -L -O --reduce_dim -s -f nc {1},{2} -setgrid,{3} -setctomiss,-999.9 {4} {5}"\
           .format(compress_lvl, remap_str, outgrid, ingrid, infile, outfile) #TODO no zip is carried out as not supported with netCDF
    # ... run it
    term_shell(args, "%{0}: Failed remapping from '{0}' to '{1}' with grid description '{2}'"
                     .format(method, infile, outfile, outgrid), True)

    return True


def remapfunc_cdo(remap_method: str):
    """
    Chosse remapping operator of CDO accoring to method. Known methods: "bilinear", "bicubic", "nearest_neighbor",
    "distance_weighted", "conservative", "conservative2", "largest_area_fraction"
    See https://code.mpimet.mpg.de/projects/cdo/embedded/index.html#x1-6370002.12 for more details on CDO's remapping
    functions:
    :param remap_method: One method for remapping (see description above)
    :return: Proper CDO remapping operator
    """
    method = remapfunc_cdo.__name__

    known_remap = {"bilinear": "remapbil", "bicubic": "remapbic", "nearest_neighbor": "remapnn",
                   "distance_weighted": "remapdis", "conservative": "remapcon", "conservative2": "remapcon2",
                   "largest_area_fraction": "remaplaf"}

    if remap_method in known_remap.keys():
        return known_remap[remap_method]
    else:
        raise ValueError("%{0}: Chosen method for remapping '{1}' is unknown. Choose one of the following: {2}"
                         .format(method, remap_method, ", ".join(known_remap.keys())))


def modify_native_data(infile: str, outfile: str, compress_lvl: int = 6):
    """
    Modify existing netCDF-datfile with (native COSMO-)data to be ingestable to Rasdaman.
    :param infile: input netCDF-file with COSMO-data on native grid
    :param outfile: name of output netCDF-file
    :param compress_lvl: deflate compression level (must be between 1 and 9)
    :return status: True in case of success
    """
    method = modify_native_data.__name__

    # sanity checks
    if not os.path.isfile(infile):
        raise FileNotFoundError("%{0}: Could not find input file '{1}'.".format(method, infile))

    args = ["cdo -O --reduce_dim -s invertlat {1} {2}".format(compress_lvl, infile, outfile),
            "ncpdq -O --rdr=time,rlon,rlat {0} {1}".format(outfile, outfile)]

    # invert the latitude axis with CDO
    term_shell(args[0], "%{0}: Failed inversion of latitude axis with file '{1}'".format(method, infile), True)
    # ... and swap dimension order with ncpdq
    term_shell(args[1], "%{0}: Failed swapping rlat and rlon coordinate axis with file '{1}'".format(method, outfile),
               True)

    return True


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
        #if clean:
        #    cleanup() #TODO: If cleaning needs to be done by everyone this needs to be adapted
        #print(" Term Shell is failed")
        #exit_fail("Command failed: " + shell_args + "\nReason: " + err_message)
        raise SlaveError(function="term_shell()", message=err_message)


def split_time_steps(s_file, COMPRESS_LEVEL, relative_split_dir):
    """
    automatically splits the data in the 8 time steps defined in the file (nc_file). The outgoing files where stored
    in split_dir named output00000X
    Each of this eight outgoing files contain only one time step
    @param s_file: file name of the file that should be splitted in the different time_steps.
    """
    try:
        args = "cdo{0}-s splitsel,1 {1} {2}/output > /dev/null 2>&1".format(COMPRESS_LEVEL, s_file, relative_split_dir)
        term_shell(args, "Failed splitting '{0}'".format(s_file), False)
        print("{0:20} ...ok".format("Split time steps"))
    except SlaveError:
        raise


def rename_splitted_data(o_file_path, s_file, relative_split_dir):
    """
    The previous step created 8 files named 'output00000X'. This function renames this files to meaningful names.
    The new file name contains the model run start time and the 'forecast hour' and 'ensemble' of the old data file
    (the parent file s_file)
    Also the content of the new file needs to be changed. The time variable gets the forecast hour as time value, the
    units of this variable will be changed to 'hours since model run start'.
    @param o_file_path: the path where the data should be stored at
    @param s_file: the file name before it got split into the time steps
    """
    try:
        for datafile in glob.glob(relative_split_dir + "/out*"):
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
        print("{0:20} ...ok".format("Time change + mv"))
    except SlaveError:
        raise


def cleanup(relative_split_dir, relative_filter_file):
    """
    If an error occurs and the script stops it's execution, first it will call this function to delete the temporary
    splitting directory and the split file. This is only in case, the script is already splitting something. All other
    files (processed or not) will stay at their actual location and will not be deleted.
    """
    shutil.rmtree(relative_split_dir)
    os.remove(relative_filter_file)
