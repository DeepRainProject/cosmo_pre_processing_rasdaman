from mpi4py import MPI
import sys
import logging
import time
import os
import shutil
import glob
from datetime import datetime, timedelta

from helper import directory_scanner
from helper import load_distributor
from helper import data_structure_builder

from prepros import get_forecast_hour
from prepros import split_to_variable
from prepros import define_nc_file
from prepros import define_out_file_path
from prepros import grib_to_netcdf
from prepros import split_time_steps
from prepros import rename_splitted_data

from merger import convert_time
from merger import move_files
from merger import build_data
from merger import remove_data

from exception import MainError

# for the local machine test
current_path = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_path)
time.sleep(0)

# ini. MPI
comm = MPI.COMM_WORLD
my_rank = comm.Get_rank()  # rank of the node
p = comm.Get_size()  # number of assigned nods

# ================================== ALL Nodes:  Read-in parameters ====================================== #

# passing the parameters file and path
scriptName = sys.argv[0]
fileName = sys.argv[1]
filePath = sys.argv[2]

if my_rank == 0:  # node is master
    print("The script name is  : {name}".format(name=scriptName))
    print("The Parameters file name is  : {name}".format(name=fileName))
    print("The Parameters file path is  : {name}".format(name=filePath))

fileObj = open(fileName)
params = {}

for line in fileObj:
    line = line.strip()
    read_in_value = line.split("=")
    if len(read_in_value) == 2:
        params[read_in_value[0].strip()] = read_in_value[1].strip()

# input from the user:
job_id = int(params["Job_ID"])  # number of submitted job
source_dir = str(params["Source_Directory"])  # where data is located
destination_dir = str(params["Destination_Directory"])  # where the processed data will be placed
input_dir = str(params["Input_Directory"])  # where the setup and the config files are located
load_level = int(params["Load_Level"]) # It can be 0 whihc means monthly and 1 means daily
MAX_HOUR = int(params["MAX_HOUR"]) # defaultt for Cosmo-EPS is 21 now
COMPRESS_LEVEL = str(params["COMPRESS_LEVEL"]) # we never used this!
COMPRESS_LEVEL = 6  # TODO: @amirpasha : need perm. fix!
variables = str(params["variables"])
variables = variables.split(",") #TODO 
DEACUMMULATE_VARS = str(params["DEACUMMULATE_VARS"])
DEACUMMULATE_VARS=DEACUMMULATE_VARS.split(",") #TODO 
RENAME_VARS = str(params["RENAME_VARS"])
RENAME_VARS=RENAME_VARS.split(",") #TODO 
VAR_OLD_NAMES = str(params["VAR_OLD_NAMES"])
VAR_OLD_NAMES=VAR_OLD_NAMES.split(",") #TODO 
VAR_NEW_NAMES = str(params["VAR_NEW_NAMES"])
VAR_NEW_NAMES=VAR_NEW_NAMES.split(",") #TODO 
CHANGE_UNITS = str(params["CHANGE_UNITS"])
CHANGE_UNITS=CHANGE_UNITS.split(",") #TODO 
UNITS = str(params["UNITS"])
UNITS=UNITS.split(",") #TODO 
CHANGE_LONG_NAMES = str(params["CHANGE_LONG_NAMES"])
CHANGE_LONG_NAMES=CHANGE_LONG_NAMES.split(",") #TODO 
LONG_NAMES = str(params["LONG_NAMES"])
LONG_NAMES=LONG_NAMES.split(",") #TODO 
REMAPPED_VARS = str(params["REMAPPED_VARS"])
REMAPPED_VARS=REMAPPED_VARS.split(",") #TODO 
REMAPPED_DIRS = str(params["REMAPPED_DIRS"])
REMAPPED_DIRS=REMAPPED_DIRS.split(",") #TODO 
NATIVE_VARS = str(params["NATIVE_VARS"])
NATIVE_VARS=NATIVE_VARS.split(",") #TODO 
NATIVE_DIRS = str(params["NATIVE_DIRS"])
NATIVE_DIRS=NATIVE_DIRS.split(",") #TODO 

if my_rank == 0:  # node is master
    print(variables)
    print(DEACUMMULATE_VARS)
    print(RENAME_VARS)
    print(VAR_OLD_NAMES)
    print(VAR_NEW_NAMES)
    print(CHANGE_UNITS)
    print(UNITS)
    print(CHANGE_LONG_NAMES)
    print(LONG_NAMES)
    print(REMAPPED_VARS)
    print(REMAPPED_DIRS)
    print(NATIVE_VARS)
    print(NATIVE_DIRS)
    
in_grid = os.path.join(input_dir, "grid_des", "cde_grid")   # CDO grid description file for native COSMO grid
tar_reg_grid = os.path.join(input_dir, "grid_des", "cde_grid_unrot_invlat") # CDO grid description file for unrotated, regular
                                                                            # target grid with decreasing latitude ordering
ingest_file = input_dir + "/ingest-files.nc"  # file where all ingestions are stored in #ToDO. add folder for ingestion files
missing_path= input_dir + "/missing" #TODO: create directory where all missing-files are stored

# ==================================== Master Logging ==================================================== #
# DEBUG: Detailed information, typically of interest only when diagnosing problems.
# INFO: Confirmation that things are working as expected.
# WARNING: An indication that something unexpected happened, or indicative of some problem in the near
# ERROR: Due to a more serious problem, the software has not been able to perform some function.
# CRITICAL: A serious error, indicating that the program itself may be unable to continue running.
# It will copy the logging messages to the stdout, for the case of container version on HPC

if my_rank == 0:  # node is master
    # delete the general logger if exist
    logger_path = current_path + '/distribution_job_{job_id}.log'.format(job_id=job_id)
    if os.path.isfile(logger_path):
        print("Logger Exists -> Logger Deleted")
        os.remove(logger_path)

    ## unified log path
    log_path = current_path + '/logs_{job_id}/'.format(job_id=job_id)
    if os.path.exists(log_path):
        print('Log path for Job_iD:{job_id}  exsits-> Log directory is deleted'.format(job_id=job_id))
        shutil.rmtree(log_path)
    os.mkdir(log_path)

    logger_path_main = log_path + 'Main_log_job_{job_id}.log'.format(job_id=job_id)
    if os.path.isfile(logger_path_main):
        print("Log path for Main Job_iD:{job_id}  exsits-> Main Log is deleted")
        os.remove(logger_path_main)

    logging.basicConfig(filename='log_distribution_job_{job_id}.log'.format(job_id=job_id), level=logging.DEBUG,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(__file__)
    logger.addHandler(logging.StreamHandler(sys.stdout))

    start = time.time()  # start of the MPI
    logger.info(' === Distributor is started === ')

# check the existence of the source path :
if not os.path.exists(source_dir):  # check if the source dir. is existing
    if my_rank == 0:
        raise MainError(function="main()->checking", critical="The source does not exist", info="exit status : 1")

# Check if the destination is existing, if so, it will delete and recreate the destination_dir
if os.path.exists(destination_dir):
    if my_rank == 0:
        shutil.rmtree(destination_dir)
        os.mkdir(destination_dir)
        logger.critical('The destination exist -> Remove and Re-Create')
else:
    if my_rank == 0:
        os.makedirs(destination_dir)
        logger.critical('The destination does not exist -> Created')

# Create a log folder for slave-nodes to write down their processes
# SWITCH The following three parts are outcommented in main-test.py
# Slave_log_path is used as file-basd logging that instantly written
# worker_log is used as traditional logging

#TODO to clean up below and merged it to upper section
worker_log = current_path + '/logs_{job_id}/'.format(job_id=job_id)
#if my_rank == 0:
#    if os.path.exists(worker_log):
#        print("Worker Logger Exists -> Logger Deleted")
#        shutil.rmtree(worker_log)
#    os.mkdir(worker_log)

slave_log_path = destination_dir + "/log_temp/"  # Place to log each node in STD
if my_rank == 0:
    os.mkdir(slave_log_path)

# check the existence of the Input path :
if not os.path.exists(input_dir):  # check if the input dir. is existing
    if my_rank == 0:
        raise MainError(function="main()->checking", critical="The input directory does not exist",
                        info="exit status : 1")
        #TODO: @BOTH does it work to stop the slaves with this MainError too?

# check in_grid and tar_reg_frid (needed for remapping) in input_dir
if not os.path.isfile(in_grid):
    if my_rank == 0:
        raise MainError(function="main()->checking",
                        critical="The CDO grid description file for the native COSMO grid cannot be found.",
                        info="exit status : 1")

if not os.path.isfile(tar_reg_grid):
    if my_rank == 0:
        raise MainError(function="main()->checking",
                        critical="The CDO grid description file for the unrotated, regular target grid cannot be found.",
                        info="exit status : 1")


if my_rank == 0:  # node is master
    # ==================================== Master : Directory scanner ================================= #

    logger.info("The source path is  : {path}".format(path=source_dir))
    logger.info("The destination path is  : {path}".format(path=destination_dir))
    logger.info("==== Directory scanner : start ====")
    ret_dir_scanner = directory_scanner(source_dir, load_level)

    # Unifying the naming of this section for both cases : Sub - Directory or File
    # dir_detail_list == > Including the name of the directories, size and number of teh files in each directory / for files is empty
    # list_items_to_process    === > List of items to process  (Sub-Directories / Files)
    # total_size_source  === > Total size of the items to process
    # total_num_files    === > for Sub - Directories : sum of all files in different directories / for Files is sum of all
    # total_num_directories  === > for Files = 0

    dir_detail_list = ret_dir_scanner[0]
    list_items_to_process = ret_dir_scanner[1]
    total_size_source = ret_dir_scanner[2]
    total_num_files = ret_dir_scanner[3]
    total_num_dir = ret_dir_scanner[4]
    logger.info("==== Directory scanner : end ====")

    # ================================= Master : Data Structure Builder ========================= #

    logger.info("==== Data Structure Builder : start  ====")
    data_structure_builder(source_dir, destination_dir, dir_detail_list, list_items_to_process, load_level)
    logger.info("==== Data Structure Builder : end  ====")

    # ===================================  Master : Load Distribution   ========================== #

    logger.info("==== Load Distribution  : start  ====")
    ret_load_balancer = load_distributor(dir_detail_list, list_items_to_process, total_size_source, total_num_files,
                                         total_num_dir, load_level, p)
    transfer_dict = ret_load_balancer
    logger.info(ret_load_balancer)
    logger.info("==== Load Distribution  : end  ====")

    # ===================================== Master : Send / Receive =============================== #

    logger.info("==== Master Communication  : start  ====")

    # Send : the list of the directories to the nodes
    for nodes in range(1, p):
        broadcast_list = transfer_dict[nodes]
        comm.send(broadcast_list, dest=nodes)

    # Receive : will wait for a certain time to see if it will receive any critical error from the slaves nodes
    idle_counter = p - len(list_items_to_process)
    while idle_counter > 1:  # non-blocking receive function
        message_in = comm.recv()
        logger.warning(message_in)
        # print('Warning:', message_in)
        idle_counter = idle_counter - 1

    # Receive : Message from slave nodes confirming the sync
    message_counter = 1
    while message_counter <= len(list_items_to_process):  # non-blocking receive function
        message_in = comm.recv()
        logger.info(message_in)
        message_counter = message_counter + 1

    # stamp the end of the runtime
    end = time.time()
    logger.debug(end - start)
    logger.info('==== Pre - Process is finished / master is terminating ====')
    logger.info('exit status : 0')

    # TODO: @amirpasha clean the logger if all successful otherwise copy the failed logger back to main folder

    sys.exit(0)

else:  # Processor is slave
    # ============================================ Slave : Send / Receive ============================================ #
    message_in = comm.recv()

    # relative logger file for Job
    relative_log = worker_log + 'Slave_log_{my_rank}_job_{job_id}.log'.format(my_rank=my_rank,
                                                                                           job_id=job_id)
    if os.path.isfile(relative_log):
        print("Logger Exists -> Logger Deleted")
        os.remove(relative_log)

    logging.basicConfig(filename=relative_log, level=logging.DEBUG,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(__file__)
    logger.addHandler(logging.StreamHandler(sys.stdout))
    logger.info('Slave logger is activated')

    if message_in is None:  # in case more than number of the dir. processor is assigned !
        message_out = "Processor : {my_rank} is idle".format(my_rank=my_rank)
        logger.info(message_out)
        comm.send(message_out, dest=0)

    else:  # if the Slave node has joblist to do
        job_list = message_in.split(';')
        print(" Processor {my_rank} recived {job_list}".format(my_rank=my_rank, job_list=job_list))
        # relative logger file for Job
        slave_temp_log = slave_log_path + '/log_file_ji_{job_id}_p_{my_rank}.log'.format(job_id=job_id,
                                                                                       my_rank=my_rank)
        log = open(slave_temp_log, "w")
        log.write(' Processor {my_rank} is created this logger\n'.format(my_rank=my_rank))
        logger.info(" Processor {my_rank} recived {job_list}".format(my_rank=my_rank, job_list=job_list))

        slave_message = ""
        for job_count in range(0, len(job_list)):
            job = job_list[job_count]  # job is the name of the directory(ies) assigned to slave_node
            logger.info(' Next item to be processed is  {job}'.format(job=job))

            # create a temporary process directory inside the job folder called
            relative_source_dir = source_dir + "/" + job     # relative means destination for the current job
            relative_destination_dir = destination_dir + "/" + job
            relative_split_dir = relative_destination_dir + "/split"
            relative_filter_file = relative_split_dir + "/split_filter.txt"
            os.mkdir(relative_split_dir)
            f = open(relative_filter_file, "w")
            f.write('write "{0}/[shortName].grib[editionNumber]";'.format(relative_split_dir))
            f.close()

            ##### ======================== Start ============================ #####

            # ===== 1. Step === preparation ====================================================================
            # Read- in the ingest files
            logger.info(' Variables to import are: {variables}'.format(variables=variables))
            # define all files that should be preprocessed (laying in the given source path)
            input_files = glob.glob("{0}/cde*".format(relative_source_dir))
            input_files.sort()
            # every file in the directory will be processed
            for input_file in input_files:
                # only files with forecast_hour between 0 and MAX_HOUR where processed. (We do not need the rest)
                log.write('INFO: Next files to be processed is  {input_file}\n'.format(input_file=input_file))
                if get_forecast_hour(input_file) > MAX_HOUR: #TODO Question? if max hour is going to be anything different than 24h
                    log.write("INFO: Processor {my_rank} is skipped file: {input_file}".format(my_rank=my_rank,
                                                                                               input_file=input_file))
                    continue
                # ===== 2. Step === split into the variables using filter file =================================
                split_to_variable(input_file, relative_filter_file)

                # loop over all variable files that are created during the step before
                for var_file in os.listdir(relative_split_dir):
                    var_name = var_file.split(".")[0]  # defines the variable name of the given file
                    if var_name not in variables:
                        # This variable should not be imported and thus does not need to be preprocessed
                        log.write("DEBUG: Var skipped")
                    else: # This variables should be imported and need to be preprocessed
                        # ===== 3. Step === Grib -> NetCDF ================================
                        # name after remapping
                        nc_file = define_nc_file(relative_split_dir,input_file)
                        # specify location the file will be stored
                        out_file_path = define_out_file_path(relative_destination_dir,var_name)
                        log.write("DEBUG: Output will be stored at this location {path_name}: "
                                  .format(path_name = out_file_path))
                        # specify actual datafile
                        actual_file = glob.glob(relative_split_dir + "/" + var_file)[0]
                        # convert grib to netCDF-data
                        grib_to_netcdf(actual_file, nc_file, COMPRESS_LEVEL)
                        log.write("DEBUG: conversion (grib -> netCDF) is done for {file_name}!"
                                  .format(file_name = actual_file))
                        # ==== 4. Step === Split time steps ====================================================
                        split_time_steps(nc_file, " ", relative_split_dir)   # TODO: Revise COMPRESS_LEVEL-input (see split_time_steps-function -> can probably be removed)
                        log.write("DEBUG: split_time_steps is done for {file_name}!".format(file_name = nc_file))
                        # ==== 5. Step === Rename data =========================================================
                        rename_splitted_data(out_file_path, nc_file, relative_split_dir)
                        log.write("DEBUG: rename_splitted_data function is done on {file_name}"
                                  .format(file_name = nc_file))
                        # ==== 6. Step === Delete (old) netCdf ("parent file") =================================
                        os.remove(nc_file)
                        log.write("DEBUG: parent file is deleted ({file_name})".format(file_name = nc_file))
            # ==== 7. Step === Delete  =================================================================
            # cleanup(relative_split_dir,relative_filter_file)
            print("DEBUG: cleanup function is done on : {relative_split_dir} & {relative_filter_file} "
                  .format(relative_split_dir=relative_split_dir,relative_filter_file=relative_filter_file))
            # ==== 8. Step === Merge ===================================================================
            for var in variables:
                logger.info("Next variable to be processed is: {var_name}".format(var_name = var))
                relative_var_dir = "{path}/{var}".format(path=relative_destination_dir, var = var)
                logger.info("Relative_var_dir is located in {path_name}".format(path_name = relative_var_dir))
                # Creating temprory dir. for the variebles # TODO comeplete the cms.
                relative_tempdir = "{path_name}/tempdir".format(path_name = relative_var_dir)
                logger.info("Relative_tempdir is located in: {path_name}".format(path_name = relative_tempdir))
                # remove the temp_dir for var if it exits
                if os.path.isdir(relative_tempdir):
                    shutil.rmtree(relative_tempdir)
                    logger.info("Reletive temp dir exsist --> Deleted")
                os.mkdir(relative_tempdir)
                logger.info("DEBUG: Temporary directory created: {path_name}".format(path_name = relative_tempdir))
                # ==== extract information for building data ===================================================
                missing_file = "{path}/{var}.missing".format(path = missing_path, var = var)
                index = variables.index(var)
                deacummulate_var = DEACUMMULATE_VARS[index]
                rename_var = RENAME_VARS[index]
                old_name = VAR_OLD_NAMES[index]
                new_name = VAR_NEW_NAMES[index]
                change_units = CHANGE_UNITS[index]
                units = UNITS[index]
                change_long_name = CHANGE_LONG_NAMES[index]
                long_name = LONG_NAMES[index]
                remapped = REMAPPED_VARS[index]
                remapped_dir = REMAPPED_DIRS[index]
                native = NATIVE_VARS[index]
                native_dir = NATIVE_DIRS[index]

                # ==== extract first and last_run ==============================================================
                first_run = convert_time(relative_destination_dir)
                last_run = datetime.strptime("{date}-{hour}".format(date=first_run, hour="21"), "%Y%m%d-%H")
                first_run = datetime.strptime("{date}-{hour}".format(date=first_run, hour="00"), "%Y%m%d-%H")

                logger.info("Import files from {0} to {1}".format(first_run.strftime("%Y-%m-%d:%H"),
                                                                  last_run.strftime("%Y-%m-%d:%H")))
                model_run = first_run
                logger.info("model_run is :  {model_run}".format(model_run = model_run))

                # ==== Build Data to import ====================================================================
                while model_run <= last_run:
                    members = [str(m).zfill(2) for m in range(1, 21)]  # ["01", "02", ..]
                    for member in members:
                        logger.info("DEBUG: Process data. Member={member}, Time={time}"
                                    .format(member=member, time=model_run.strftime("%Y%m%d-%H")))
                        # move all files that belong to "model_run" to relative_tempdir
                        # and store the found hours in "existing_hours"
                        existing_hours = move_files(model_run, member, relative_tempdir, relative_var_dir)
                        logger.info("DEBUG: Files were moved. Hours are: {hours}".format(hours = existing_hours))
                        # build one datafile for model_run for that member
                        build_data(model_run, member, existing_hours, relative_tempdir, relative_var_dir, " ", in_grid,
                                   tar_reg_grid, missing_file, deacummulate_var, rename_var, old_name, new_name,
                                   change_units, units, change_long_name, long_name, remapped, remapped_dir, native,
                                   native_dir)   # ML: consider parsing arguments in a dictionary
                        logger.info("DEBUG: Files were build.")
                        # remove all datafiles that where used to build the file above (would be shorter)
                        remove_data(model_run, member, relative_var_dir, relative_tempdir)
                        logger.info("DEBUG: Files were removed")
                    logger.info("DEBUG: ============================")
                    model_run = model_run + timedelta(0, 10800)  # add three hours (go to next model run)

            job_message = "  / Directory {job} is done /".format(job=job)
            slave_message = slave_message + job_message
        # Send : the finish message back to master
        message_out = "Processor {my_rank} report : {in_message} .".format(my_rank=my_rank, in_message=slave_message)
        comm.send(message_out, dest=0)
        print(message_out)
        logger.info('Processor {my_rank} is finished this logger'.format(my_rank=my_rank))
        print('Processor {my_rank} is finished this logger\n'.format(my_rank=my_rank))
exit_status = 0
MPI.Finalize()
sys.exit(exit_status)
