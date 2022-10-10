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

from helper import directory_scanner
from helper import load_distributor
from helper import data_structure_builder

from prepros import extract_ingestions
from prepros import read_ingestions
from prepros import get_forecast_hour
from prepros import split_to_variable
from prepros import define_step_file
from prepros import define_out_file_path
from prepros import change_grid
from prepros import split_time_steps
from prepros import rename_splitted_data
from prepros import cleanup

from merger import convert_time
from merger import needs_to_create_missing
from merger import move_files
from merger import build_data
from merger import remove_data
from merger import get_member

from exception import MainError
from exception import SlaveError

try:
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
    # JobID = sys.argv[3] #TODO: it will read in the job ID from the batch script

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
    job_id = int(params["Job_ID"])  # number of submitted job TODO @amirpasha: it should read in Slurm job number
    # job_id = int(JobID)                                       # will replace the line above
    source_dir = str(params["Source_Directory"])  # where data is located
    destination_dir = str(params["Destination_Directory"])  # where the processed data will be placed
    input_dir = str(params["Input_Directory"])  # where the setup and the config files are located
    load_level = int(params["Load_Level"])
    MAX_HOUR = int(params["MAX_HOUR"])
    COMPRESS_LEVEL = str(params["COMPRESS_LEVEL"])
    COMPRESS_LEVEL = " "  # TODO: @amirpasha : need perm. fix!
    rot_grid = input_dir + "/gridneu.dat"  # this file describes how to change the grids
    ingest_file = input_dir + "/ingest-files.nc"  # file where all ingestions are stored in #ToDO. add folder for ingestion files 
    missing_path= input_dir + "/missing/"#TODO: create directory where all missing-files are stored

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
            os.mkdir(destination_dir)
            logger.critical('The destination does not exist -> Created')

    # Create a log folder for slave-nodes to write down their processes
    slave_log_path = destination_dir + "/log_temp"  # TODO: @Amirpasha bring this line up

    if my_rank == 0:
        os.mkdir(slave_log_path)

    # check the existence of the Input path :
    if not os.path.exists(input_dir):  # check if the input dir. is existing
        if my_rank == 0:
            raise MainError(function="main()->checking", critical="The input directory does not exist", info="exit status : 1")
            #TODO: @BOTH does it work to stop the slaves with this MainError too?

    # check rot_grid data (needed to change the coordinate system) in the Input_dir
    if not os.path.isfile(rot_grid):
        if my_rank == 0:
            raise MainError(function="main()->checking", critical="The data to change the grid does not exist",
                            info="exit status : 1")

    if my_rank == 0:  # node is master
        ingestions = []  # using extract_ingestions we get all possible ingestion files
        logger.info('Master : initiate the ingestion file ')
        # ingestions = extractIngestions()  # TODO: @Jessica function does not exists

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

        # ===============================  Master : Prepare Ingestion data  ========================== #
        logger.info("==== Prepare Ingestion data  : start  ====")
        logger.debug("before ingestion")
        # ingestions = extract_ingestions(input_dir,ingest_file) #TODO - INFO  @jessica function does have a return value so we can not assign it to ingest
        #TODO @Jessica check the existence of teh missing files 
        #                    if not os.path.isfile(missing):
        #               raise SlaveError(function="main()->merging",
        #                                message="No file for missing values given!")
        extract_ingestions(input_dir, ingest_file)  # TODO - TODO @jessica check if the function works ;-)
        #TODO @BOTH is the above ingest part done then?
        #TODO @BOTH maybe create one file for the master to create a clearer structure?
        logger.debug("after ingestion")
        logger.info("==== Prepare Ingestion data : end  ====")

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
        try:
            # ============================================= Slave : Send / Receive ============================================ #
            message_in = comm.recv()
            if message_in is None:  # in case more than number of the dir. processor is assigned !
                message_out = "Processor : {my_rank} is idle".format(my_rank=my_rank)
                comm.send(message_out, dest=0)

            else:  # if the Slave node has joblist to do
                job_list = message_in.split(';')
                print(" Processor {my_rank} recived {job_list}".format(my_rank=my_rank, job_list=job_list))
                # relative logger file for Job
                relative_log = slave_log_path + '/log_file_ji_{job_id}_p_{my_rank}.log'.format(job_id=job_id,
                                                                                               my_rank=my_rank)
                log = open(relative_log, "w")
                log.write(' Processor {my_rank} is created this logger\n'.format(my_rank=my_rank))
                slave_message = ""
                for job_count in range(0, len(job_list)):
                    job = job_list[job_count]  # job is the name of the directory(ies) assigned to slave_node
                    log.write(' Next item to be processed is  {job}\n'.format(job=job))

                    #  stop the slave processor to continue further and send the failed attempt to the master node.
                    source_fail = False
                    pre_process_is_done = False
                    # create a temporary process directory inside the job folder called
                    relative_source_dir = source_dir + "/" + job
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
                    ingestions = read_ingestions(ingest_file)
                    # define all files that should be preprocessed (laying in the given source path)
                    input_files = glob.glob("{0}/cde*".format(relative_source_dir))
                    input_files.sort()
                    # every file in the directory will be processed
                    for input_file in input_files:
                        # only files with forecast_hour between 0 and 24 where processed. (We do not need the rest)
                        log.write('INFO: Next files to be processed is  {input_file}\n'.format(input_file=input_file))
                        if get_forecast_hour(input_file) > MAX_HOUR:
                            log.write("INFO: Processor {my_rank} is skipped file: {input_file}".format(my_rank=my_rank,
                                                                                                       input_file=input_file))
                            continue
                        else:
                            get_forecast_hour(input_file)
                        log.write('INFO:Function1: get_forecast_hour is applied on {input_file}\n'.format(
                            input_file=input_file))
                        # ===== 2. Step === split into the variables using filter file =================================
                        split_to_variable(input_file, relative_filter_file)
                        # ===== 3. Step === Grib -> NetCDF (and change coordinate grid) ================================
                        for var_file in os.listdir(relative_split_dir):
                            var_name = var_file.split(".")[0]  # defines the variable name of the given file
                            if var_name not in ingestions:
                                log.write("DEBUG:Var skipped")
                            else:  # part of the ingestion
                                # name after changing the grid
                                step_file = define_step_file(relative_split_dir,input_file)
                                # specify location the file will be stored
                                out_file_path = define_out_file_path(relative_destination_dir,var_name)
                                log.write("DEBUG:Output will be recorded here later {out_file_path}: ".format(
                                    out_file_path=out_file_path))
                                # specify actual datafile
                                actual_file = glob.glob(relative_split_dir + "/" + var_file)[0]
                                # changes the grid of the actual file
                                change_grid(COMPRESS_LEVEL, rot_grid, actual_file, step_file)
                                log.write("DEBUG: Change Grid is done for {var_name}!".format(var_name=var_name))
                                #    log.write('Function : "Grib -> NetCDF" is applied on {input_file}\n'.format(input_file=input_file))   # TODO : remove after testing
                                # ==== 4. Step === Split time steps ====================================================
                                split_time_steps(step_file, COMPRESS_LEVEL, relative_split_dir)
                                log.write("DEBUG: split_time_steps is done for {var_name}!".format(var_name=var_name))
                                # ==== 5. Step === Rename data =========================================================
                                log.write("DEBUG: rename_splitted_data function will be run now : on {step_file}".format(
                                        step_file=step_file))
                                # log.write(' Function : rename_splitted_data(out_file_path, step_file) is applied on {input_file}\n'.format(input_file=input_file))   # TODO : remove after testing
                                rename_splitted_data(out_file_path, step_file, relative_split_dir)
                                log.write("DEBUG: rename_splitted_data function is done : on {step_file}".format(
                                    step_file=step_file))
                                # ==== 6. Step === Delete (old) netCdf ("parent file") =================================
                                log.write("DEBUG: rename_splitted_data function is done : on {step_file}".format(
                                    step_file=step_file))
                                os.remove(step_file)
                                log.write("DEBUG: rename_splitted_data function is done : on {step_file}".format(
                                    step_file=step_file))
                    # ==== 7. Step === Delete  =================================================================
                    print("DEBUG: cleanup function is going to start on : {relative_split_dir} & {relative_filter_file} ".format(relative_split_dir=relative_split_dir,relative_filter_file=relative_filter_file))
                    # cleanup(relative_split_dir,relative_filter_file)
                    print("DEBUG: cleanup function is done on : {relative_split_dir} & {relative_filter_file} ".format(relative_split_dir=relative_split_dir,relative_filter_file=relative_filter_file))
                    # ==== 8. Step === Merge ===================================================================

                    for var in ingestions: # Replacing the missing values from default value locate din the missing_path 
                        missing="{path}missing-{var}.nc".format(path=missing_path, var=var)
                        relative_tempdir = "{dir}/tempdir".format(dir=relative_destination_dir)

                        # ==== extract start and end_time ==============================================================
                        start_time = convert_time(relative_destination_dir)
                        end_time = datetime.strptime("{date}-{hour}".format(date=start_time, hour="21"), "%Y%m%d-%H")
                        start_time = datetime.strptime("{date}-{hour}".format(date=start_time, hour="00"), "%Y%m%d-%H")
                        # Todo : delte the line below 
                        print("Import files from {0} to {1}".format(start_time.strftime("%Y-%m-%d:%H"), end_time.strftime("%Y-%m-%d:%H")))
                        log.write("Import files from {0} to {1}".format(start_time.strftime("%Y-%m-%d:%H"), end_time.strftime("%Y-%m-%d:%H")))
                        actual_time = start_time

                        # ==== Build Data to import ====================================================================

                        while actual_time <= end_time:
                            members = [str(m).zfill(2) for m in range(1, 21)]  # ["01", "02", ..]
                            for member in members:
                                existing = move_files(actual_time, member, relative_tempdir, relative_destination_dir)  # move all founded files to tempdir
                                build_data(actual_time, member, existing, missing, relative_tempdir, relative_destination_dir, " ")  # build one datafile for actual_time for that member
                                remove_data(actual_time, member, relative_tempdir, relative_destination_dir)  # remove all datafiles that where used to build the file above

                            actual_time = actual_time + timedelta(0, 10800)  # add three hours (go to next model run)

                    job_message = "  / Directory {job} is done /".format(job=job)
                    slave_message = slave_message + job_message
                # Send : the finish message back to master
                message_out = "Processor {my_rank} report : {in_message} .".format(my_rank=my_rank,
                                                                                   in_message=slave_message)
                comm.send(message_out, dest=0)
                print(message_out)
                log.write('Processor {my_rank} is finished this logger\n'.format(my_rank=my_rank))
                print('Processor {my_rank} is finished this logger\n'.format(my_rank=my_rank))
        except SlaveError as e:
            log.write("In {} the following error occurred:".format(e.function))
            log.write(e.message)
        finally:
            log.close()
    exit_status = 0
except MainError as e:
    logger.critical(e.critical)
    logger.info(e.info)
    exit_status = 1
finally:
    MPI.Finalize()
    sys.exit(exit_status)
