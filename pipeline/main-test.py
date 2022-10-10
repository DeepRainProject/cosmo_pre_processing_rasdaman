from mpi4py import MPI
import sys
import logging
import time
import os
import subprocess
import shutil
import glob
from datetime import datetime, timedelta

from helper import directory_scanner
from helper import load_distributor
from helper import data_structure_builder

from prepros import extract_ingestions
from prepros import read_ingestions

from merger import convert_time
from merger import move_files
from merger import build_data
from merger import remove_data

from exception import MainError
from exception import SlaveError


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

    # delete the Log-directory #TODO @amirpasha based on job_id


    logger_path_main = current_path +'/logs/'+ 'Main_log_job_{job_id}.log'.format(job_id=job_id)
    if os.path.isfile(logger_path_main):
        print("Logger Exists -> Logger Deleted")
        os.remove(logger_path_main)

    logging.basicConfig(filename=logger_path_main, level=logging.DEBUG,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    logger = logging.getLogger(__file__)
    logger.addHandler(logging.StreamHandler(sys.stdout))

    start = time.time()  # start of the MPI
    logger.info(' === Distributor is started === ')

# check the existence of the source path :
if not os.path.exists(source_dir):  # check if the source dir. is existing
    if my_rank == 0:
        raise MainError(function="main()->checking", critical="The source does not exist", info="exit status : 1")

# Check if the destination is existing. This should be true, because only step 8 is executed
if not os.path.exists(destination_dir):
    if my_rank == 0:
        raise MainError(function='main()->checking', critical="The destination does not exist", info="exit status : 1")

# Create a log folder for slave-nodes to write down their processes
#slave_log_path = destination_dir + "/log_temp"  # TODO: @Amirpasha bring this line up

#if not os.path.exists(slave_log_path):
#    if my_rank == 0:
#        os.mkdir(slave_log_path)

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
    # ============================================= Slave : Send / Receive ============================================ #
    # this will keep the slave nod engage until the master is done 
    message_in = comm.recv()
    
    # relative logger file for Job
    relative_log = current_path +'/logs/'+ 'Slave_log_{my_rank}_job_{job_id}.log'.format(my_rank=my_rank,job_id=job_id)
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
        logger.info(" Processor {my_rank} recived {job_list}".format(my_rank=my_rank, job_list=job_list))

        slave_message = ""
        for job_count in range(0, len(job_list)):
            job = job_list[job_count]  # job is the name of the directory(ies) assigned to slave_node
            logger.info(' Next item to be processed is  {job}'.format(job=job))

            #  stop the slave processor to continue further and send the failed attempt to the master node.
            source_fail = False
            pre_process_is_done = False
            relative_destination_dir = destination_dir + "/" + job
            logger.info(' Next Relative destination location is : {ingestions}'.format(ingestions=relative_destination_dir))

            ##### ======================== Start ============================ #####

            # ===== 1. Step === preparation ====================================================================
            # Read- in the ingest files
            ingestions = read_ingestions(ingest_file)
            logger.info(' Ingestions is : {ingestions}'.format(ingestions=ingestions))

            # ==== Step === Merge ==============================================================================

            for var in ingestions: # Replacing the missing values from default value locate din the missing_path
                logger.info("Next var in ingestion-list to be processed is :  {var_name}".format(var_name=var))
                relative_var_dir = "{path}/{var}".format(path=relative_destination_dir, var=var)
                logger.info("Relative_var_dir is located in {relative_var_dir}".format(relative_var_dir=relative_var_dir))
                missing="{path}missing-{var}.nc".format(path=missing_path, var=var)
                logger.info("Missing Var is located in :  {missing}".format(missing=missing))
                relative_tempdir = "{dir}/tempdir".format(dir=relative_var_dir)
                logger.info("Relative_tempdir is located in :  {relative_tempdir}".format(relative_tempdir=relative_tempdir))
                # remove the temp_dir for var
                if os.path.isdir(relative_tempdir):
                    logger.info("Reletive temp dir exsist")
                    #delete_message = ('rm -r {relative_tempdir}'.format(relative_tempdir=relative_tempdir))
                    #subprocess.call(delete_message)
                    shutil.rmtree(relative_tempdir)
                    logger.info("Reletive temp dir exsist --> Deleted")
                os.mkdir(relative_tempdir)
                logger.info("DEBUG: Temporary directory created: {path}".format(path=relative_tempdir))
                # ==== extract start and end_time ==============================================================
                start_time = convert_time(relative_destination_dir)
                end_time = datetime.strptime("{date}-{hour}".format(date=start_time, hour="21"), "%Y%m%d-%H")
                start_time = datetime.strptime("{date}-{hour}".format(date=start_time, hour="00"), "%Y%m%d-%H")
                logger.info("Import files from {0} to {1}".format(start_time.strftime("%Y-%m-%d:%H"), end_time.strftime("%Y-%m-%d:%H")))
                actual_time = start_time
                logger.info("Actual_time is :  {actual_time}".format(actual_time=actual_time))
                # ========== Make them Directory 
                #os.mkdir(relative_tempdir, exist_ok=True)
                #log.write("DEBUG: Temporary directory created: {path}\n".format(path=relative_tempdir))
                # ==== Build Data to import ====================================================================
                while actual_time <= end_time:
                    members = [str(m).zfill(2) for m in range(1, 21)]  # ["01", "02", ..]
                    for member in members:
                        # ==== 5. Step === Rename data =========================================================
                        logger.info("DEBUG: Process data. Member={member}, Time={time}".format(member=member,
                                                                                             time=actual_time.strftime("%Y%m%d-%H"))        )
                        existing = move_files(actual_time, member, relative_tempdir, relative_var_dir) # move all founded files to tempdir
                        logger.info("DEBUG: Files were moved. Hours are: {hours}".format(hours=existing))
                        build_data(actual_time, member, existing, missing, relative_tempdir, relative_var_dir, " ")  # build one datafile for actual_time for that member
                        logger.info("DEBUG: Files were build.")
                        remove_data(actual_time, member, relative_var_dir,relative_tempdir)  # remove all datafiles that where used to build the file above
                        logger.info("DEBUG: Files were removed")
                    logger.info("DEBUG: ============================")
                    actual_time = actual_time + timedelta(0, 10800)  # add three hours (go to next model run
                    logger.info("Actual time: {actual_time}".format(actual_time=actual_time))
            job_message = "  / Directory {job} is done /".format(job=job)
            slave_message = slave_message + job_message
        # Send : the finish message back to master
        message_out = "Processor {my_rank} report : {in_message} .".format(my_rank=my_rank,
                                                                           in_message=slave_message)
        comm.send(message_out, dest=0)
        print(message_out)
        logger.info('Processor {my_rank} is finished this logger'.format(my_rank=my_rank))
        print('Processor {my_rank} is finished this logger'.format(my_rank=my_rank))

MPI.Finalize()
