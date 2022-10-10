#!/usr/bin/env python3
"""
This is the parent script creating the rasdaman parameters.
"""
import logging
import time
import os
import shutil 
from shutil import copyfile
import sys
from mpi4py import MPI


## ========= User input ======= ##

# year_start = 2017
# year_end = 2017 
# month_start = 03
# month_end = 04
scriptName = sys.argv[0]
year = sys.argv[1]
month = sys.argv[2]
#year = "2017" 
#month = "01"
logger_ID = year + month 
## ============================ ## 


def create_parameter_file(file_name, year, month):
    f = open(file_name, "w")
    f.write("# ============ input parameters =================== #\n")
    f.write("# 0:deactivate / 1: active\n")
    f.write("# Load Level =  = 0: sub-directory level / 1: file level\n")
    f.write("\n")
    f.write("Job_ID = {year}{month}\n".format(year=year, month=month))
    f.write(
        "Source_Directory = /p/scratch/deepacf/deeprain/cosmo-eps/{year}/{month}/\n".format(year=year, month=month))
    f.write("Destination_Directory = /p/scratch/deepacf/deeprain/cosmo-eps_process/remaped_precp/{year}/{month}\n".format(year=year,
                                                                                                     month=month))
    f.write("Input_Directory = /p/project/deepacf/deeprain/mozaffari1/rasdaman/remaped_precip/rasdaman/input\n")
    f.write("Load_Level = 0\n")
    f.write("MAX_HOUR = 24\n")
    f.write('COMPRESS_LEVEL = 6\n')
    f.write('variables = tp\n')
    f.write('DEACUMMULATE_VARS = true\n')
    f.write('RENAME_VARS = true\n')
    f.write('VAR_OLD_NAMES = tp\n')
    f.write('VAR_NEW_NAMES = PR1h\n')
    f.write('CHANGE_UNITS = true\n')
    f.write('UNITS = kg m**-2 h**-1\n')
    f.write('CHANGE_LONG_NAMES = true\n')
    f.write('LONG_NAMES = Hourly Precipitation\n')
    f.write('REMAPPED_VARS = true\n')
    f.write('REMAPPED_DIRS = remapped\n')
    f.write('NATIVE_VARS =\n')
    f.write('NATIVE_DIRS = ""\n') 
    f.close()

def create_batch_file(file_name, template_file, parameter_file_name, script_name, destination):
    copyfile(template_file, file_name)
    f = open(file_name, "a")
    f.write("srun python {script} {parameters} {dest}".format(script=script_name, parameters=parameter_file_name, dest=destination))

def main():
    comm = MPI.COMM_WORLD
    my_rank = comm.Get_rank()  # rank of the node
    p = comm.Get_size()  # number of assigned nods
    if my_rank == 0:  # node is master
        logging.basicConfig(filename='log_creator_{logger_ID}.log'.format(logger_ID=logger_ID), level=logging.DEBUG,
                            format='%(asctime)s:%(levelname)s:%(message)s')
        logger = logging.getLogger(__file__)
        logger.addHandler(logging.StreamHandler(sys.stdout))

        template_file = "template_Batch_hdfml_Rasdaman_WF.sh"
        script_name = "main.py"
        destination = "/p/project/deepacf/deeprain/mozaffari1/rasdaman/remaped_precip/rasdaman"
        logger.info(" Read in Batch template from {template_file}".format(template_file = template_file))

         #for year in range(2017, 2017):  # [2011,2012,...,2017]
                #pos_month = [str(m).zfill(2) for m in range(1, 13)]  # [01,02,...,12]
                #for month in pos_month:
        parameter_file_name = "parameters_Rasdaman_{year}{month}.dat".format(year=year, month=month)
        batch_file_name = "s{year}{month}_Batch_hdfml_Rasdaman_WF_.sh".format(year=year, month=month)
        create_parameter_file(parameter_file_name, year, month)
        create_batch_file(batch_file_name, template_file, parameter_file_name, script_name, destination)
        logger.info(" Parameters file is created with name : {parameter_file_name}".format(parameter_file_name = parameter_file_name))
        logger.info(" Batch file is created with name : {batch_file_name}".format(batch_file_name = batch_file_name))

if __name__ == "__main__":
    main()