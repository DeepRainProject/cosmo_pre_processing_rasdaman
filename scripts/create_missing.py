#!/usr/bin/env python3

#===== imports    =================================================

import sys
import os
import subprocess

#==================================================================
if len(sys.argv) == 4:
    dat_file = sys.argv[1]
    var_name = sys.argv[2]
    miss_val = sys.argv[3]
    if not os.path.isfile(dat_file):
        print(dat_file+": This needs to be the datafile which is used to create the missing file")
        sys.exit()
    dest_dir=os.getcwd()+"/preproc/"+var_name
    if not os.path.isdir(dest_dir):
        dest = "missing.nc"
    else:
        dest = dest_dir+"/missing.nc"
else:
    print("Usage: ./create_missing.py <DAT> <VAR> <MISS>")
    print("DAT= datafile which is used to create the missing file")
    print("VAR= variable for which the missing file is created (must fit to datafile of course)")
    print("MISS= definition of missing value. Should be meaningful in sense of missing values of that variable")
    sys.exit()

args="ncap2 -s 'where("+var_name+">0) "+var_name+"="+miss_val+"; elsewhere "+var_name+"="+miss_val+";' "+dat_file+" "+dest
return_code = subprocess.call(args, shell=True) 
if return_code != 0:
    print("Could not create file - something went wrong")