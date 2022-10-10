#!/bin/bash -x
#SBATCH --account=deepacf
#SBATCH --nodes=2
#SBATCH --ntasks=48
##SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --gres=gpu:0
##SBATCH --gres=gpu:N
#SBATCH --output=log_Rasdaman_prepros_out.%j
#SBATCH --error=log_Rasdaman_prepros_err.%j
#SBATCH --time=10:00:00
#SBATCH --partition=batch
#SBATCH --mail-type=ALL
#SBATCH --mail-user=a.mozaffari@fz-juelich.de
##jutil env activate -p cdeepacf


module --force purge
module use $OTHERSTAGES
module load Stages/2020

module load GCC/10.3.0
module load ParaStationMPI/5.4.10-1
module load CDO/2.0.0rc3
module load netCDF/4.7.4
module load SciPy-Stack/2021-Python-3.8.5
module load mpi4py/3.0.3-Python-3.8.5
module load netcdf4-python/1.5.4-Python-3.8.5
module load NCO/4.9.5

