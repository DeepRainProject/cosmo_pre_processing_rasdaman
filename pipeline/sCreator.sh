#!/bin/bash -x
#SBATCH --account=deepacf
#SBATCH --nodes=1
#SBATCH --ntasks=1
##SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --gres=gpu:0
##SBATCH --gres=gpu:N
#SBATCH --output=creator_out.%j
#SBATCH --error=creator_err.%j
#SBATCH --time=00:01:00
#SBATCH --partition=batch
#SBATCH --mail-type=ALL
#SBATCH --mail-user=a.mozaffari@fz-juelich.de
##jutil env activate -p cdeepacf


module purge
module use $OTHERSTAGES
module load Stages/2019a
module load GCC/8.3.0
module load ParaStationMPI/5.4.4-1

module load SciPy-Stack/2019a-Python-3.6.8
module load mpi4py/3.0.1-Python-3.6.8
module load netCDF/4.6.3
module load basemap/1.2.0-Python-3.6.8
module load netcdf4-python/1.5.0.1-Python-3.6.8

srun python creator.py 2017 05
