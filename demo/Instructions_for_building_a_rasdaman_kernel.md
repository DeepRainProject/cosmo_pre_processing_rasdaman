Instructions for building a rasdaman kernel for Jupyter by Martin and paths tested by Amirpasha


# Instructions for building a rasdaman kernel for Jupyter

## load modules
module --force purge
module use $OTHERSTAGES
ml Stages/2018b
ml GCCcore/.7.3.0 Python

## set up virtual environment
python3 -m venv rasdaman_env
source rasdaman_env/bin/activate

## change python path
export PYTHONPATH=/p/home/jusers/mozaffari1/juwels/rasdaman_env/lib/python3.6/site-packages:${PYTHONPATH}

## install python packages
pip install --upgrade pip
pip install --ignore-installed ipykernel
pip install OWSLib
pip install netCDF4
pip install xarray
pip install h5netcdf
pip install matplotlib

## create kernel
python3 -m ipykernel install --user --name=rasdaman_env

## create kernel.sh run script
vi rasdaman_env/kernel.sh

content:
#! /bin/bash 
module use /usr/local/software/juwels/OtherStages
module load Stages/2018b GCCcore/.7.3.0 Python
source /p/home/jusers/mozaffari1/juwels/rasdaman_env/bin/activate
export PYTHONPATH=/p/home/jusers/mozaffari1/juwels/rasdaman_env/lib/python3.6/site-packages:${PYTHONPATH}
exec python -m ipykernel $@


make it executable:
chmod +x rasdaman_env/kernel.sh

## Update your kernel file to use the kernel.sh script
vi ~/.local/share/jupyter/kernels/rasdaman_env/kernel.json

Content:
{
 "argv": [
  "/p/home/jusers/mozaffari1/juwels/rasdaman_env/kernel.sh",
  "-m",
  "ipykernel_launcher",
  "-f",
  "{connection_file}"
 ],
 "display_name": "rasdaman_env",
 "language": "python"
}





## current workaround for special characters in xarray

vi rasdaman_env/lib/python3.6/site-packages/xarray/backends/h5netcdf_.py

replace the existing maybe function with this one:

def maybe_decode_bytes(txt):
    if isinstance(txt, bytes):
        if (txt == b'Deutscher Wetterdienst - Zentrale, Frankfurter Stra\xdfe 135, 63067 Offenbach/Main'):
            txt = txt.replace(b'\xdfe',b'\xc3\x9fe')
            return txt.decode("utf-8")
        else:
            return txt.decode('utf-8')
    else:
        return txt

