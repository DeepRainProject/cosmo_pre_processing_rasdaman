*************
Datastructure
*************

Location: ``/mnt/nfs/grb/<Year>/<Month>/<Day>/``\\
Filename: ``cde<YYYYMMDD>.FF.mEE.<Extension>`` \\

-----------
Definitions
-----------
:Forecast Hour: The number of hours that have passed since the model run started.


------
Origin
------
The data stored in the rasdaman database originally comes from the DWD. It is stored in Grib1 or Grib2 format. Every day contains 8 modelruns. Each model run runs 21 to 45 hours (forecast hours). For each of this hour of the model run 20 different ensemble member are stored. Each file contains for all this information 74 variables.\\

-------
Format
-------
Every day 8 model runs where started. At 00, 03, 06, 09, 12, 15, 18, 21 o Clock. Depending on which year we are looking at 21 to 45 forecast hours where stored:
- 2011 to 2012 21 forecast hours are stored every day
- 2013 to 2014 27 forecast hours are stored every day
- 2015 to 2017 45 forecast hours are stored every day

| The file name already contains the following information:

| The start of the model runs (hours are given in the file):

:year:			cde**YYYY**\MMDD.FF.mEE
:month:			cdeYYYY**MM**\DD.FF.mEE
:day:			cdeYYYYMM**DD**\.FF.mEE

| The forecast hours (see above)

:forecast hour:		cdeYYYYMMDD.**FF**\.mEE

| The ensemble member (the members 1 up to 20 are stored):

:ensemble member:	cdeMMDD.FF.m**EE**


---------
Workflow
---------
1) Preprocessing (variables)
    - Split data into 74 variables
    - remove unneeded variable-files
    - Convert from Grib-format to NetCdf
2) Preprocessing (time steps)
    - split files into the 8 model runs
    - rename the output
    - delete old NetCdf file
3) Data Management and Ingestion
    - merge datafiles
    - prepare ingest-file
    - import data
    - remove items after ingestion

As visible: This three steps are devided in preprocessing and importing. That is why two scripts need to be executed. 
