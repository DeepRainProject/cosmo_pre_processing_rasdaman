This directory contains all scripts needed for the import of data to rasdaman. 

1.  To import the data first use `01.preproc.py <source-path>` where `<source-path>` contains the data that should be preprocessed.
    
    *  This needs the datafile "gridneu.dat" because the data from DWD stores latitude and longitude in "rotated pole grid" and we have to change this to "equidistand grid". 

2.  After that `02.split.py <path>` must be executed, where `<path>` is the path used as working-directory of "01-preproc.py" (usually `preproc/<VAR>/`).

3.  The last step is importing the data using `03.import.py <path>`. `<path>` again is the working-directory (usually `preproc/<VAR>/`). 
    * This script also needs the ingest-template file (e.g. ingest_t.json.template) in the ingest-directory (`ingest/`).
    * This also needs the missing values defined in one directory (e.g. `/preproc/missing`). 

For more information look at the documentations. 