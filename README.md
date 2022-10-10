# COSMO - EPS data pre-processing for Rasdaman data cube 

This is the **master branch**. It is used to publish the final code that is used to pre-process [COSMO-EPS](https://www.dwd.de/EN/research/weatherforecasting/num_modelling/04_ensemble_methods/ensemble_prediction/ensemble_prediction_en.html;jsessionid=57F90E1D36C2FBC70EE508C1A1344A42.live31081?nn=484822#COSMO-D2-EPS) data that is produced by Deutscher Wetterdienst (German Meteorological Service.
The original data resides in [Jülich Super Computing centre](https://www.fz-juelich.de/de/ias/jsc) and avilable upon request from [MeteoCloud](https://datapub.fz-juelich.de/slcs/meteocloud/doc_p_largedata_slmet_slmet111_met_data_dwd.html). 

The resulted data is avilable on Jülich node of [Federated earth server](http://fz-juelich.earthserver.xyz/rasdaman/ows#/services), developed by [Jacobs University of Bremen](https://www.jacobs-university.de/)

![earth_server](https://user-images.githubusercontent.com/17433615/194857453-fe69ae19-f37d-456b-b256-3d09ce8d69bb.png)

Source: https://earthserver.eu/


The code is designed nativly as a wrapper for [mpi4py](https://mpi4py.readthedocs.io/en/stable/) to destribute the work accross mutiple workers. It is developed and deployed on [Jülich Super Computing centre](https://www.fz-juelich.de/de/ias/jsc) infrastructure including [JUWELS cluster and JUWELS booster](https://www.fz-juelich.de/en/ias/jsc/systems/supercomputers/juwels). 

![image](https://user-images.githubusercontent.com/17433615/194859261-aca11a2c-0071-4912-b98d-e72abf46c63a.jpeg)

Source: https://www.fz-juelich.de/

This repository required the following packages:

"""
test

"""
