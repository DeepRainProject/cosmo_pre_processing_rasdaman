This project includes three different Cubes. This means we have running 3 different VMs, that have all rasdaman installed.

This is the **master branch**. The only Code that will be pushed into this branch is from the develop branch. This is the branch containing a release. No new branch will be created out of this. Use the develop branch instead.

The actual documentation can be found in the documentation folder. The scripts for the MeteoCube and GeoCube are stored in /scripts/ and for EnterpriseCube in /pipeline/.

**Here is a short overview over the Cubes:**

| Enterprise Cube | 
| --------------- | 
| 134.94.199.213  |
| `http://zam10213.zam.kfa-juelich.de:7020/rasdaman/ows`  |
| Accessible from "Forschungszentrum Jülich" and "Jacobs University Bremen" |
| It will replace the MeteoCube and will store the data provided by DWD |


| GeoCube |
| ------- |
| zam10182 (134.94.199.182) |
| `http://zam10182.zam.kfa-juelich.de:7020/rasdaman/ows` |
| Accessible from "Forschungszentrum Jülich" and "Jacobs University Bremen" |
| this will store the data mentioned here: https://gitlab.version.fz-juelich.de/toar/toar-location-services/-/wikis/Rasdaman-Data |


| MeteoCube |
| --------- |
| zam10129 (134.94.199.129) |
| `http://zam10129.zam.kfa-juelich.de:7020/rasdaman/ows` |
| Accessible from "Forschungszentrum Jülich" and "Jacobs University Bremen" |
| will be replaced by the EnterpriseCube |



