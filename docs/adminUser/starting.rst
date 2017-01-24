
.. hightlight:: rest

########
Overview
########

.. contents::  

***************
Important notes
***************

The user lmwriter performs all processes associated with running a Lifemapper
instance.  Using the root user will result in read/write permissions on 
data and logfiles getting set incorrectly, and will cause errors in later 
operations.

The 'config' file referenced below is <APP_PATH>/config/config.lmserver.ini 
and config.lmcompute.ini.  Variables in these files may be overridden by the 
user in <APP_PATH>/config/site.ini.  APP_PATH is also present in the config 
file, and generally is configured as /opt/lifemapper.

Any feature described below by "Currently" will be made more configurable in the 
near future.  

****************
Data computation
****************
LmServer **initBoom** (LmDbServer.tools.initBoom.py) populates the database with 
inputs for a BOOM archive from an input data package and parameters.  

LmServer **archivistBorg** (LmDbServer.tools.archivistBorg) assembles jobs for 
computation from a configuration file created by initBoom.  Objects and related 
makeflow document are recorded in the database.

LmCompute **JobMediator** requests jobs, retrieves them, 
sends them to SGE for computation, then returns them to LmServer.  Currently the
JobMediator only communicates via http. 

LmServer **JobManager** responds to requests for jobs, sending inputs and/or
urls pointing to input data.  The JobManager also accepts completed 
results, storing data on the filesystem and recording completed/error status 
in the database.

**************
Data retrieval
**************
All object metadata and data (complete or not) can be queried through 
REST services.  An incomplete browse/query interface is available at 
<FQDN>/services .

#####################
Starting job creation
#####################

The pipeline creates jobs from input species data (either a csv file or a list
of species names/ids and a web service supported by the apiquery module).  
The pipeline uses variables set in the config file.  These variables correspond 
to values in the database and either Lifemapper code or provided data. 

To run the initialization script, initBoom, as user lmwriter do::

    $ $PYTHON /opt/lifemapper/LmDbServer/tools/initBoom.py
    
To start the archivistBorg daemon (previously pipeline) as user lmwriter do::

    $ $PYTHON /opt/lifemapper/LmDbServer/tools/archivistBorg.py start

To stop::

    $ $PYTHON /opt/lifemapper/LmDbServer/tools/archivistBorg.py stop
    
    
##########################
Modifying archive creation
##########################

The LmServer roll comes with default species data, environmental data, and
algorithm parameters.  These may all be modified.

************
Species Data
************
Species data may be pre-defined CSV file, such as the GBIF data dump, or 
a web service, such as the BISON service.  The **DATASOURCE** variable in the 
config file is a keyword which identifies the source and tells the pipeline 
how to process it.

Currently species data must be one of the pre-defined options, GBIF, BISON,
or IDIGBIO.

******************
Environmental Data
******************
Available environmental data is defined by the **SCENARIO_PACKAGE** variable in the 
config file. One scenario for modeling (for current-day species data, this is 
usually observed environmental data), **DEFAULT_MODEL_SCENARIO**, and 
one or more for projecting, **DEFAULT_PROJECTION_SCENARIOS**, are set in the 
config file.

Currently environmental data must be one of the pre-defined options; 
SCENARIO_PACKAGE codes, and their corresponding scenario codes are listed
in each package's metadata file (SCENARIO_PACKAGE.py), in 
LmDbServer.tools.bioclimMeta.py and here::
   
     * 30sec-present-future-SEA (Southeast Asia)
            
         # current: WC-30sec-SEA
         # future: CCSM4-RCP4.5-2050-30sec-SEA
                   CCSM4-RCP4.5-2070-30sec-SEA
                   CCSM4-RCP8.5-2050-30sec-SEA
                   CCSM4-RCP8.5-2070-30sec-SEA
                    
     * 30sec-present-future-CONUS (Continental United States)
            
         # current: WC-30sec-CONUS
         # future: CCSM4-RCP4.5-2050-30sec-CONUS
                   CCSM4-RCP4.5-2070-30sec-CONUS
                   CCSM4-RCP8.5-2050-30sec-CONUS
                   CCSM4-RCP8.5-2070-30sec-CONUS

     * 5min-past-present-future (global)

         # past: CCSM4-lgm-5min (last glacial maximimum)
                 CCSM4-mid-5min (mid-holocene)
         # current: WC-5min
         # future: CCSM4-RCP4.5-2050-5min
                   CCSM4-RCP4.5-2070-5min
                   CCSM4-RCP8.5-2050-5min
                   CCSM4-RCP8.5-2070-5min

     * 10min-past-present-future (global)

         # past: CCSM4-lgm-10min (last glacial maximimum)
                 CCSM4-mid-10min (mid-holocene)
         # current: WC-10min
         # future: CCSM4-RCP4.5-2050-10min
                   CCSM4-RCP4.5-2070-10min
                   CCSM4-RCP8.5-2050-10min
                   CCSM4-RCP8.5-2070-10min
                   

These data may be downloaded from svc.lifemapper.org/dl/ with filenames the code 
with extension tar.gz.  Metadata for each of these packages is included in the 
source code, and will be populated correctly for the configured SCENARIO_PACKAGE.

To update the user and/or climate data, copy the variables in the 
[LmServer - pipeline] section of config.lmserver.ini into the site.ini file and 
change as desired.  

----------------
SCENARIO_PACKAGE
----------------
must be one of the pre-defined options listed above

-----------------------------------------------------
DEFAULT_MODEL_SCENARIO / DEFAULT_PROJECTION_SCENARIOS
-----------------------------------------------------
must be pre-defined codes for the chosen scenario package listed above  

----------
DATASOURCE
----------

GBIF::
   If GBIF, a CSV file with the expected fields must be provided.  The files 
   gbif_merged.tar.gz or gbif_subset.tar.gz may be downloaded from 
   http://lifemapper.org/dl , and uncompressed into 
   /state/partition1/lmserver/data/species/.  If using the subset, the 
   variable OCCURRENCE_FILENAME must contain that filename in site.ini.  
   
IDIGBIO or BISON::
   When either of these options are chosen, the buildBoom process will 
   dynamically query the provider to build the archive.  With BISON, the first 
   query will build a list of taxa for which to query the BISON service.
   With IDIGBIO, a list of taxa with 'accepted GBIF taxon id', is queried.  
   This file, idig_gbifids.txt, is installed with the roll.
   
USER::
   Anything other than the GBIF, IDIGBIO, or BISON in DATASOURCE indicates 
   user-provided data, installed into /state/partition1/lmserver/data/species/.  
   Data and metadata files must have the same basename.  The Data file must be 
   in CSV format and the metadata file must be a python dictionary.  
   Data and metadata must conform to the requirements listed in 
   LmDbServer/tools/occurrence.meta.example 

**********
Algorithms
**********
One or more algorithms must be set in the DEFAULT_ALGORITHMS variable in the 
config file.  The algorithm must be designated by the code pre-populated in the 
database.  

Currently, the pipeline will use default parameters for all algorithms.  
Algorithms available are the AT&T version of Maxent, and the 12 
algorithms provided by openModeller::

    ATT_MAXENT    | Maximum Entropy (ATT Implementation)
    SVM           | SVM (Support Vector Machines)
    DG_GARP_BS    | GARP (single run) - DesktopGARP implementation
    AQUAMAPS      | AquaMaps (beta version) 
    RNDFOREST     | Random Forests
    GARP_BS       | GARP with Best Subsets - new openModeller implementation 
    ENFA          | Ecological-Niche Factor Analysis
    ENVSCORE      | Envelope Score
    GARP          | GARP (single run) - new openModeller implementation
    ENVDIST       | Environmental Distance
    BIOCLIM       | Bioclimatic Envelope Algorithm
    DG_GARP       | GARP (single run) - DesktopGARP implementation
    MAXENT        | Maximum Entropy (openModeller Implementation)
    CSMBS         | Climate Space Model - Broken-Stick Implementation
    ANN           | Artificial Neural Network
    
 
