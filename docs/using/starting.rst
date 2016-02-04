
########
Overview
########

***************
Important notes
***************

The user lmwriter performs all processes associated with running a Lifemapper
instance.  Using the root user will result in read/write permissions on 
data and logfiles getting set incorrectly, and will cause errors in later 
operations.

The 'config' file referenced below is <APP_PATH>/config/config.ini.  Variables in
this file may be overridden by the user in <APP_PATH>/config/site.ini.  APP_PATH
is also present in the config file, and generally is configured as 
/opt/lifemapper.

****************
Data computation
****************
LmServer **Pipeline** assembles jobs for computation from input data and 
parameters.  Objects and related jobs are recorded in the database.

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

To start the pipeline as user lmwriter do::

    $ $PYTHON /opt/lifemapper/LmDbServer/pipeline/localpipeline.py

To Stop the pipeline (replace pragma with the datasource name configured for 
this instance, i.e. bison, idigbio)::

    $ touch /opt/lifemapper/log/pipeline.pragma.die
    
    
##########################
Modifying archive creation
##########################

The LmServer roll comes with default species data, environmental data, and
algorithm parameters.  These may all be modified.

************
Species Data
************
Species data may be pre-defined CSV file, such as the GBIF data dump, or 
a web service, such as the BISON service.  The DATASOURCE variable in the 
config file is a keyword which identifies the source and tells the pipeline 
how to process it.

Currently species data must be one of the pre-defined options, GBIF, BISON,
or IDIGBIO.

******************
Environmental Data
******************
Available environmental data is defined by the SCENARIO_PACKAGE variable in the 
config file. One scenarios for modeling (for current-day species data, this is 
usually observed environmental data), DEFAULT_MODEL_SCENARIO, and 
one or more for projecting, DEFAULT_PROJECTION_SCENARIOS, are set in the 
config file.

Currently environmental data must be one of the pre-defined options; 
SCENARIO_PACKAGE codes are: 30sec-present-future-SEA (Southeast Asia), 
30sec-present-future-CONUS (Continental United States), 5min-past-present-future 
and 10min-past-present-future (both global).  These data may be downloaded from 
svc.lifemapper.org/dl/ with filenames the code with extension tar.gz.  Metadata 
for each of these packages is included in the source code, and will be populated 
correctly for the configured code.


**********
Algorithms
**********
One or more algorithms must be set in the DEFAULT_ALGORITHMS variable in the 
config file.  The algorithm must be designated by the code in the database.  

Currently algorithms available are the AT&T version of Maxent, and the 12 
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
