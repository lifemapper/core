# Install and Configure Lifemapper on Docker

## Containers

* Backend (PostgreSQL server)
* Compute
* NGinx (PostgreSQL client)
* System test (lmtest and specify-openapi-tools)
* Solr
* Flask

## Environment variables 
* from [lifemapper-server] (https://github.com/lifemapper/lifemapper-server/blob/main/src/version.mk)
* from [lifemapper-compute] (https://github.com/lifemapper/lifemapper-compute/blob/main/src/version.mk)

## System dependencies

### Flask container only:

* mapserver 7.0.0 (including mapserver-bin, libmapserver2, python3-mapscript)
* webclient (lifemapper/viz-client repo)
* mod-wsgi ?
* lmdata-image [lifemapper-server] (https://github.com/lifemapper/lifemapper-server/blob/main/src/lmdata-image/prepData.sh.in) scripts and Environment variables above to pull data
* geotiff (package manager)


### Solr container only:

* solr (docker hub?)

### Backend container only:

* PostgreSQL 9.6 (docker hub)
* Postgis2
* psycopg2-binary (python pip install)
* lmdata-species [lifemapper-server] (https://github.com/lifemapper/lifemapper-server/blob/main/src/lmdata-species/prepData.sh.in) scripts and Environment variables above to pull data

### Backend and Compute containers:

* lmdata-env [lifemapper-server] (https://github.com/lifemapper/lifemapper-server/blob/main/src/lmdata-env/prepData.sh.in) scripts and Environment variables above to pull data. Put data in shared volume;
* [lmpy] (https://github.com/lifemapper/lmpy)
* [biotaphypy] (https://github.com/biotaphy/BiotaPhyPy)
* requests 
* scipy (includes numpy)
* matplotlib
* idigbio
* dendropy
* libgdal26, libgdal-dev (package manager)
* gdal
* libproj / (apt proj-bin, proj-data) (package manager)



### Testing container:
* [lmtest] (https://github.com/lifemapper/lmtest/) 
* specify-open-api-tools (specify/open_api_tools repo)


Source or binary installs

* [cctools] (http://ccl.cse.nd.edu/software/downloadfiles.php)


## Installation resources

Combine Environmental variables and values with 

* variables in lifemapper/core/LmServer/config/config.lmserver.ini 
  and values in lifemapper-server/src/version.mk
* variables in lifemapper/core/LmCompute/config/config.lmcompute.ini 
  and values in lifemapper-compute/src/version.mk

### Process 

* XML files with [Backend post-install instructions] (https://github.com/lifemapper/lifemapper-server/tree/main/nodes) for directory and user setup
* [Backend Post-install and config script] (https://github.com/lifemapper/lifemapper-server/blob/main/src/rocks-lifemapper/initLM.in)  

* XML files with [Compute post-install instructions] (https://github.com/lifemapper/lifemapper-compute/tree/main/nodes) for directory and user setup
* [Compute Post-install and config script] (https://github.com/lifemapper/lifemapper-compute/blob/main/src/rocks-lmcompute/initLMcompute.in) 
  
## Misc 

Move helper shell scripts from roll repos lifemapper-server/src/rocks-lifemapper and 
lifemapper-compute/src/rocks-lmcompute into core/bin
