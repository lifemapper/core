# Install and Configure Lifemapper on Docker

## Containers

* Backend (PostgreSQL)
* Solr
* Flask
* Compute

## Python dependencies

* from pip - in requirements.txt
* lmpy (wheel file from lifemapper-server roll)

## System dependencies

All repos are at github.com

### Flask container only:

* mapserver 7.0.0 (including mapserver-bin, libmapserver2, python3-mapscript)
* webclient (lifemapper/viz-client repo)
* geotiff 
* mod-wsgi?
* lmdata-image (lifemapper/lifemapper-server repo, ./lmdata-image/src/prepSrc scripts to pull data) 

### Solr container only:

* solr (docker hub?)

### Backend container only:

* PostgreSQL 9.6 (docker hub)
* Postgis2
* psycopg2-binary (python pip install)
* lmdata-species (lifemapper/lifemapper-server repo, ./lmdata-species/src/prepSrc scripts to pull data)

### Backend and Compute containers:

* lmdata-env (put data in shared volume; lifemapper/lifemapper-server repo, ./lmdata-env/src/prepSrc scripts to pull data)


### All containers?:

* lmcompute (lifemapper/core repo, check prepSrc in lifemapper/lifemapper-server/src/lmserver)
* lmserver (lifemapper/core repo, check prepSrc in lifemapper/lifemapper-compute/src/lmcompute)
* lmtest (lifemapper/lmtest repo)
* specify-open-api-tools (specify/open_api_tools repo)

Python installs via pip

* Cython
* scipy>=1.7.2
* matplotlib>=3.5.0
* requests>=2.26.0

Package manager installs

* libgdal
* libproj

Source or binary installs

* cctools (http://ccl.cse.nd.edu/software/downloadfiles.php)


## Installation resources

Combine Environmental variables and values with 

* variables in lifemapper/core/LmServer/config/config.lmserver.ini 
  and values in lifemapper-server/src/version.mk
* variables in lifemapper/core/LmCompute/config/config.lmcompute.ini 
  and values in lifemapper-compute/src/version.mk

Process 

* lifemapper/lifemapper-server repo 
  * ./nodes/lifemapper-server-base.xml for directory and user setup
  * ./src/rocks-lifemapper/* scripts for post-install and configuration on all containers
  
* lifemapper/lifemapper-compute repo 
  * ./nodes/lifemapper-compute-base.xml for directory and user setup
  * ./src/rocks-lmcompute/initLMcompute.in for post-install and configuration on all Compute
  
## Misc 

Move helper shell scripts from roll repos lifemapper-server/src/rocks-lifemapper and 
lifemapper-compute/src/rocks-lmcompute into core/bin
