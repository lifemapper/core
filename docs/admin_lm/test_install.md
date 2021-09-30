# Testing a new or updated Lifemapper installation

## Install

* Use installLifemapperSystem.rst
  * install lmcompute and lmserver on FE
  * set nodes to install and reboot for re-install
  * check logs, database contents for initial users, inputs, and makeflow
  
## Populate

  * AFTER node install, run matt_daemon to execute makeflows 
  	* intersect GRIM
  	* run boomer (process initial gbif subset data)
  	* additional makeflows created by boomer
  * AFTER boomer completes
  	* export gbif taxonomy from database 
  	* import gbif taxonomy to solr
  * check database contents for progress on occurrencesets, projections
  
  
## Running
  * submit jobs with Maxent and OM
  	
  
## Edge cases