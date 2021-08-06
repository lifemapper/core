
.. highlight:: rest

Boom Testing
================
.. contents::  


Install test boom data
----------------------

All bash and Python scripts assume you are in directory /opt/lifemapper

* To get package of boom parameters and data (from yeti download) and put it 
  all in the correct places::  

   rocks/bin/getBoomPackageForUser heuchera_boom_global_10min  someuser

* To transform data layers (already in /share/lm/data/layers) into mxe 
  files::
    
   rocks/bin/transformData heuchera_boom_global_10min

* Woof: create archive objects and makeflows.  This uses params file to 
  get occ and scenario metadata, and write config (ini) file for booming::

   rocks/bin/initWorkflow /share/lm/data/archive/someuser/heuchera_boom_global_10min.params

* Woof public archive:: 

   rocks/bin/initWorkflow /opt/lifemapper/config/boom.public.params
   
* Optional cleanup before woofing, includes database, filesystem, and Solr, 
  for makeflows, single-species and multi-species.  Except for 'clear user' this
  does not include non-computed layers, trees, or scenarios:

  * Clear user (clears ALL user data in db, filesystem, Solr).  Arg obsolete_date must future, mjd format)::
  
    $PYTHON /opt/lifemapper/LmDbServer/tools/janitor.py --user=someuser --obsolete_date=100000

  * Delete obsolete data for user::
  
    $PYTHON /opt/lifemapper/LmDbServer/tools/janitor.py --user=someuser --obsolete_date=58600

  * Delete obsolete gridset ::
  
    $PYTHON /opt/lifemapper/LmDbServer/tools/janitor.py --gridsetid=45


* All other processes are initiated by makeflows created by woof.
   
Post boom request using test data
---------------------------------

* Override the PUBLIC_USER in /opt/lifemapper/config/config.site.ini with
  the user who owns the Scenarios you want to use (otherwise it's private data).
  PUBLIC_USER is in the to the [LmServer - environment] section.
  
* Post an ini file by ::
   rocks/bin/fillDB /state/partition1/lmscratch/temp/<anon_user_params>.ini
  
