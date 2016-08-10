
.. hightlight:: rest

Install or Update a Lifemapper Server/Compute installation
==========================================================
.. contents::  

Introduction
------------
For systems with both the LmCompute and LmServer rolls installed, you will want 
to update the LmCompute roll and LmServer rpms (lifemapper-lmserver, rocks-lifemapper) 
without losing data.

(If update) Stop processes
--------------------------

#. **Stop the archivist** as lmwriter ::    

     % $PYTHON /opt/lifemapper/LmDbServer/pipeline/archivist.py stop

   **TODO:** Move to command **lm stop archivist** 
     
#. **Stop the jobMediator** as lmwriter::

     lmwriter$ $PYTHON /opt/lifemapper/LmCompute/tools/jobMediator.py stop

   **TODO:** Move to command **lm stop jobs** 

Install both rolls on Frontend
------------------------------

#. **Copy new LmServer and LmCompute rolls to server**, for example::

   # scp lifemapper-compute-6.2-0.x86_64.disk1.iso  server.lifemapper.org:
   # scp lifemapper-server-6.2-0.x86_64.disk1.iso server.lifemapper.org:

#. **Add a new roll and rpms**, ensuring that old rpms/files are replaced::

   # rocks add roll lifemapper-server-6.2-0.x86_64.disk1.iso clean=1
   # rocks add roll lifemapper-compute-6.2-0.x86_64.disk1.iso clean=1
   
#. **(If update) Remove some rpms manually** 
   
   #. Do this just in case the rpm versions have not changed, to ensure that
      scripts are run.  If the configuration (rocks-lifemapper, rocks-lmcompute) 
      rpms are new, the larger version git tag will force the new rpm to be 
      installed, **but if the rpm versions have not changed**, you must remove 
      them to ensure that the installation scripts are run.  Even if the source 
      code rpms (lifemapper-lmserver and lifemapper-compute) have changed, 
      you can remove them to avoid error messages about file conflicts in 
      shared code.::  

      # rpm -el rocks-lifemapper rocks-lmcompute

#. **Create distribution**::

   # rocks enable roll lifemapper-compute lifemapper-server
   # (cd /export/rocks/install; rocks create distro; yum clean all)

#. **Create and run LmServer/LmCompute scripts**::

   # rocks run roll lifemapper-server > add-server.sh 
   # rocks run roll lifemapper-compute > add-compute.sh 
   # bash add-server.sh > add-server.out 2>&1
   # bash add-compute.sh > add-compute.out 2>&1
   
   or 
   
   # (rocks run roll lifemapper-server > add-server.sh; 
      rocks run roll lifemapper-compute > add-compute.sh;
      bash add-server.sh > add-server.out 2>&1;
      bash add-compute.sh > add-compute.out 2>&1)
    
#. **Check problem areas**:

   * Symlink from /share/lmserver/log to /opt/lifemapper/log
   * Contents of /var/lib/pgsql/9.1/data - if this directory exists and is 
     populated when doing a clean install, delete it

#. **Reboot front end** ::  

   # reboot
   

Look for LmServer Errors
------------------------

#. **FIXME** (Added to lm-species-data rpm) Install the default dataset 
   (gbif_subset) for the default user (GBIF)::
   
   # cd /share/lmserver/data/species/
   # wget http://yeti.lifemapper.org/dl/gbif_subset.tar.gz
   # tar -xzvf gbif_subset.tar.gz 
   # chown root:lmwriter gbif_subset.txt
   # rm -f gbif_subset.tar.gz
   
#. **Check log files** After the frontend boots up, check the success of 
   initialization commands in log files in /tmp (these may complete up to 5
   minutes after reboot).  The post-99-lifemapper-lm*.log files contain all
   the output from all reinstall-reboot-triggered scripts and are created fresh 
   each time.  All other logfiles have output appended to the end of an existing 
   logfile (from previous runs) and will be useful if the script must be re-run
   manually for testing:
  * LmServer logfiles:
     * post-99-lifemapper-lmserver.debug (calls initLM on reboot) 
     * initLM.log
     * installServerCronJobs.log
     * initDbserver.log (only if new db)
  * LmCompute logfiles:
     * post-99-lifemapper-lmcompute.debug  (calls initLMcompute on reboot) 
     * initLMcompute.log 
     * installComputeCronJobs.log
     * seedData.log
     
#. **Check default archive values (DATASOURCE, ARCHIVE_USER, OCCURRENCE_FILENAME ...)** :  

   * Look at values in /opt/lifemapper/config/config.lmserver.ini
   * Update values to be modified in /opt/lifemapper/config/site.ini
   * Override any of the following (or other)variables by adding them to site.ini
     * Default ARCHIVE_USER is kubi.
     * Default OCCURRENCE_FILENAME is gbif_subset.txt.  If this is KU production
       installation, override this with the latest full data dump by downloading 
       the data from yeti into /share/lmserver/data/species/
     * Default SCENARIO_PACKAGE is 10min-past-present-future.  To change this, 
       override the variable SCENARIO_PACKAGE in site.ini, then 
       * run `/opt/lifemapper/rocks/bin/updateArchiveInput` to download and 
         install the data (log output will be in /tmp/updateArchiveInput.log):
       * identify options for DEFAULT_MODEL_SCENARIO and 
         DEFAULT_PROJECTION_SCENARIOS by looking at the metadata newly installed  
         in /share/lmserver/data/climate/<SCENARIO_PACKAGE>.csv
       * add the variables DEFAULT_MODEL_SCENARIO and 
         DEFAULT_PROJECTION_SCENARIOS in site.ini with appropriate values
   * If you have modified ARCHIVE_USER or SCENARIOS, run the following (log 
     output will be in /tmp/fillDB.log):: 
       # /opt/lifemapper/rocks/bin/fillDB

#. **Test database contents** ::  

   # export PGPASSWORD=`grep sdlapp /opt/lifemapper/rocks/etc/users | awk '{print $2}'`
   # psql -U sdlapp -d mal
   psql (9.1.22)
   Type "help" for help.
   mal=> select scenariocode, userid from scenario;

   
Install nodes from Frontend
---------------------------

#. **(If update) Remove some compute-node rpms manually** 
   
   #. Do this just in case the rpm versions have not changed, to ensure that
      scripts are run.::  

      # rocks run host compute 'rpm -el lifemapper-lmcompute rocks-lmcompute'

#. **(If new install) Add compute nodes to cluster**
 
Follow insert-ethers procedure 
   
#. **Rebuild the compute nodes** ::  

   # rocks set host boot compute action=install
   # rocks run host compute reboot 
