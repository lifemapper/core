
.. hightlight:: rest

Updating an existing Lifemapper Compute installation
====================================================
.. contents::  

Introduction
------------
For systems with both the LmCompute and LmServer rolls installed, you will want 
to update the LmCompute roll and LmServer rpms (lifemapper-lmserver, rocks-lifemapper) 
without losing data.

.. contents::  

Stop processes
--------------

#. **Stop the archivist** as lmwriter ::    

     % $PYTHON /opt/lifemapper/LmDbServer/pipeline/archivist.py stop

   **TODO:** Move to command **lm stop archivist** 
     
#. **Stop the jobMediator** as lmwriter::

     lmwriter$ $PYTHON /opt/lifemapper/LmCompute/tools/jobMediator.py stop

   **TODO:** Move to command **lm stop jobs** 

Update everything
-----------------

#. **Copy new LmServer and LmCompute rolls to server**, for example::

   # scp lifemapper-compute-6.2-0.x86_64.disk1.iso  server.lifemapper.org:
   # scp lifemapper-server-6.2-0.x86_64.disk1.iso server.lifemapper.org:

#. **Add a new roll and rpms**, ensuring that old rpms/files are replaced::

   # rocks add roll lifemapper-server-6.2-0.x86_64.disk1.iso clean=1
   # rocks add roll lifemapper-compute-6.2-0.x86_64.disk1.iso clean=1
   
#. **Remove some rpms manually** 
   
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

#. **TEMP: Updating YETI** Create the Borg database prior to reboot so that 
   scripts can run on that db as well ::  
   
   #. export PGPASSWORD=`grep admin /opt/lifemapper/rocks/etc/users | awk '{print $2}'`
   #. psql -U admin -d template1 --file=LmDbServer/dbsetup/createBorg.sql
   #. psql -U admin -d borg --file=LmDbServer/dbsetup/createBorgTypeViews.sql
   #. psql -U admin -d borg --file=LmDbServer/dbsetup/createBorgFunctions.sql
   #. psql -U admin -d borg --file=LmDbServer/dbsetup/createBorgLayerFunctions.sql
    
#. **Reboot front end** ::  

   # reboot
   
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
   
#. **Remove some compute-node rpms manually** 
   
   #. Do this just in case the rpm versions have not changed, to ensure that
      scripts are run.::  

      # rocks run host compute 'rpm -el lifemapper-lmcompute rocks-lmcompute'

#. **Rebuild the compute nodes** ::  

   # rocks set host boot compute action=install
   # rocks run host compute reboot 

#. **Populate database (if new db or new BORG)** ::  

   # rocks/bin/fillDB

#. **Test database population** ::  

   # export PGPASSWORD=`grep sdlapp /opt/lifemapper/rocks/etc/users | awk '{print $2}'`
   # psql -U sdlapp -d mal
   # select scenariocode, userid from scenario;

