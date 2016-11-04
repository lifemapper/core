
.. highlight:: rest

Install or Update a Lifemapper Server/Compute installation
==========================================================
.. contents::  

.. _Setup Development Environment : docs/developer/developEnv.rst

Introduction
------------
For systems with both the LmCompute and LmServer rolls installed, you will want 
to update the LmCompute roll and LmServer rpms (lifemapper-lmserver, rocks-lifemapper) 
without losing data.

(If update) Stop processes
--------------------------

#. **Stop the archivist** as lmwriter ::    

     % $PYTHON /opt/lifemapper/LmDbServer/pipeline/archivist.py stop

#. **Stop the jobMediator** as lmwriter::

     lmwriter$ $PYTHON /opt/lifemapper/LmCompute/tools/jobMediator.py stop

#. **Caution** If want to completely wipe out existing install::

   # wget https://raw.githubusercontent.com/pragmagrid/lifemapper-server/kutest/cleanRoll.sh -O cleanServerRoll.sh
   # wget https://raw.githubusercontent.com/pragmagrid/lifemapper-compute/kutest/cleanRoll.sh -O cleanComputeRoll.sh

Install both rolls on Frontend
------------------------------

#. **Download** new LmServer and LmCompute rolls to server, then validate 
   checksums::

   # cd /state/partition1/apps/
   # wget http://lifemapper.org/dl/lifemapper*.*
   # sha256sum lifemapper*.iso
   # cat lifemapper*.sha
   

Update existing (maintains FE data)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. You may remove source code rpms (lifemapper-lmserver and 
   lifemapper-compute) to avoid error messages about file conflicts in 
   shared code, but error messages about conflicting shared files from the 
   first install of the source code rpm may be safely ignored. 
#. In case the configuration rpm (rocks-lifemapper, rocks-lmcompute) versions 
   have not changed, remove rpms to ensure that configuration scripts are run.  
   If these rpms  are new, the larger version git tag will force the new 
   rpm to be installed, **but if the rpm versions have not changed**, you 
   must remove them to ensure that the installation scripts are run.::
      
   # rpm -el rocks-lifemapper rocks-lmcompute

New install (destroys data)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you do not need to save the existing data files and database records, 
run the cleanRoll scripts for each roll. 
   
#. **Add a new roll and rpms**, ensuring that old rpms/files are replaced::

   # rocks add roll lifemapper-server-6.2-0.x86_64.disk1.iso clean=1
   # rocks add roll lifemapper-compute-6.2-0.x86_64.disk1.iso clean=1
   
#. **Create distribution**::

   # rocks enable roll lifemapper-compute lifemapper-server
   # (cd /export/rocks/install; rocks create distro; yum clean all)

#. **Create and run LmServer/LmCompute scripts**::

    # rocks run roll lifemapper-server > add-server.sh
    # rocks run roll lifemapper-compute > add-compute.sh
    # bash add-server.sh > add-server.out 2>&1
    # bash add-compute.sh > add-compute.out 2>&1
    
Finish install
--------------

#. **Reboot front end** ::  

   # reboot
   
(OPT) To change defaults
------------------------

#. **To change defaults**, for either lifemapper-compute or lifemapper-server,
   such as DATASOURCE, ARCHIVE_USER, compute parameters,

   #. create the configuration file site.ini (in /opt/lifemapper/config/) 
      prior to reboot.  Two example files are present in that same directory.
      Variables to override for both rolls should be placed in the site.ini file.
      
   #. If you wish to change the SCENARIO_PACKAGE (and corresponding 
      DEFAULT_SCENARIO) variables for LmServer, you must do this after the 
      installation is complete (after reboot).

   #. If you updated the SCENARIO_PACKAGE 
   
      1. Create a [ LmCompute - environment ] section containing  
         the variable SCENARIO_PACKAGE_SEED with the same value

      2. Run the following to download data ::
   
         # rocks/bin/getClimateData

      3. Run the following to catalog metadata for LmServer::
   
         # rocks/bin/fillDB

      4. Run the following to convert and catalog data for LmCompute ::

         # /opt/lifemapper/rocks/bin/seedData

   #. If you ONLY updated the ARCHIVE_USER
   
      #. Run the following to catalog metadata for LmServer::
   
         # rocks/bin/fillDB
         

Install nodes from Frontend
---------------------------

#. **(Optional)** When updating an existing installation, remove unchanged 
   compute-node rpms manually to ensure that scripts are run.::  

      # rocks run host compute 'rpm -el rocks-lmcompute'
    
#. **Rebuild the compute nodes** ::  

   # rocks set host boot compute action=install
   # rocks run host compute reboot 

Add compute input layers to the Frontend
----------------------------------------

#. Seed the data for LmCompute on the frontend (if not done in optional step
   above) ::

   # /opt/lifemapper/rocks/bin/seedData

   
Look for Errors
---------------
   
#. **Check log files** After the frontend boots up, check the success of 
   initialization commands in log files in /tmp (these may complete up to 5
   minutes after reboot).  The post-99-lifemapper-lm*.log files contain all
   the output from all reinstall-reboot-triggered scripts and are created fresh 
   each time.  All other logfiles are in /state/partition1/lmscratch/log 
   and may be output appended to the end of an existing logfile (from previous 
   runs) and will be useful if the script must be re-run manually for testing.
#. **Clean compute nodes**  
   
LmCompute
~~~~~~~~~

#. Check LmCompute logfiles

   * /tmp/post-99-lifemapper-lmcompute.debug  (calls initLMcompute on reboot) 
   * initLMcompute.log 
   * installComputeCronJobs.log
   * seedData.log (seedData must be run manually by user after reboot)

LmServer
~~~~~~~~

#. Check LmServer logfiles

   * /tmp/post-99-lifemapper-lmserver.debug (calls initLM on reboot) 
   * initLM.log
   * installServerCronJobs.log
   * initDbserver.log (only if new db)
     
#. **Test database contents** ::  

   # export PGPASSWORD=`grep sdlapp /opt/lifemapper/rocks/etc/users | awk '{print $2}'`
   # psql -U sdlapp -d mal
   psql (9.1.22)
   Type "help" for help.
   mal=> select scenariocode, userid from scenario;

Change Data Defaults
--------------------

#. **Check default archive values (DATASOURCE, ARCHIVE_USER, OCCURRENCE_FILENAME ...)** :  

   * Look at values in /opt/lifemapper/config/config.lmserver.ini
   * Update values to be modified in /opt/lifemapper/config/site.ini
   * Override any of the following (or other) variables by adding them to 
     site.ini and downloading climate data if necessary.
   
     * Default ARCHIVE_USER is kubi.
     * Default OCCURRENCE_FILENAME is gbif_subset.txt.  If this is KU production
       installation, override this with the latest full data dump by downloading 
       the data from yeti into /share/lmserver/data/species/
     * Default species file of "Accepted" GBIF Taxon Ids for iDigBio occurrences
       is IDIG_FILENAME with a value of idig_gbifids.txt.  Download the file 
       from yeti into /share/lmserver/data/species.
     * Default SCENARIO_PACKAGE is 10min-past-present-future.  To change this, 
       override the variable SCENARIO_PACKAGE (for LmServer) and 
       SCENARIO_PACKAGE_SEED (for LmCompute).
     
       * identify options for DEFAULT_MODEL_SCENARIO and 
         DEFAULT_PROJECTION_SCENARIOS by looking at the metadata newly installed  
         in /share/lmserver/data/climate/<SCENARIO_PACKAGE>.csv
       * add the variables DEFAULT_MODEL_SCENARIO and 
         DEFAULT_PROJECTION_SCENARIOS in site.ini with appropriate values
         
then follow the instructions in **(OPT) To change defaults** above.
   
