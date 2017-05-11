
.. highlight:: rest

Install or Update a Lifemapper Server/Compute installation
==========================================================
.. contents::  

.. _Setup Development Environment : docs/developer/developEnv.rst
.. _Setup User Data : docs/adminUser/setupUserData.rst

Current versions
----------------
Download the current Lifemapper roll files and shasums:

* lifemapper-compute-<yyyy.mm.dd>-0.x86_64.iso (and .sha)
* lifemapper-server-<yyyy.mm.dd>-0.x86_64.iso

#. **Download** new LmServer and LmCompute rolls to server, then validate 
   checksums.  Replace filename with latest roll names::

   # cd /state/partition1/apps/
   # wget http://lifemapper.org/dl/lifemapper-compute-2017.05.04-0.x86_64.disk1.iso
   # wget http://lifemapper.org/dl/lifemapper-compute-2017.05.04-0.x86_64.disk1.sha
   # wget http://lifemapper.org/dl/lifemapper-server-2017.05.04-0.x86_64.disk1.iso
   # wget http://lifemapper.org/dl/lifemapper-server-2017.05.04-0.x86_64.disk1.sha
   # sha256sum -c lifemapper-*.sha

(If update) Stop processes
--------------------------

#. **Stop the archivist** as lmwriter ::    

     % $PYTHON /opt/lifemapper/LmDbServer/pipeline/archivist.py stop

#. **Stop the jobMediator** as lmwriter::

     lmwriter$ $PYTHON /opt/lifemapper/LmCompute/tools/jobMediator.py stop

#. **Caution** If want to **completely destroy** existing install, run::

   # bash /opt/lifemapper/rocks/etc/clean-lm-server-roll.sh
   # bash /opt/lifemapper/rocks/etc/clean-lm-compute-roll.sh

Install both rolls on Frontend
------------------------------

Update existing install
~~~~~~~~~~~~~~~~~~~~~~~

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

New install
~~~~~~~~~~~
If you do not need to save the existing data files and database records, 
run the cleanRoll scripts for each roll. 
   
#. **Add a new roll and rpms**, ensuring that old rpms/files are replaced.  
   Replace the following roll name with the latest version, identified
   at the top of this document::

   # rocks add roll lifemapper-server-6.2-0.x86_64.disk1.iso clean=1
   # rocks add roll lifemapper-compute-6.2-0.x86_64.disk1.iso clean=1
   
#. **Create distribution**::

   # rocks enable roll lifemapper-compute lifemapper-server
   # (cd /export/rocks/install; rocks create distro; yum clean all)

#. **Create and run LmServer/LmCompute scripts**::

    # rocks run roll lifemapper-server > add-server.sh
    # rocks run roll lifemapper-compute > add-compute.sh
    # bash add-server.sh 2>&1 | tee add-server.out
    # bash add-compute.sh 2>&1 | tee add-compute.out
    
Finish install
--------------

#. **Reboot front end** ::  

   # reboot
   
Install nodes from Frontend
---------------------------

#. **(Optional)** When updating an existing installation, remove unchanged 
   compute-node configuration rpms manually to ensure that scripts are run.::  

      # rocks run host compute 'rpm -el rocks-lmcompute'
    
#. **Rebuild the compute nodes** ::  

   # rocks set host boot compute action=install
   # rocks run host compute reboot 

   
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

   # export PGPASSWORD=`grep admin /opt/lifemapper/rocks/etc/users | awk '{print $2}'`
   # psql -U admin -d borg
   psql (9.1.22)
   Type "help" for help.
   mal=> select scenariocode, userid from scenario;

Populate archive
----------------
#. Download new environmental data from Yeti.  Requirements for assembling 
   environmental data are at:  `Setup User Data`_

   * For now, update config.site.ini with SCENARIO_PACKAGE corresponding to the 
     basename of a tar.gz file present in the yeti download directory.  The 
     compressed file must contain scenario metadata with the SCENARIO_PACKAGE 
     basename and .meta file extension and layer data.  (TODO: Change to accept 
     an argument) Then call::
     
     # rocks/bin/getClimateData

#. Populate the database with inputs for the default archive.  This runs 
   LmDbServer/boom/boominput.py with no arguments::

     # rocks/bin/fillDB
   
   * The boominput script will either accept a boom initialization configuration  
     file (example in LmServer/boom/boomInit.sample.ini) or pick up default 
     arguments from config.lmserver.ini and config.site.ini.

   * The configuration will find either:
   
     * SCENARIO_PACKAGE for scenario creation. SCENARIO_PACKAGE indicating a 
       file ENV_DATA_PATH/SCENARIO_PACKAGE.py describing and pointing to local 
       data.
     * or SCENARIO_PACKAGE_MODEL_SCENARIO and 
       SCENARIO_PACKAGE_PROJECTION_SCENARIOS, with codes for scenarios that 
       are already described in the database.
       
   * The boominput script will:
    
     * assemble all of the metadata and populate the database with inputs for a 
       BOOM process.  
     * build and write a shapegrid for a "Global PAM"
     * write a configuration file to the user data space with all of the 
       designated or calculated metadata for the BOOM process
       
   * Additional values will be pulled from the scenario package metadata 
     (<SCENARIO_PACKAGE>.py) file included in <SCENARIO_PACKAGE>.tar.gz.

   * Values for these data and this archive will be written to a new config 
     file named <SCENARIO_PACKAGE.ini> and placed in the user's (PUBLIC_USER
     or ARCHIVE_USER) data space (/share/lm/data/archive/user/)

#. Convert and catalog data for LmCompute.  The script uses the  
   SCENARIO_PACKAGE_SEED value from config.lmserver.ini, so override it 
   in config.site.ini if you have added new data. ::

   # /opt/lifemapper/rocks/bin/seedData

#. Data value/location requirements :  

   * to use a unique userId/archiveName combination.  
   * the SCENARIO_PACKAGE data must be installed in the ENV_DATA_PATH directory,
     this will be correct if using the getClimateData script
   * If the DATASOURCE is USER (anything except GBIF, IDIGBIO, or BISON),
    
     * the species data files USER_OCCURRENCE_DATA(.csv and .meta) must be 
       installed in the user space (/share/lm/data/archive/<userId>/).
     * Requirements for assembling occurrence data are at:  `Setup User Data`_

   * If the DATASOURCE is GBIF, with CSV file and known column definitions, the
     default OCCURRENCE_FILENAME is gbif_subset.txt.  If this is KU 
     production installation, override this with the latest full data dump 
     by downloading the data from yeti into /share/lmserver/data/species/
