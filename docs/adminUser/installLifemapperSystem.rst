
.. highlight:: rest

Install or Update a Lifemapper Server/Compute installation
==========================================================
.. contents::  

.. _Configure Archive Data : docs/adminUser/buildLifemapperData.rst

Current versions
----------------
#. **Download** new LmServer and LmCompute rolls to server, then validate 
   checksums.  Replace filename with latest roll names::

   # sha256sum -c lifemapper-*.sha

(If update) Stop processes
--------------------------

#. **Stop the daboom daemon** as lmwriter ::    

     lmwriter$ $PYTHON /opt/lifemapper/LmDbServer/boom/daboom.py stop

#. **Stop the mattDaemon** as lmwriter::

     lmwriter$ $PYTHON /opt/lifemapper/LmServer/tools/mattDaemon.py stop

#. **Caution** If want to **completely destroy** existing install, including
   deleting the database and clearing lm data from filesystem, run::

   # bash /opt/lifemapper/rocks/etc/clean-lm-server-roll.sh
   # bash /opt/lifemapper/rocks/etc/clean-lm-compute-roll.sh

Update existing install
~~~~~~~~~~~~~~~~~~~~~~~
#. Remove old rolls (without cleaning or removing individual rpms)::
   # rocks remove roll lifemapper-server lifemapper-compute
   
#. Remove old elgis repository rpm (it will cause yum to fail and pull old 
   pgbouncer, postgresql, rpms from the roll.  TODO: update static rpms in roll) ::
   # rpm -evl --quiet --nodeps elgis-release
   
#. (Optional) When updating an existing install, it should always be true that  
   the configuration rpms (rocks-lifemapper, rocks-lmcompute) have new version 
   numbers, matching the code rpms (lifemapper-lmserver or lifemapper-lmcompute).  
   As long as this is true, rpms will be replaced correctly.  If it is false, 
   the configuration rpms must be manually removed so that configuration scripts 
   will be run on install. If the above is true on a lifemapper-compute 
   installation, do the same thing for every node::
   # rpm -el rocks-lifemapper rocks-lmcompute
      

Install both rolls on Frontend
------------------------------

New install
~~~~~~~~~~~
If you do not need to save the existing data files and database records, 
run the cleanRoll scripts for each roll. 
   
#. **Add a new roll and rpms**, ensuring that old rpms/files are replaced.  
   Replace the following roll name with the latest version, identified
   at the top of this document::

   # rocks add roll lifemapper-server-*.iso clean=1
   # rocks add roll lifemapper-compute-*.iso clean=1
   
#. **Create distribution**::

   # rocks enable roll lifemapper-compute lifemapper-server
   # (cd /export/rocks/install; rocks create distro; yum clean all)

#. **Create and run LmServer/LmCompute scripts**::

    # rocks run roll lifemapper-server > add-server.sh
    # rocks run roll lifemapper-compute > add-compute.sh
    # bash add-server.sh 2>&1 | tee add-server.2017.11.20.2.out
    # bash add-compute.sh 2>&1 | tee add-compute.2017.11.20.2.out

#. **IF** installing compute roll first or alone, manually set the 
   LM_dbserver and LM_webserver attributes.  If this server will also
   host the web/db server, set the value=true otherwise, value=<ip or FQDN>::
   
    # rocks add host attr localhost LM_webserver value=true
    # rocks add host attr localhost LM_dbserver value=true

    
Finish install
--------------

#. **Reboot front end** ::  

   # reboot
   
Install nodes from Frontend
---------------------------

#. **Rebuild the compute nodes** ::  

   # rocks set host boot compute action=install
   # rocks run host compute reboot     

Install bugfixes
----------------
#. Compute Nodes - check/fix node group permissions on /state/partition1/lmscratch ::  
   # rocks run host compute "hostname; ls -lah /state/partition1/"
   # rocks run host compute "chgrp -R lmwriter /state/partition1/lmscratch"
   # rocks run host compute "chmod -R g+ws /state/partition1/lmscratch"
      
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
   
Check LmServer
~~~~~~~~~~~~~~
#. Check LmServer logfiles

   * /tmp/post-99-lifemapper-lmserver.debug (calls initLM on reboot) 
   * files in /state/partition1/lmscratch/log
     * initLM.log
     * installServerCronJobs.log
     * fillDB
     
#. Check database contents ::  

   # export PGPASSWORD=`grep admin /opt/lifemapper/rocks/etc/users | awk '{print $2}'`
   # psql -U admin -d borg
   psql (9.1.22)
   Type "help" for help.
   borg=> select scenariocode, userid from scenario;

Check LmCompute
~~~~~~~~~~~~~~~
#. Check LmCompute logfiles

   * /tmp/post-99-lifemapper-lmcompute.debug  (calls initLMcompute on reboot) 
   * files in /state/partition1/lmscratch/log
     * initLMcompute.log 
     * installComputeCronJobs.log
     * seedData.log (seedData must be run manually by user after reboot)

Start Archive Booming
~~~~~~~~~~~~~~~~~~~~~
#. BOOM manually for direct testing
   * Run boom daemon (as lmwriter) with output BOOM config file ::  
    [lmwriter]$ $PYTHON /opt/lifemapper/LmDbServer/boom/daboom.py \
                --config_file=<NEW_CONFIG_FILE>  \
                start

   * If needed, cleanup by deleting the makeflow record from the database and 
     file from the filesystem.
      borg=> SELECT * from mfprocess where metadata like '%GRIM%';

Configure for new/test data
---------------------------
#. Download and install test environmental data for both server and compute, 
   species data and BOOM parameter file for server.  Bash script getTestPackage 
   is in lmserver roll
   * There are 2 good test packages, named **test_sax** and **test_heuchera**
   * Run getTestPackage bash script with test package name.  This downloads
     data package, installs all into correct directories and sets permissions.::  
     # /opt/lifemapper/rocks/bin/getTestPackage <TEST_PACKAGE_NAME>

#. (ONLY if not using getTestPackage) Copy species data for server into user dataspace ::  
   # cp <SPECIES_DATA_FILES> /share/lm/data/archive/<user>/
           
#. (ONLY if not using getTestPackage) Download and install environmental data 
   for both server and compute. Bash script getClimateData is in lmserver roll.
   * Run getClimateData bash script with scen package name.  This downloads
     data package and sets permissions.::  
     # /opt/lifemapper/rocks/bin/getClimateData <SCEN_PACKAGE_NAME>

#. (ONLY if not using getTestPackage) Catalog BOOM archive parameters, including 
   environmental data, for Server
   * Run fillDB bash script (as root) with boom parameter file pointing to
     chosen environmental and species data. 
   * If you installed data with getTestPackage, the boom parameter file
     will be in the /state/partition1/lmscratch/temp directory
   * When running this way, the script will not create a makeflow record and file. ::  
     # /opt/lifemapper/rocks/bin/fillDB <BOOM_PARAM_FILE>
     
   * Results of fillDB: 
     * User data directory
       * create if needed
       * output a BOOM config file, to be used as input to the boomer script. 
       * output new shapegrid 
       * fix permissions
     * print BOOM config filename to the screen and to the output logfile.
     * (NOT in this case) insert a makeflow record and file to run the boomer script.  

#. Catalog environmental data for Compute: 
   * Run seedData (as root) with scen package name.  This builds files in  
     alternate data formats and creates/fills the LmCompute sqlite3 database 
     with file locations so data does not need to be pulled from the server for 
     computations. ::  
     # /opt/lifemapper/rocks/bin/seedData <SCEN_PACKAGE_NAME>
     
#. BOOM manually for direct testing
   * See instructions in **Start Archive Booming** above
         
Clear user data
---------------
#. Delete user data from database::
      borg=> SELECT * from lm_clearUserData(<username>)

#. Delete user data from filesystem::
      # rm -rf /share/lm/data/archive/<username>


Misc Data Info
--------------
#. Make sure there is an environmental data package (<SCEN_PKG>.tar.gz) 
   containing a metadata file (<SCEN_PKG>.py) and a CSV file containing 
   layer file hash values and relative filenames ((<SCEN_PKG>.csv) and 
   layer data files.  The tar.gz file should be uncompressed in the 
   /share/lm/data/layers directory, or present on the download directory
   of the Lifemapper website (lifemapper.org/dl).

#. Create a BOOM parameter file based on the template in 
   /opt/lifemapper/config/boomInit.sample.ini as "alternate" data input to the 
   fillDB script

#. Either allow the makeflow produced by fillDB to be run automatically, 
   or run the boom daemon as described above. 
  
#. Data value/location requirements :  

   * to use a unique userId/archiveName combination.  
   * the SCENARIO_PACKAGE data must be installed in the ENV_DATA_PATH directory,
     this will be correct if using the getClimateData script
   * If the DATASOURCE is USER (anything except GBIF, IDIGBIO, or BISON),
    
     * the species data files USER_OCCURRENCE_DATA(.csv and .meta) must be 
       installed in the user space (/share/lm/data/archive/<userId>/).
     * Requirements for assembling occurrence data are at:  `Configure Archive Data`_

   * If the DATASOURCE is GBIF, with CSV file and known column definitions, the
     default OCCURRENCE_FILENAME is gbif_subset.txt.  If this is KU 
     production installation, override this with the latest full data dump 
     by downloading the data from yeti into /share/lmserver/data/species/
