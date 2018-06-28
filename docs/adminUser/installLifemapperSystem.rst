
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
   
#. (Obsolete?) Remove old elgis repository rpm (it will cause yum to fail 
   and pull old pgbouncer, postgresql, rpms from the roll.  TODO: update static 
   rpms in roll) ::
   
   # rpm -evl --nodeps elgis-release
   
#. (Optional) When updating an existing install, it should always be true that  
   the configuration rpms (rocks-lifemapper, rocks-lmcompute) have new version 
   numbers, matching the code rpms (lifemapper-lmserver or lifemapper-lmcompute).  
   As long as this is true, rpms will be replaced correctly.  If it is false, 
   the configuration rpms must be manually removed so that configuration scripts 
   will be run on install. If the above is true on a lifemapper-compute 
   installation, do the same thing for every node::
   
   # rpm -el rocks-lmcompute
      

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

    # (rocks run roll lifemapper-server > add-server.sh; \
       rocks run roll lifemapper-compute > add-compute.sh; \
       bash add-server.sh 2>&1 | tee add-server.out; \
       bash add-compute.sh 2>&1 | tee add-compute.out)

#. **IFF** installing compute roll first or alone, manually set the 
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

   # rocks run host compute "(hostname; chgrp -R lmwriter /state/partition1/lmscratch; chmod -R g+ws /state/partition1/lmscratch)"
   # rocks run host compute "(hostname; ls -lahtr /state/partition1/lmscratch)"
      
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
     * transformData.log (transformData must be run manually by user after reboot)

Configure for new scenario data
---------------------------
#. Download (if missing) and install test scenario data for **server**. 
     This downloads the data package, installs all into correct directories. 
     Script is installed by the rocks-lifemapper rpm 
     into /opt/lifemapper/rocks/bin::
    # getScenPackageForUser  <SCENARIO_PACKAGE>  <USERID>
    
#. Download (if missing) and install test scenario data for **compute**. 
     This downloads the data package, installs all into correct directories, 
     and converts to ASCII and MXE for use with Maxent SDM. Script is installed 
     by the rocks-lmcompute rpm into /opt/lifemapper/rocks/bin::
    # transformData <SCENARIO_PACKAGE>

	* Downloaded data package <SCENARIO_PACKAGE>.tar.gz contains: 
	    * <SCENARIO_PACKAGE>.py file containing metadata for scenarioPackage, 
	      scenarios and layers
	    * raster layers for scenarios
	        
	* Verified test packages include: 
	   * sax_layers_10min  (12 layers, worldclim,soil,etc, observed scenario only)
	   * 10min-past-present-future (20 layers, worldclim, observed, 2 past, 4 future 
	     scenarios)
	   * biotaphyCONUS (35 layers, worldclim, soils, landcover, 30 sec, CONUS,
	     observed scenario only)

#. Catalog test scenario data for **server**. Insert layers,
     scenarios, and scenario package in the database. Script is installed by the 
     rocks-lifemapper rpm into /opt/lifemapper/rocks/bin::
    # catalogScenPackageForUser  <SCENARIO_PACKAGE>  <USERID>
     
     

Configure for new boom job
---------------------------
#. Download and install test boom data. This downloads the
     data package, installs all into correct directories, and prints 
     instructions. Script is installed by the 
     rocks-lifemapper rpm into /opt/lifemapper/rocks/bin::
    # getBoomPackageForUser  <BOOM_DATA>  <USERID>

   * Downloaded data package <BOOM_DATA>.tar.gz contains: 
	    * <BOOM_DATA>.ini file containing names of datafiles in package
	    * <boom parameter>.ini file containing input data names and parameters
	    * species package containing data and metadata
	    * (optional) <species tree>.nex tree in nexus format
	    * (optional) <biogeographic hypotheses>.tar.gz containing one or more 
	      shapefiles in a directory named <biogeographic hypotheses>

   * Verified test packages include: 
	    * sax_boom_data : 
	        * saxifragales data (~2300sp) 
	        * sax_layers_10min current scenario, 
	        * tree
	        * biogeo hypotheses
	   * heuchera_boom_global_data:  
	        * heuchera data (64sp) 
	        * 10min-past-present-future
	        * tree
	        * biogeo hypotheses
	   * heuchera_boom_data 
	        * heuchera data (64sp) 
	        * biotaphyCONUS (30 sec/CONUS)
	        * tree
	        * biogeo hypotheses
               
#. Catalog BOOM data inputs in database for **server**. Create and insert 
    gridset, and optional shapegrid, matrices, tree and biogeographic hypotheses
    in the database and write the configuration file for this boom job.
    if do_walk is 1, a boom script will immediately process species data to 
    create Makeflows for boom job. Script is installed by the rocks-lifemapper 
    rpm into /opt/lifemapper/rocks/bin::
    # catalogBoomJob  <boom parameter>.ini  <do_walk>
   
   * Results of catalogBoomJob:
     * Verify scenario data exists for this user
     * Create gridset for Boom 
     * Optionally create shapegrid, matrices, tree, biogeographic hypotheses
     * Write BOOM config file, to be used as input to the boomer script. 
     * print to screen and logfile:
       * BOOM config filename
       * BOOM command 
       * Encoding command for biogeographic hypotheses (with parameters)
       * Encoding command for tree (with parameters)

Process species data to create Makeflows for boom job
------------------------------------------------------
#. BOOM data inputs to create and catalog in the database data objects and 
   makeflow scripts for a BOOM workflow.  Run python boom daemon (as lmwriter) 
   with output BOOM config file created by fillDB.  The fillDB script will print 
   the full filepath of the BOOM  config file it has created ::  
    [lmwriter]$ $PYTHON /opt/lifemapper/LmDbServer/boom/daboom.py --config_file=<BOOM_CONFIG_FILE>  start

Encode biogeographic hypotheses for MCPA calculations
------------------------------------------------------
#. Encode biogeographic hypotheses as lmwriter user with python script.  This
   may be done prior to BOOMing the data.  The fillDB script will print the 
   command with user and gridset parameters::
    [lmwriter]$ $PYTHON LmServer/tools/boomInputs.py  --user=<ARCHIVE_USER>  --gridset_name=<ARCHIVE_NAME>
     
Encode tree with species identifiers for MCPA calculations
-----------------------------------------------------------
#. Encode tree as lmwriter user with python script.  This must be done after 
   BOOMing the data because it uses species squids, generated by BOOM, to the 
   tree.  The fillDB script will print the command with user and tree parameters::
    [lmwriter]$ $PYTHON LmServer/tools/boomInputs.py  --user=<ARCHIVE_USER>  --tree_name=<TREE_NAME>
     
         
Clear user data
---------------
#. Delete all user data from database::
      borg=> SELECT * from lm_clearUserData(<username>)

#. Delete all user data from filesystem::
      # rm -rf /share/lm/data/archive/<username>

#. Delete computed user data (not input scenarios) from database::
      borg=> SELECT * from lm_clearComputedUserData(<username>)



Misc Data Info
--------------
#. Make sure there is an environmental data package (<SCEN_PKG>.tar.gz) 
   containing a metadata file (<SCEN_PKG>.py) and a CSV file containing 
   layer file hash values and relative filenames ((<SCEN_PKG>.csv) and 
   layer data files.  The tar.gz file should be uncompressed in the 
   /share/lm/data/layers directory, or present on the download directory
   of the Lifemapper website (lifemapper.org/dl).

#. Create a BOOM parameter file based on the template in 
   /opt/lifemapper/config/boomInit.sample.ini as data input to the 
   catalogBoomJob script

#. Either allow the makeflow produced by fillDB to be run automatically, 
   or run the boom daemon as described above. 
  
#. Data value/location requirements :  

   * to use a unique userId/archiveName combination.  
   * the SCENARIO_PACKAGE data must be installed in the ENV_DATA_PATH directory,
     this will be correct if using the getClimateData or getBoomPackage scripts
   * If the DATASOURCE is USER (anything except GBIF, IDIGBIO, or BISON),
    
     * the species data files USER_OCCURRENCE_DATA(.csv and .meta) must be 
       installed in the user space (/share/lm/data/archive/<userId>/).
     * Requirements for assembling occurrence data are at:  `Configure Archive Data`_

   * If the DATASOURCE is GBIF, with CSV file and known column definitions, the
     default OCCURRENCE_FILENAME is gbif_subset.txt.  If this is KU 
     production installation, override this in a config.site.ini file with the 
     latest full data dump by downloading the data from yeti 
     into /share/lmserver/data/species/
