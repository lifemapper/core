
.. highlight:: rest

Populate Lifemapper for a BOOM job
==================================
.. contents::  


Configure for new scenario data
-------------------------------
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
     * biotaphyCONUS12 (12 layers, worldclim, soils, landcover, 30 sec, CONUS,
       observed scenario only)
     * biotaphyNA (35 layers, worldclim, soils, landcover, 30 sec, NA,
       observed scenario only)
     * biotaphyNA12 (12 layers, worldclim, soils, landcover, 30 sec, NA,
       observed scenario only)

#. Catalog test scenario data for **server**. Insert layers,
     scenarios, and scenario package in the database. Script is installed by the 
     rocks-lifemapper rpm into /opt/lifemapper/rocks/bin::
    # catalogScenPackageForUser  <SCENARIO_PACKAGE>  <USERID>
     
     

Configure for new boom job
--------------------------
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
      * heuchera_boom_na_10min (smallest):  
           * heuchera data (64sp) 
           * 10min-past-present-future
           * tree
           * biogeo hypotheses
      * heuchera_boom_global_10min (smallest):  
           * heuchera data (64sp) 
           * 10min-past-present-future
           * tree
           * biogeo hypotheses
       * sax_boom_global_10min: 
           * saxifragales data (~2300sp) 
           * sax_layers_10min current scenario, 
           * tree
           * biogeo hypotheses

   * UN-Verified test packages include: 
       * sax_boom_conus_30sec
       	   * saxifragales data (~2300sp) 
       	   * biotaphy12conus
           * tree
           * biogeo hypotheses
               
#. Catalog BOOM data inputs in database for **server**. Create and insert 
    gridset, and optional shapegrid, matrices, tree and biogeographic hypotheses
    in the database and write the configuration file for this boom job.
    if init_makeflow is 1, a boom script will immediately process species data to 
    create Makeflows for boom job. Script is installed by the rocks-lifemapper 
    rpm into /opt/lifemapper/rocks/bin::
    # initWorkflow  <boom parameter>.ini  <init_makeflow>
   
   * Results of initWorkflow:
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
-----------------------------------------------------
#. BOOM data inputs to create and catalog in the database data objects and 
   makeflow scripts for a BOOM workflow.  Run python boom daemon (as lmwriter) 
   with output BOOM config file created by fillDB.  The fillDB script will print 
   the full filepath of the BOOM  config file it has created ::  
    [lmwriter]$ $PYTHON /opt/lifemapper/LmDbServer/boom/daboom.py --config_file=<BOOM_CONFIG_FILE>  start

Encode biogeographic hypotheses for MCPA calculations
-----------------------------------------------------
#. Encode biogeographic hypotheses as lmwriter user with python script.  This
   may be done prior to BOOMing the data.  The fillDB script will print the 
   command with user and gridset parameters::
    [lmwriter]$ $PYTHON LmServer/tools/boomInputs.py  --user=<ARCHIVE_USER>  --gridset_name=<ARCHIVE_NAME>
     
Encode tree with species identifiers for MCPA calculations
----------------------------------------------------------
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
   initWorkflow script

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
