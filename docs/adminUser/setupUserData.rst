
.. highlight:: rest

Document and package user data inputs
=====================================
.. contents::  


Environmental Data
------------------
#. Create compressed tar file named SCENARIO_PACKAGE.tar.gz where 
   SCENARIO_PACKAGE is named in a configuration file.  The compressed file 
   contains:
   
   * raster environmental layers in Geotiff format
   * <SCENARIO_PACKAGE>.py  describing the layers and scenarios (layersets) in 
     in the package.  
   * Sample metadata is in LmDbServer/tools/10min-past-present-future.v2.py
   * <SCENARIO_PACKAGE>.csv with hash value and the relative pathname to every 
     file.  This file may be created with the script
     LmDbServer/tools/createClimateHashfile.py

#. Seed data on LmCompute by putting CSV file in /share/lm/data/layers 
   * NOTE: This currently expects the package name (also CSV base filename) to be 
     the value for SCENARIO_PACKAGE_SEED in config/config.lmcompute.ini.  This
     must change to use a command line option.
     
#. Place publicly available data in the yeti download directory for installation. 

     
Species Data
------------
#. Create compressed tar file named <USER_OCCURRENCE_DATA>.tar.gz where 
   USER_OCCURRENCE_DATA is named in a configuration file. The compressed
   file contains:
   
   * <USER_OCCURRENCE_DATA>.csv of species records in CSV format, with the 
     first line containing field names
   * <USER_OCCURRENCE_DATA>.meta describing the types and roles of each
     field name.  
   * sample occurrence metadata is in LmServer/config/occurrence.sample.meta
     
