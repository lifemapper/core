
.. highlight:: rest

Document and package user data inputs
=====================================
.. contents::  

.. _Setup Development Environment : docs/developer/developEnv.rst

Environmental Data
------------------
#. Create compressed tar file of raster environmental layers in Geotiff format
#. Create metadata for layers in the format demonstrated in 
   LmDbServer/tools/10min-past-present-future.v2.py
#. Create CSV file with hash value and relative pathname to every file
   using LmDbServer/tools/createClimateHashfile.py
#. Seed data on LmCompute by putting CSV file in /share/lm/data/layers 
   * This currently expects the package name (also CSV base filename) to be 
     the value for SCENARIO_PACKAGE_SEED in config/config.lmcompute.ini.  This
     must change to use a command line option.