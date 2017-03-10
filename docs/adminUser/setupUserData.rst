
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