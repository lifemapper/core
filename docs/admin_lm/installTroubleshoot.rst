
.. highlight:: rest

General coding standards and best practices for Lifemapper
==========================================================
.. contents::  

.. _Basic Lifemapper Testing Approach:  docs/developer/testingOverview.rst
.. _Testing suite construction:  docs/developer/testSuite.rst

*****
Rolls
*****

#. Cleaned old rolls
#. Manually removed lmwriter user/group/dirs
#. Reboot
#. Installed new rolls
#. Reboot


***********
Roll errors
***********

#. Fixed layers.db permissions
#. Fnstalled nodes
#. Fixed lmscratch permissions on nodes, (rocks run host compute "chgrp -R lmwriter /state/partition1/lmscratch”)
#. Look at reboot log on nodes 
   * Saved at /state/partition1/tmpdata/post-99-lifemapper-lmcompute.debug.compute-0-0

*************************
Code install Errors/fixes
*************************

#. Checked logfiles  errors in cron jobs:  lmserver_build_solr_index.log, bad import TEMP_PATH
#. Removed bad grim MF records in db
#. Removed bad grim MF files
#. Manually added matrixcolumns and grim MF jobs
#. Manually ran boominput on heuchera data for biotaphy user
#. Manually removed daboom mfprocesses from db (since we're running by hand)

**************
To-be-scripted
**************

#. Manually copied heuchera data to /share/lm/data/archive/biotaphy/

*****************************************
Process override (Normally MF will start)
*****************************************
#. Ran daboom on biotaphy 
