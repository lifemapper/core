
.. highlight:: rest

Basic Lifemapper BOOM Testing Approach
======================================
.. contents::  

.. _Lifemapper Install : docs/adminUser/installLifemapperSystem.rst
.. _Development Environment Setup: docs/developer/developEnv.rst
.. _Testing Scope and Steps:  docs/developer/testSuite.rst

************************
Initial testing coverage
************************
To avoid testing soon-to-be obsolete code, test the re-factored code which 
integrates single- and multi-species data and computations.  Because the code 
is in process, some sections will not be ready for testing and may be skipped 
over temporarily.  More detailed instructions covering the scope of tests
and expected steps are at `Testing Scope and Steps`_

************
Testing data
************
All test data should be existing data packages or new packages prepared along 
with the tests.  It should be either installed with LM or the test itself 
should retrieve it. 

**************
Testing levels
**************
Lifemapper Testing will be done at the following levels (bottom up).  The 
breadth of each level should be tested before moving to the next level.

#. Object:  Start at the LmServer.sdm and LmServer.legion modules (not the base 
   objects that they inherit from).  
#. Database: tests should script inserting and retrieving data objects from the 
   database with the LmServer.db.scribe and LmServer.db.borgscribe
#. Web services: For each of the following, tests should use test data 

   #. Going around CherryPy
   #. Through HTTP and CherryPy
   #. Through LmClient
   #. Through QGIS

****************
Order of testing
****************
At each level, test LM in the following order.  Those created by:

#. Initialization (SDM and Legion)
#. Archivist
#. Input preparation for Compute
#. Output retrieval from Compute
#. Subsetting of Global PAM

*******************
Testing environment
*******************
Code will be tested on a virtual installation of Lifemapper on Rocks using the 
most recent Lifemapper rolls. To ensure the most recent code is tested, the 
Lifemapper code of the installation will be replaced with a local Lifemapper 
repository checked out from Github.  Testing code should be written on a 
development machine, checked into Github, then checked out onto the testing VM 
for testing at the command line.  Full instructions for each step are available 
at:

* `Lifemapper Install`_ 
* `Development Environment Setup`_
  
  
