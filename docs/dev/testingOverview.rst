
.. highlight:: rest

Basic Lifemapper BOOM Testing Approach
======================================
.. contents::  

.. _Lifemapper Install : docs/adminUser/installLifemapperSystem.rst
.. _Development Environment Setup: docs/developer/developEnv.rst
.. _Testing Scope and Steps:  docs/developer/testSuite.rst
.. _Coding Standards and Best Practices:  docs/developer/lmStandards.rst

***********
Preparation
***********
#. Research standards and best practices for different testing strategies

   #. Unit tests
   #. Run time tests
   #. Ecosystem tests
   #. Run level tests
   
#. Determine how or if the proposed Lifemapper testing module, LmTest, needs to 
   be modified to support best practices.  Test some, propose several 
   alternatives for unit testing, automated tests, and more.
   
   #. Document findings and proposals in a file in the docs/developer/ 
      directory, with links to supporting information.  
   #. We will discuss options at a follow-up project meeting 
   #. We will decide on a course of action at that meeting or shortly after.
   
#. Document overall strategy chosen, how it will work, organization of files,
   and including links to supporting information in a README.rst file at the 
   top level of the LmTest module prior to writing test code.  This document
   will be updated as the testing suite matures.

************************
Initial testing coverage
************************
To avoid testing soon-to-be obsolete code, test the re-factored code which 
integrates single- and multi-species data and computations.  Because the code 
is in process, some sections will not be ready for testing and may be skipped 
over temporarily.  

#. More detailed instructions covering the scope of tests and expected steps 
   are at `Testing Scope and Steps`_
#. Lifemapper standards are at `Coding Standards and Best Practices`_

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
  
  
