
.. highlight:: rest

Configure a Lifemapper Server/Compute installation
==================================================
.. contents::  

.. _Configure Archive Data : docs/adminUser/configureLifemapper.rst

Compute
-------
#. Seed the sqlite database with data to be used for the archive BOOM.  The
   scenario package should be present, or available from the Yeti download
   directory::

   # /opt/lifemapper/rocks/bin/fillDB <SCENARIO_PACKAGE>


Server
------

#. Create or download a ini file (BOOM init) with the desired archive 
   configuration values.  An example of this can be found at 
   LmServer/config/boomInit.sample.ini
   
#. Populate the database with SCENARIO_PACKAGE metadata and create an archive 
   configuration (BOOM config) file from the BOOM init file.   
   The BOOM config will be placed in the archive user dataspace 
   (/share/lm/data/archive/<USER>/<ARCHIVE_NAME>.ini)::

   # /opt/lifemapper/rocks/bin/seedData <SCENARIO_PACKAGE>
   

Download the current Lifemapper roll files and shasums:

* lifemapper-compute-<yyyy.mm.dd>-0.x86_64.iso (and .sha)
* lifemapper-server-<yyyy.mm.dd>-0.x86_64.iso

#. **Download** new LmServer and LmCompute rolls to server, then validate 
   checksums.  Replace filename with latest roll names::

   # cd /state/partition1/apps/
