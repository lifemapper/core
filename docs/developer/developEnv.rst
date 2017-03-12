
.. hightlight:: rest

Set up development environment
##############################
.. contents::  

.. _Install Lifemapper System : docs/adminUser/installLifemapperSystem.rst

Introduction
************
To test and debug and test code not yet tagged, use a cluster with both 
LmCompute and LmServer installed (see **Install Lifemapper System** 
instructions at `Install Lifemapper System`_).  After install connect 
code checked out from the git repository.

Install Frontend and Nodes
**************************

#. Install with instructions at `Install Lifemapper System`_

#. **Reboot front end** ::  

   # reboot
   
#. Install nodes from Frontend 


Connect development code on Frontend
************************************

#. Clone lifemapper workspace git repository ::  

   # mkdir /state/partition1/workspace
   # cd /state/partition1/workspace
   # git clone https://github.com/lifemapper/core

#. Copy database connection file to git tree::
      
   # cd /opt/lifemapper
   # cp -p LmServer/db/connect.py /state/partition1/workspace/core/LmServer/db/

  OR recreate connection file with :: 
  
   # /opt/lifemapper/rocks/bin/confDbconnect
   
#. Copy files with script-updated variables to your git tree. 
   Config files were created in the non-linked config directory
   correctly without intervention.  The maxent and lmMaxent executables will  
   be installed into the rocks/bin directory without intervention.::
      
   # cd /opt/lifemapper
   # find . -name "*.in"  | grep -v config | grep -v axent
     ./LmDbServer/dbsetup/addDBFunctions.sql.in
     ./LmDbServer/dbsetup/defineDBTables.sql.in
   # cp -p LmDbServer/dbsetup/addDBFunctions.sql /state/partition1/workspace/core/LmDbServer/dbsetup/
   # cp -p LmDbServer/dbsetup/defineDBTables.sql /state/partition1/workspace/core/LmDbServer/dbsetup/

  OR recreate files with sed commands:: 

   # cd /state/partition1/workspace/core
   # sed -e 's%@LMHOME@%/opt/lifemapper%g' LmDbServer/dbsetup/addDBFunctions.sql.in > LmDbServer/dbsetup/addDBFunctions.sql
   etc 
     
#. If there are new changes to the config files, not included in the 
   installed rpm, modify those in the /opt/lifemapper/config/ directory

#. Delete installed lifemapper component directories then symlink to your git 
   repository ::  

   # cd /opt/lifemapper
   # rm -rf Lm* 
   # ln -s /state/partition1/workspace/core/LmBackend
   # ln -s /state/partition1/workspace/core/LmCommon
   # ln -s /state/partition1/workspace/core/LmCompute
   # ln -s /state/partition1/workspace/core/LmDbServer
   # ln -s /state/partition1/workspace/core/LmDebug
   # ln -s /state/partition1/workspace/core/LmServer
   # ln -s /state/partition1/workspace/core/LmWebServer


Connect development code on Nodes
*********************************

#. Clone or update lifemapper workspace git repository ::  

   # rocks run host compute "(cd /state/partition1/; mkdir workspace; git clone http://github.com/lifemapper/core)"

#. Currently there are no *.in files for which to fill variables on nodes
   The maxent and lmMaxent executables will be installed into the rocks/bin 
   directory without intervention.
  
#. Delete installed lifemapper component directories and symlink to your git tree ::  

   #  rocks run host compute "(cd /opt/lifemapper; rm -rf Lm*)"
   #  rocks run host compute "(cd /opt/lifemapper; ln -s /state/partition1/workspace/core/LmBackend)"
   #  rocks run host compute "(cd /opt/lifemapper; ln -s /state/partition1/workspace/core/LmCommon)"
   #  rocks run host compute "(cd /opt/lifemapper; ln -s /state/partition1/workspace/core/LmCompute)"

Sync development code
*********************

#. To sync frontend with github::

   # cd /state/partition1/workspace/core/; git pull
   
#. To sync nodes with github::

   # rocks run host compute "(cd /state/partition1/workspace/core/; git pull)"

   
Troubleshooting
***************
   
If the database updates failed, it may be because pgbouncer failed to 
restart, so:
   
   #. Check for lock files in /var/run/pgbouncer/, /var/lock/subsys/ , and
      /var/run/postgresql/ (owned by pgbouncer).
   #. Double check that pgbouncer is not running
   #. Delete lock files
   #. Restart pgbouncer
   
If you are installing on a new machine, you will    
   #. Re-run the failed command::  
           
      # /rocks/bin/initLM
         
   #. Check the output in /tmp/initLM.log


Add/change Server input data/user
*********************************

#. Change the archive user  as ``root``.  Follow instructions at 
   (`Install Lifemapper System`_) **To change defaults**

