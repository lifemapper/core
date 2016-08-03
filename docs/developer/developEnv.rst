
.. hightlight:: rest

Set up development environment
##############################
.. contents::  

.. _Install Lifemapper System : docs/adminUser/installLifemapperSystem.rst

Introduction
************
To test and debug and test code not yet tagged, use a cluster with both 
LmCompute and LmServer installed (see **Install Lifemapper System** 
instructions at `Install Lifemapper System`_), then connect code checked out 
from the git repository.

Connect development code on Frontend
************************************

#. Clone or update lifemapper workspace git repository ::  

   # mkdir /state/partition1/workspace
   # cd /state/partition1/workspace
   # git clone https://github.com/lifemapper/core

#. Copy database connection file to git tree::
      
   # cd /opt/lifemapper
   # cp -p LmServer/db/connect.py /state/partition1/workspace/core/LmServer/db/

   #. If you forget to copy connection file, create it manually with::  

      # /opt/lifemapper/rocks/bin/confDbconnect
   
#. Copy files with replaced variables from installation to your git tree. 
   Config files will be created in the non-linked config directory
   correctly without intervention.  The maxent and lmMaxent executables will  
   be installed into the rocks/bin directory without intervention.::
      
   # cd /opt/lifemapper
   # find . -name "*.in"  | grep -v config | grep -v axent
     ./LmCompute/tools/lmJobScript.in
     ./LmDbServer/dbsetup/addDBFunctions.sql.in
     ./LmDbServer/dbsetup/defineDBTables.sql.in
   # cp /opt/lifemapper/<*.in file without .in> /state/partition1/workspace/core/
      
   #. If you forget to copy updated files, fix them manually, similarly to the sed
      command below::  

      # cd /state/partition1/workspace/core
      # sed -e 's%@LMHOME@%/opt/lifemapper%g' LmDbServer/dbsetup/addDBFunctions.sql.in > LmDbServer/dbsetup/addDBFunctions.sql
    
#. If there are new changes to the config files, not included in the 
   installed rpm, modify those in the /opt/lifemapper/config/ directory

#. Move installed lifemapper component directories to a new directory and 
   symlink to your git repository ::  

   # cd /opt/lifemapper
   # mkdir installed_1.1.5.lw
   # mv Lm* installed_1.1.5.lw/
   # ln -s /state/partition1/workspace/core/LmBackend
   # ln -s /state/partition1/workspace/core/LmCommon
   # ln -s /state/partition1/workspace/core/LmCompute
   # ln -s /state/partition1/workspace/core/LmDbServer
   # ln -s /state/partition1/workspace/core/LmDebug
   # ln -s /state/partition1/workspace/core/LmServer
   # ln -s /state/partition1/workspace/core/LmWebServer
   
Connect development code on Frontend
************************************

#. Clone or update lifemapper workspace git repository ::  

   # mkdir /state/partition1/workspace
   # cd /state/partition1/workspace
   # git clone https://github.com/lifemapper/core

#. Copy files with replaced variables from installation to your git tree. 
   Config files will be created in the non-linked config directory
   correctly without intervention.  The maxent and lmMaxent executables will  
   be installed into the rocks/bin directory without intervention.::
      
   # cd /opt/lifemapper
   # find . -name "*.in"  | grep -v config | grep -v axent
     ./LmCompute/tools/lmJobScript.in
   # cp /opt/lifemapper/LmCompute/tools/lmJobScript /state/partition1/workspace/core/LmCompute/tools/

#. Delete installed lifemapper component directories and symlink to your git tree ::  

   # cd /opt/lifemapper
   # rm -rf Lm*
   # ln -s /state/partition1/workspace/core/LmBackend
   # ln -s /state/partition1/workspace/core/LmCommon
   # ln -s /state/partition1/workspace/core/LmCompute

   
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

#. Change the archive user  as ``root`` 

   Add ARCHIVE_USER to the [LmCommon - common] section of site.ini file.  
   
   The ARCHIVE_USER must own all occurrence and scenario records; so you must 
   insert new or re-insert existing climate data as this user.  The user will 
   be added automatically when running this script :: 

     # $PYTHON /opt/lifemapper/rocks/bin/fillDB 

   **TODO:** Move to lm command **lm init data**

#. **Start the archivist**  as ``lmserver`` to initialize new jobs for the new species data.::

     % $PYTHON /opt/lifemapper/LmDbServer/pipeline/archivist.py start
   
   **TODO:** Move to command **lm start archivist**

