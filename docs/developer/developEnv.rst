
.. hightlight:: rest

Updating an existing Lifemapper Server installation
###################################################
.. contents::  

.. _Update Combined System : docs/UpdatingCombinedSystem.rst

Introduction
************
After the roll is installed, and the instance has been populated, you will want
to update the code, default data, configuration, or database at some point from 
the lifemapper-server roll.  You can apply those changes without losing data.  
If both LmServer and LmCompute are installed on this machine, use 
**Update Combined System** instructions at `Update Combined System`_

Stop processes
**************

#. **Stop the pipeline** as lmwriter (replace 'pragma' with the datasource name 
   configured for this instance, i.e. bison, idigbio) ::    

     % touch /opt/lifemapper/log/pipeline.pragma.die

   **TODO:** Move to command **lm stop pipeline** 

Update roll
***********

#. **Copy new Lifemapper roll with updated RPMs to server**, for example::

   # scp lifemapper-server-6.2-0.x86_64.disk1.iso server.lifemapper.org:

#. **If** this is a development machine:

   #. Clone or update lifemapper workspace git repository ::  

      # cd /state/partition1/workspace
      # git clone https://github.com/lifemapper/core

   #. Remove symlinks to lifemapper workspace git repository directories ::  

      # cd /opt/lifemapper
      # rm Lm*

   #. **If not already done** in your workspace, replace variables in *.in files 
      within the source code into new files.  These can be found with the `find`
      command.  Config files will be created in the non-linked config directory
      correctly without intervention.  This **must** be done before rebooting::  

      # cd /state/partition1/workspace/core
      # find . -name "*.in" | grep -v LmCompute | grep -v config 
        ./LmDbServer/dbsetup/defineDBTables.sql.in
        ./LmDbServer/dbsetup/addDBFunctions.sql.in
      # cd LmDbServer/dbsetup/
      # sed -e 's%@LMHOME@%/opt/lifemapper%g' addDBFunctions.sql.in > addDBFunctions.sql
      # sed -e 's%@LMHOME@%/opt/lifemapper%g' defineDBTables.sql.in > defineDBTables.sql

#. **Add a new version of the roll**, using **clean=1** to ensure that 
   old rpms/files are deleted::

   # rocks add roll lifemapper-server-6.2-0.x86_64.disk1.iso clean=1

#. **Remove some rpms manually** 
   
   #. If the **lifemapper-lmserver** rpm is new, the larger version git tag will  
      force the new rpm to be installed. If the rpm has not changed, you will  
      need to remove it to ensure that the rpm is installed and installation  
      scripts are run.::  

      # rpm -el lifemapper-lmserver
   
   #. Previously, the **rocks-lifemapper** rpm did not have a version, and so 
      defaulted to rocks version 6.2 (rocks-lifemapper-6.2-0.x86_64.rpm).  
      The new version, 1.0.x (i.e. rocks-lifemapper-1.0.0-0.x86_64.rpm) has a lower 
      revision number than the previous rpm, so 1.0.0 will not be installed 
      unless 6.2 is forcibly removed.::

      # rpm -el rocks-lifemapper

   **Note**: Make sure to change rocks-lifemapper version when building roll to 
   make sure that the rpm is replaced and scripts are run.

#. **Install roll**::

   # rocks enable roll lifemapper-server
   # (cd /export/rocks/install; rocks create distro)
   # yum clean all
   # rocks run roll lifemapper-server > add-server.sh 
   # bash add-server.sh > add-server.out 2>&1
    
#. **If** this is a development machine

   #. If there are changes to the config files, modify those in the 
      /opt/lifemapper/config/ directory

   #. Move or remove installed lifemapper component directories and symlink to 
      your git repository ::  

      # cd /opt/lifemapper
      # mkdir installed-1.0.8.lw
      # mv Lm* installed-1.0.8.lw/
      # ln -s /state/partition1/workspace/core/LmBackend
      # ln -s /state/partition1/workspace/core/LmCommon
      # ln -s /state/partition1/workspace/core/LmCompute
      # ln -s /state/partition1/workspace/core/LmDbServer
      # ln -s /state/partition1/workspace/core/LmDebug
      # ln -s /state/partition1/workspace/core/LmServer
      # ln -s /state/partition1/workspace/core/LmWebServer


#. **Reboot front end** ::  

   # reboot
   
   
#. **Troubleshooting** 
   
   #. If the database updates failed, it may be because pgbouncer failed to 
      restart, so::
   
      #. Check for lock files in /var/run/pgbouncer/, /var/lock/subsys/ , and
         /var/run/postgresql/ (owned by pgbouncer).
      #. Double check that pgbouncer is not running
      #. Delete lock files
      #. Restart pgbouncer
      #. Re-run the failed command::
          
         # /rocks/bin/initLM
         
      #. Check the output in /tmp/initLM.log

Add a new computation server
****************************

.. _Using : docs/Using.rst#add-a-new-lmcompute

.. _Add a new LmCompute : docs/Using.rst#add-a-new-lmcompute

   Instructions at **Add a new LmCompute** at `Using`_
#. Follow instructions at  `Add a new LmCompute`_


Add/change Archive User
***********************

#. Change the archive user  as ``root`` 

   Add ARCHIVE_USER to the [LmCommon - common] section of site.ini file.  
   
   The ARCHIVE_USER must own all occurrence and scenario records; so you must 
   insert new or re-insert existing climate data as this user.  The user will 
   be added automatically when running this script :: 

     # $PYTHON /opt/lifemapper/LmDbServer/tools/initCatalog.py scenario 

   **TODO:** Move to command **lm init catalog**

#. **Start the pipeline**  as ``lmserver`` to initialize all new jobs with the new species data.::

     % $PYTHON /opt/lifemapper/LmDbServer/pipeline/localpipeline.py &
   
   **TODO:** Move to command **lm start pipeline**

          


   
Test
****

#. **Test the LmWebServer** setup as user ``lmwriter``
  
   To become lmwriter use do: ::

     # su - lmwriter
     
   As lmwriter user, execute the following to check if the web server is setup correctly, 
   successful output is similar to that shown under each.   ::  

     % python2.7 /opt/lifemapper/LmWebServer/scripts/createTestUser.py
       Successfully created user
       
     % python2.7 /opt/lifemapper/LmWebServer/scripts/checkJobServer.py)
       30 Mar 2015 14:17 MainThread.log.debug line 80 DEBUG    {'epsgcode': '4326', 'displayname': 'Test Chain57111.8872399', 'name': 'Test points57111.8872399', 'pointstype': 'shapefile'}
       30 Mar 2015 14:17 MainThread.log.debug line 80 DEBUG    Test Chain57111.8872399
       30 Mar 2015 14:17 MainThread.log.warning line 136 WARNING  Database connection is None! Trying to re-open ...
       Closed/wrote dataset /share/lmserver/data/archive/unitTest/000/000/000/194/pt_194.shp
       creating index of new  LSB format
       30 Mar 2015 14:17 MainThread.log.debug line 80 DEBUG       inserted job to write points for occurrenceSet 194 in MAL
       Occurrence job id: 962
       Model job id: 963
       Projection job id: 964
     
   This test shows the result of URLs on the local server.  EML is not configured, 
   so errors for this format may be ignored.  We will add configuration to identify 
   installed formats.  ::  

     % python2.7 /opt/lifemapper/LmWebServer/scripts/checkLmWeb.py
       30 Mar 2015 14:17 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net
       30 Mar 2015 14:17 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/projections
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/scenarios
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/rad/
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/rad/experiments
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/rad/layers
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/occurrences/117/atom
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/occurrences/117/csv
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/occurrences/117/eml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/occurrences/117/html
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/occurrences/117/json
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/occurrences/117/kml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/occurrences/117/shapefile
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/occurrences/117/xml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/scenarios/3/atom
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/scenarios/3/eml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/scenarios/3/html
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/scenarios/3/json
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/scenarios/3/xml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments/118/atom
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments/118/eml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments/118/html
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments/118/json
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments/118/kml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments/118/model
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments/118/status
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/experiments/118/xml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/ascii
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/atom
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/eml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG     returned HTTP code: 500
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/html
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/json
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/kml
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/raw
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/tiff
       30 Mar 2015 14:18 MainThread.log.debug line 80 DEBUG    Url: http://pc-167.calit2.optiputer.net/services/sdm/layers/58/xml
       
  **TODO:** Move to commands **lm test <user | jobserver | web>**
  
#. **Run the pipeline**  as user lmwriter

   To start the pipeline  ::  

     % python2.7 /opt/lifemapper/LmDbServer/pipeline/localpipeline.py

   To Stop the pipeline  ::    

     % touch /opt/lifemapper/pipeline.pragma.die
     
     
   **TODO:** Move to commands **lm start/stop pipeline**
   
#. After the pipeline has run for awhile, and there are some completed jobs, test this:
 
     % python2.7 /opt/lifemapper/LmWebServer/scripts/checkLmWeb.py

   **TODO:** Move to command **lm test web**
