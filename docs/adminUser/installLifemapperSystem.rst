
.. highlight:: rest

Install or Update a Lifemapper Server/Compute installation
==========================================================
.. contents::  

.. _Configure Archive Data : docs/adminUser/buildLifemapperData.rst

Current versions
----------------
#. **Download** new LmServer and LmCompute rolls to server, then validate 
   checksums.  Replace filename with latest roll names::

   # sha256sum -c lifemapper-*.sha

(If update) Stop processes
--------------------------

#. **Stop the daboom daemon** as lmwriter ::    

     lmwriter$ $PYTHON /opt/lifemapper/LmDbServer/boom/daboom.py stop

#. **Stop the mattDaemon** as lmwriter::

     lmwriter$ $PYTHON /opt/lifemapper/LmServer/tools/mattDaemon.py stop

#. **Caution** If want to **completely destroy** existing install, including
   deleting the database and clearing lm data from filesystem, run::

   # bash /opt/lifemapper/rocks/etc/clean-lm-server-roll.sh
   # bash /opt/lifemapper/rocks/etc/clean-lm-compute-roll.sh

Update existing install
~~~~~~~~~~~~~~~~~~~~~~~
#. Remove old rolls (without cleaning or removing individual rpms)::

   # rocks remove roll lifemapper-server lifemapper-compute
   
#. (Obsolete?) Remove old elgis repository rpm (it will cause yum to fail 
   and pull old pgbouncer, postgresql, rpms from the roll.  TODO: update static 
   rpms in roll) ::
   
   # rpm -evl --nodeps elgis-release
   
#. (Optional) When updating an existing install, it should always be true that  
   the configuration rpms (rocks-lifemapper, rocks-lmcompute) have new version 
   numbers, matching the code rpms (lifemapper-lmserver or lifemapper-lmcompute).  
   As long as this is true, rpms will be replaced correctly.  If it is false, 
   the configuration rpms must be manually removed so that configuration scripts 
   will be run on install. If the above is true on a lifemapper-compute 
   installation, do the same thing for every node::
   
   # rpm -el rocks-lmcompute
      

Install both rolls on Frontend
------------------------------

New install
~~~~~~~~~~~
If you do not need to save the existing data files and database records, 
run the cleanRoll scripts for each roll. 
   
#. **Add a new roll and rpms**, ensuring that old rpms/files are replaced.  
   Replace the following roll name with the latest version, identified
   at the top of this document::

   # rocks add roll lifemapper-server-*.iso clean=1
   # rocks add roll lifemapper-compute-*.iso clean=1
   
#. **Create distribution**::

   # rocks enable roll lifemapper-compute lifemapper-server
   # (cd /export/rocks/install; rocks create distro; yum clean all)

#. **Create and run LmServer/LmCompute scripts**::

    # (rocks run roll lifemapper-server > add-server.sh; \
       rocks run roll lifemapper-compute > add-compute.sh; \
       bash add-server.sh 2>&1 | tee add-server.out; \
       bash add-compute.sh 2>&1 | tee add-compute.out)

#. **IFF** installing compute roll first or alone, manually set the 
   LM_dbserver and LM_webserver attributes.  If this server will also
   host the web/db server, set the value=true otherwise, value=<ip or FQDN>::
   
    # rocks add host attr localhost LM_webserver value=true
    # rocks add host attr localhost LM_dbserver value=true

    
Finish install
--------------

#. **Reboot front end** ::  

   # reboot
   
Install nodes from Frontend
---------------------------

#. **Rebuild the compute nodes** ::  

   # rocks set host boot compute action=install
   # rocks run host compute reboot     

Install bugfixes
----------------

#. Compute Nodes - check/fix node group permissions on /state/partition1/lmscratch ::

   # rocks run host compute "(hostname; chgrp -R lmwriter /state/partition1/lmscratch; chmod -R g+ws /state/partition1/lmscratch)"
   # rocks run host compute "(hostname; ls -lahtr /state/partition1/lmscratch)"
      
Look for Errors
---------------
   
#. **Check log files** After the frontend boots up, check the success of 
   initialization commands in log files in /tmp (these may complete up to 5
   minutes after reboot).  The post-99-lifemapper-lm*.log files contain all
   the output from all reinstall-reboot-triggered scripts and are created fresh 
   each time.  All other logfiles are in /state/partition1/lmscratch/log 
   and may be output appended to the end of an existing logfile (from previous 
   runs) and will be useful if the script must be re-run manually for testing.
#. **Clean compute nodes**  
   
Check LmServer
~~~~~~~~~~~~~~
#. Check LmServer logfiles

   * /tmp/post-99-lifemapper-lmserver.debug (calls initLM on reboot) 
   * files in /state/partition1/lmscratch/log
     * initLM.log
     * installServerCronJobs.log
     * fillDB
     
#. Check database contents ::  

   # export PGPASSWORD=`grep admin /opt/lifemapper/rocks/etc/users | awk '{print $2}'`
   # psql -U admin -d borg
   psql (9.1.22)
   Type "help" for help.
   borg=> select scenariocode, userid from scenario;

Check LmCompute
~~~~~~~~~~~~~~~
#. Check LmCompute logfiles

   * /tmp/post-99-lifemapper-lmcompute.debug  (calls initLMcompute on reboot) 
   * files in /state/partition1/lmscratch/log
     * initLMcompute.log 
     * installComputeCronJobs.log
     * transformData.log (transformData must be run manually by user after reboot)

