
.. highlight:: rest

Install or Update a Lifemapper Server/Compute installation
==========================================================
.. contents::  

git config --global user.email "zzeppozz@gmail.com"
git config --global user.name "Aimee Stewart"

Current versions
----------------
#. **Download** new LmServer and LmCompute rolls to server, then validate 
   checksums.  Replace filename with latest roll names::

   # sha256sum -c lifemapper-*.sha

(If update) Stop processes
--------------------------

#. **Stop the mattDaemon** as lmwriter::

     lmwriter$ $PYTHON /opt/lifemapper/LmServer/tools/matt_daemon.py stop

#. **Caution** If want to **completely destroy** existing install, including
   deleting the database and clearing lm data from filesystem, run::

   # bash /opt/lifemapper/rocks/etc/clean-lm-server-roll.sh
   # bash /opt/lifemapper/rocks/etc/clean-lm-compute-roll.sh

Update existing code and script RPMs (without new roll)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. Copy individual rpms to /export/rocks/install/contrib/7.0/x86_64/RPMS/ 
   This will only update RPMs that are part of the original roll.
   To add rpms that are not yet part of the rolls, put them into a directory 
   shared from FE to nodes (/share/lm/). 
   
#. then rebuild distribution.  ::
   
   # (cd /export/rocks/install; rocks create distro; yum clean all)
   # yum list updates
   # yum update
   
#. Run scripts to update config and DB types/views/functions ::
   
   # /opt/lifemapper/rocks/bin/updateLM
   
#. Install new rpms on FE (only if re-installed roll)  ::
   
   # rpm -iv /share/lm/*rpm

#. Update nodes ::
   
   # rocks set host boot compute action=install
   # rocks run host compute reboot

#. Update nodes with non-roll rpms::
   
   # rocks run host compute "(hostname; rpm -iv /share/lm/*rpm)"


Update existing rolls
~~~~~~~~~~~~~~~~~~~~~~~
#. Remove old rolls (without cleaning or removing individual rpms)::

   # rocks remove roll lifemapper-server lifemapper-compute
   

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
   # (module unload opt-python; \
      cd /export/rocks/install; \
      rocks create distro; \
      yum clean all)

#. **Create and run LmServer/LmCompute scripts**::

    # (module unload opt-python; \
       rocks run roll lifemapper-compute > add-compute.sh; \
       bash add-compute.sh 2>&1 | tee add-compute.out)

    # (module unload opt-python; \
       rocks run roll lifemapper-server > add-server.sh; \
       bash add-server.sh 2>&1 | tee add-server.out)

#. **IFF** installing compute roll first or alone, manually set the 
   LM_dbserver and LM_webserver attributes.  If this server will also
   host the web/db server, set the value=true otherwise, value=<ip or FQDN>::
   
    # rocks add host attr localhost LM_webserver value=true
    # rocks add host attr localhost LM_dbserver value=true

    
Finish install
--------------

#. **Reboot front end** ::  

   # shutdown -r now
   
Install nodes from Frontend
---------------------------

#. **Rebuild the compute nodes** ::  

   # rocks set host boot compute action=install
   # rocks run host compute reboot     

Install bugfixes
----------------

#. Compute Nodes - check/fix node group permissions on /state/partition1/lmscratch ::

   # /opt/lifemapper/rocks/bin/fixNodePermissions
      
Start matt_daemon
-----------------------

#. Start makeflow with matt_daemon

   # /opt/lifemapper/rocks/bin/matt_daemon
      
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
   psql (9.6.15)
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

Accepted species from GBIF:
https://www.gbif.org/species/search?rank=SPECIES&dataset_key=d7dddbf4-2cf0-4f39-9b2a-bb099caae36c&status=ACCEPTED&advanced=1

Troubleshooting
----------------
   
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
   
Unable to open database on port 6432 (pgbouncer)
-------------------------------------------------
Fail message:: 
Failed to open Borg (user=sdlapp dbname=borg host=notyeti-191 port=6432): 
('could not connect to server: Connection refused\n\tIs the server running on 
host "notyeti-191" (192.168.191.1) and accepting\n\tTCP/IP connections on port 6432?\n',)

Clue:: Server is running on public IP, not private

Solution:: config.lmserver.ini should have FQDN in DB_HOSTNAME 
           (i.e. notyeti-191.lifemapper.org)

Fix::  When running initLM (on reboot, after install), subcommand
       rocks/bin/updateCfg should fill in FQDN 
       
Fail message:: 
>>> scribe.open_connections()
30 Mar 2021 12:14 MainThread.borg_scribe.open_connections 
line 61 ERROR    Failed to open Borg (user=sdlapp dbname=borg host=notyeti-194.lifemapper.org port=6432): 
('ERROR:  no more connections allowed (max_client_conn)\n',)
False

and

[root@notyeti-194 ~]# psql -U admin -d borg -p 6432
psql: could not connect to server: No such file or directory
        Is the server running locally and accepting
        connections on Unix domain socket "/var/run/postgresql/.s.PGSQL.6432"?


Clue:: No more connections allowed (max_client_conn), 
       look at value in /etc/pgbouncer/pgbouncer.ini, max_client_conn = 0, 

Solution::  change max_client_conn = (500 * feCPUCount), also fix 
            default_pool_size = (200 * feCPUCount) and 
            reserve_pool_size = (20 * feCPUCount) 

Reason:: updateCfg failed the first time through because compute nodes had not
         been added to cluster, so value was computed incorrectly 
