################
Troubleshooting:
################

Yum error on Rocks 6.2
~~~~~~~~~~~~~~~~~~~~~~
* "yum list updates" fails with [Errno 14] problem making ssl connection::
   [root@notyeti-193 lifemapper]# yum list updates
   Setting up Install Process
   https://download.postgresql.org/pub/repos/yum/9.1/redhat/rhel-6-x86_64/repodata/repomd.xml: [Errno 14] problem making ssl connection
   Trying other mirror.
   Error: Cannot retrieve repository metadata (repomd.xml) for repository: pgdg91. Please verify its path and try againe-giving-errno-14-problem-making-ssl-connection/
   
* Tried following, but ca-certificates openssl nss were already installed
  (http://centosquestions.com/yum-update-giving-errno-14-problem-making-ssl-connection/)

* Used following, disabling pgdg91 repo
  (https://centrify.force.com/support/Article/KB-11461-Centrify-Yum-repo-fails-with-Errno-14-problem-making-ssl-connection/)::
   yum-config-manager --disable pgdg91
   
   
Lifemapper build/configure/populate issues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Bad python import points to unrecognized Unicode 
-------------------------------------------------
http://effbot.org/pyfaq/when-importing-module-x-why-do-i-get-undefined-symbol-pyunicodeucs2.htm

Some pre-built python libraries are built with UC2 (2 bytes per unicode char)
and some with UC4 (4 bytes per char).  These must agree or you will get errors
(i.e. in gdal).  Solution is to not use the offending pre-built libraries.

August 2019:
* non-opt-python Python 2.7.5 is UC4 
* opt-python Python 2.7.13 is UC2

To find out::
	import sys
	if sys.maxunicode > 65535:
	    print 'UCS4 build'
	else:
	    print 'UCS2 build'
	       
Solution:: Make sure to run "module load opt-python" to load Python 2.7.13 
     
     
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