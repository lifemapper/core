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