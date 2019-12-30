
.. highlight:: rest

Reload Production Lifemapper
==================================
.. contents::  


Backup lmuser table only
-------------------------------
Backup::
  pg_dump  -h localhost  -p 5432  -U admin -W --table="lmuser" \
    --data-only  --column-inserts borg > lmuser_data.sql

Reload::
  psql  -h localhost  -p 5432  -U admin  borg  -f lmuser_data.sql


Backup data and configuration
--------------------------------

* Save any db tables to file like above
* Compress /etc/auto.* files 
* Compress any data to be saved
* Save configuration of zfs and symlinks to large data store for LM install 
  to a file by echoing the output of these commands to a file:
  
  * df -h
  * zpool list
  * zfs list
  * ls -lahtr /share/lm/data
  * ls -lahtr /share/lmserver/data/species
  * more /etc/auto.data

* Save backup files from ZFS disks, also to another machine in case ZFS restore fails
* Export zfs::
  zpool export tank
   


Reload Rocks on Yeti
-------------------------

#. Boot from bootable USB flash drive containing Kernel ISO
#. Install, getting latest rolls from notyeti
#. Enable http
#. Copy public ssh keys over and set to allow only with key
#. Install ZFS from official repo, details in zfs.rst doc
#. Import zfs::
   zpool import tank
#. Create space on tank for lm data, and share with autofs

   #. Create new lmdata volume::
      zfs create tank/lmdata
      zfs set sharenfs=on tank/lmdata
   #. Create /etc/auto.lmdata file with location::
      archive  yeti.local:/tank/lmdata/archive
      pgsql yeti.local:/tank/lmdata/pgsql
   #. Add line to /etc/auto.master file::
      /lmdata  /etc/auto.lmdata  --ghost  --timeout=1200
   #. Restart autofs and test::
      systemctl restart autofs
   #. Setup debugging for autofs by adding line to /etc/sysconfig/autofs::
      LOGGING=”debug”
      
#. Move big data (database and user data) to tank, fix permissions, symlink::
   cp -rp /var/lib/pgsql /tank/lmdata/pgsql
   chown -R postgres:postgres /tank/lmdata/pgsql       
   chmod -R 700 /tank/lmdata/pgsql       
   (cd /var/lib; mv pgsql pgsqlold; ln -s /lmdata/pgsql)
   cp -rp /state/partition1/lm/data/archive /tank/lmdata/archive
   chgrp -R lmwriter /tank/lmdata/archive   
   chmod -R 775 /tank/lmdata/archive
   (cd /state/partition1/lm/; mv archive archiveOLD; ln -s /lmdata/archive)
   
#. Share ZFS dir to nodes
   #. Edit sharenfs attribute. Two options, did both, nothing worked until 411 
      stuff below.  First is command, second added line to /etc/exports::

      zfs set sharenfs=rw=@<private ip prefix>.0/24 tank/lmdata
      /tank/lmdata 192.168.201.1(rw,async,no_root_squash) 192.168.201.0/255.255.255.0(rw,async)

#. in /var/411 remake the maps and then force getting new files on compute nodes, 
   plus resetart autofs::

     make clean
     make force
     rocks sync users
     systemctl restart autofs
     rocks run host compute "411get --all; systemctl restart autofs"

   