
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

* Save backup files on ZFS, also to another machine in case ZFS restore fails