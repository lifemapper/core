
.. highlight:: rest

Update Lifemapper via roll (or code RPMs)
=========================================
.. contents::  


Yeti update notes
-------------------------------
#. Two things extra to do on yeti update - 
   1. edit /etc/httpd/conf.d/lifemapper.conf , uncommenting yeti-specific rewrites
   1. restart apach
   1. edit /opt/lifemapper/config/boom.public.params removing “_subset” from occ data value