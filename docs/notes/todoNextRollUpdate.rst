
.. highlight:: rest

Next roll update
================
.. contents::  

Introduction
------------
Don't forget to handle these issues.

Upgrade CherryPy
----------------

Our installed version is 3.8, current version is 8.1

Upgrade SOLR/JTS
----------------

We have version 6.1 installed, but the /opt/solr directory is linked to the 
/opt/solr-5.3.0 directory, which may be causing problems.  Latest version is 
6.2.  

**Update**: /opt/solr is not linked to 5.3.0

Installed JTS is very old, version 1.8.0, version 1.14 is current.  Pull with
wget instead of using jts zipfile in repo.