
.. highlight:: rest

Update code via RPM
===================
.. contents::  


Build code RPMs
----------------------

For lifemapper-server and lifemapper compute rolls:

#. Commit, push, tag, push all code updates to github 
   * lifemapper/core repo
   * lifemapper/rutabaga repo for webclient changes
   * biotaphy/ot_service_wrapper repo for OTOL connect changes

#. Update LMCODE_VERSION and VERSION in github:
   * pragmagrid/lifemapper-compute/src/version.mk 
   * pragmagrid/lifemapper-server/src/version.mkself
   * pragmagrid/lifemapper-server/src/webclient/version.mk.in
   * pragmagrid/lifemapper-server/src/biotaphy-otol
   
#. Build RPMs for lmserver:
  * src/lmserver - lifemapper-lmserver
  * src/rocks-lifemapper - rocks-lifemapper
  * src/webclient  - lifemapper-webclient
  * src/biotaphy-otol - opt-lifemapper-biotaphy-otol
  
#. Build RPMs for lmcompute:
  * src/lmcompute - lifemapper-lmcompute
  * src/rocks-lmcompute -rocks-lmcompute
  
Install new RPMs on test server
-------------------------------

#. Move new rpms (including noarch rpms) to 
   /export/rocks/install/contrib/6.2/x86_64/RPMS/ 

#. **Create distribution**::

   # (cd /export/rocks/install; rocks create distro; yum clean all)

#. Update with yum::

   # yum list updates
   # yum update
  
#. Update lmserver config file with vals for @vars@::
   # rocks/bin/updateCfg
  
#. DB connection file LmServer/db/connect.py file should still exist, but if it 
   is missing, run::
   # rocks/bin/confDbconnect
  
  