.. hightlight:: rest

Install
#######

.. contents::

.. _Rocks Install Procedure : docs/developer/rocksInstall.rst
.. _Virtual Cluster Troubleshooting : docs/developer/virtualCluster.rst
.. _Lifemapper Install Procedure : docs/developer/installLifemapperSystem.rst


The following details how to install both Lifemapper rolls, LmServer and 
LmCompute, on a single physical or virtual cluster.  Some steps may be 
unnecessary, or already complete, for your instance.

Initial Rocks Setup
-------------------
#. `Rocks Install Procedure`_
#. Optional steps `Virtual Cluster Troubleshooting`_

LmServer Roll Install
---------------------

If both rolls will be installed on the server, LmServer and LmCompute may be 
installed at the same time to save time and reboot only once.

#. `Lifemapper Install Procedure`_


Update (deprecated)
###################
   
The following is out-of-date, instead follow instructions at 
`Lifemapper Install Procedure`_

Updating a System with both LmCompute and LmServer
--------------------------------------------------
#. Update LmCompute and LmServer with new LmCompute and LmServer rolls. 
   https://github.com/pragmagrid/lifemapper-server/blob/kutest/docs/UpdatingCombinedSystem.rst

Updating a System with only LmServer
------------------------------------
#. Update LmServer source code with new roll for both code and configuration.
   https://github.com/pragmagrid/lifemapper-server/blob/kutest/docs/Updating.rst
   
   
Updating a System with only LmCompute
-------------------------------------
#. Update LmCompute source code with new roll for code amd configuration.  
   https://github.com/pragmagrid/lifemapper-compute/blob/kutest/docs/Updating.rst
