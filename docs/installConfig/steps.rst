########################
Lifemapper install steps
########################

The following details how to install both Lifemapper rolls, LmServer and 
LmCompute, on a single physical or virtual cluster.  Some steps may be 
unnecessary, or already complete, for your instance.

*******************
Initial Rocks Setup
*******************
#. Install and configure: 
   https://github.com/lifemapper/core/blob/master/docs/installConfig/install.rst
#. Optional troubleshooting steps for virtual clusters:  
   https://github.com/lifemapper/core/blob/master/docs/installConfig/virtualCluster.rst

*********************
LmServer Roll Install
*********************

This may be installed first or second, little difference.

#. Add the roll to your cluster with the following:
   https://github.com/pragmagrid/lifemapper-server/tree/kutest#adding-a-roll-to-a-live-frontend
#. Reboot as directed in the instructions
#. Configure, populate, test :
   https://github.com/pragmagrid/lifemapper-server/blob/master/docs/Using.rst
#. Create Layer package (until this is replaced by a package used by both LmServer and LmCompute).
#. https://github.com/lifemapper/core/blob/master/docs/using/starting.rst

**********************
LmCompute Roll Install
**********************

#. Add the roll to your cluster:  
   https://github.com/pragmagrid/lifemapper-compute/tree/kutest#adding-a-roll-to-a-live-frontend
#. Reboot as directed in the instructions
#. Install on compute nodes, as directed in instructions.
#. Seed Layers (until this is replaced by a package used by both LmServer and LmCompute).
#. Configure and test: 
   https://github.com/pragmagrid/lifemapper-compute/tree/kutest#using-a-roll

#######################
Lifemapper update steps
#######################
   
Updating a System with both LmCompute and LmServer
**************************************************
#. Update LmCompute and LmServer with new LmCompute roll, 
   lifemapper-lmserver rpm for LmServer code, rocks-lifemapper rpm for LmServer 
   configuration.  Configuration changes should be implemented in the 
   rocks-lifemapper script 'updateLM' in the /opt/lifemapper/rocks/bin directory.  
   Full instructions at:
   https://github.com/pragmagrid/lifemapper-server/blob/kutest/docs/UpdatingCombinedSystem.rst

Updating a System with only LmServer
************************************
#. Update LmServer source code with new lifemapper-lmserver rpm for code,
   and rocks-lifemapper rpm for configuration.  One-time configuration changes 
   may be needed and should be implemented in the rocks-lifemapper script
   'updateLM' in the /opt/lifemapper/rocks/bin directory.  Full instructions at:
   https://github.com/pragmagrid/lifemapper-server/blob/kutest/docs/Updating.rst
   
   **Do not** re-install the LmServer roll, as that will destroy the existing 
   database.
   
Updating a System with only LmCompute
*************************************
#. Update LmCompute source code with new lifemapper-lmcompute rpm for code,
   and rocks-lmcompute rpm for configuration.  One-time configuration changes 
   may be needed and should be implemented in the rocks-lmcompute script
   'updateIP-lmcompute' in the /opt/lifemapper/rocks/bin directory.  Full
   instructions at:
   https://github.com/pragmagrid/lifemapper-compute/blob/kutest/docs/Updating.rst
