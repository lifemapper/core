.. hightlight:: rest

Create a Lifemapper Virtual Cluster
###################################
.. contents::  

.. _Basic Rocks Install : docs/developer/rocksInstall.rst

--------------------------
Create the Virtual Cluster
--------------------------

Notyeti contains tools in the directory: /tank/root/

add-cluster.sh::
   A helper script
   
notyeti-vcs.csv::
   A list of clusters on this machine, with the command and parameters for creation
   
-----------------------------
Build VCs on NotYeti with SGE
-----------------------------

Follow instructions at **Basic Rocks Install** at `Basic Rocks Install`_

If building a virtual cluster on NotYeti, make sure to add SGE from the   
default Rocks server. SGE was (mistakenly) not installed on NotYeti.
(???) The SGE roll has problems if installed after the initial build.

---------------------------------
Defer checksum from NIC to kernel
---------------------------------
If you are unable to add compute nodes (using insert-ethers) on a virtual 
cluster, it may be due to a NIC which introduces delays in the communications.  
KU has this trouble with 
This will need to be added to all virtual frontends that are built on Yeti and 
NotYeti and other hosts with a similar NIC 

Execute the following command on a new virtual frontend::

    ethtool -K eth0 tx off rx off 

Add to /etc/rc.local::

    # This commands resets the checksum that is done on the NIC (defers 
    # it to kernel). When checksum is done on the NIC it introduces 
    # delays in the response communication between frontend and compute 
    # node and building of compute node fails.
    ethtool -K eth0 tx off rx off

-------------------------
Add virtual compute nodes
-------------------------
On the new virtual Front End, run insert-ethers, choosing Appliance Type 
'Compute'::

    [root@notyeti-195 ~]# insert-ethers

Then start a virtual compute node from the physical host::

    [root@notyeti ~]# rocks start host vm vm-notyeti-195-0
    
After a short time, the Virtual Frontend will recognize the new machine, 
indicated by first a popup message, next list its MAC address in the window 
with by empty parentheses.  

After another short time, it will insert it as a compute node, indicated by an
asterick between the parentheses.

Follow this procedure for each virtual compute node, then exit the program.  
    

------------------------
Remove a Virtual Cluster
------------------------
To completely remove a cluster (i.e. notyeti-193) and associated files, 
files and database records::

   # rocks remove cluster notyeti-193
   # zfs destroy tank/vms/notyeti-193
   
   