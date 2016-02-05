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

-----------------------------
Build VCs on NotYeti with SGE
-----------------------------
If building a virtual cluster on NotYeti, make sure to add SGE from the   
default Rocks server. SGE was (mistakenly) not installed on NotYeti.

    