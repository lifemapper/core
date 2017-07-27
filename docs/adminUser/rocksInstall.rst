######
Notes:
######

********************
Fresh Rocks Install:
********************

Start/restart a virtual cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Make sure that the boot action is "install", then start or restart an existing 
cluster.  Open the "virt-manager" application in order to interact with
the installation user-interface:: 

 * rocks set host boot <vm-name> action=install
 * (opt) rocks run host <vm-name> "shutdown -h now"
 * rocks start host vm <vm-name>
 * virt-manager

Install the rolls:
~~~~~~~~~~~~~~~~~~

Make sure to get the SGE roll directly from SDSC download site (default on 
the installation screen) and the Python roll from notyeti.lifemapper.org
(with the updated Python roll).  All other rolls should come from the yeti or 
notyeti sites if they are physically located there (to speed download time).
  
For all clusters, install the following

* area51
* base 
* ganglia
* hpc
* kernel
* os
* python
* sge (LmCompute)
* webserver (LmServer)

Physical clusters only:
  
* kvm
* zfs

Other info:

* Lawrence Geo:  N38.969  W95.245
* Public Interface: assigned by Greg Smith for MAC address
  Note that since we are not using DHCP, any MAC address assigned on creation
  will be fine.
* Private Interface:  (notyeti VMs: available internal 192.168.xxx.1, where
  xxx is the last quartet of the public IP address)
* Gateway:  129.237.201.254
* DNS:  129.237.133.1
* NTP server:  time.ku.edu
* Auto-Partitioning

Update Python Roll
~~~~~~~~~~~~~~~~~~
**If** the sqlite3 module is not available in python2.7:

* Get an updated python roll from pc-167.calit2.optiputer.net in /root/roll/ 
  with sqlite3.so
* Copy the iso, then remove old rpm, add roll making sure it will override 
  existing::

        rpm -el opt-python-27-2.7.9-1.x86_64
        rocks add roll python-6.2….iso clean=1
        rocks enable roll python
        (cd /export/rocks/install; rocks create distro)
        yum clean all
        rocks run roll python > add-roll-python.sh
        bash add-roll-python.sh > add-roll-python.out 2>&1

Enable www access
~~~~~~~~~~~~~~~~~
Follow procedure at http://yeti.lifemapper.org/roll-documentation/base/6.2/enable-www.html

Insert compute nodes
~~~~~~~~~~~~~~~~~~~~
Start insert-ethers process on the Frontend, then start each node.  Wait until 
each node has been recognized ( ) and accepted (*) in the insert-ethers
window before starting the next node.

**************************
All KU-Lifemapper Clusters
**************************

Secure SSH
~~~~~~~~~~

Add public key to new virtual frontend for key-based authentication::

    ssh-copy-id -i ~/.ssh/id_rsa.pub root@xxx.xxx.xxx.xxx

Turn off password authentication by editing the values in /etc/ssh/sshd_config::

    PasswordAuthentication no
    ChallengeResponseAuthentication no
    UsePAM no 
    
Then restart the sshd service::

    service sshd restart

Setup for security updates
~~~~~~~~~~~~~~~~~~~~~~~~~~

Download, build, then install the security-updates roll into directory below. 
Instructions at https://github.com/rocksclusters/security-updates ::

    cd /state/partition1/site-roll/rocks/src/roll 
    git clone https://github.com/rocksclusters/security-updates.git

Install the KU Production (kuprod) roll. Download iso and sha files, current
version is: 
* http://svc.lifemapper.org/dl/kuprod-1.0-0.x86_64.disk1.iso
