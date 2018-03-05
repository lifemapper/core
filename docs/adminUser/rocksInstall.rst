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
  
For all clusters, install the following (Rocks 7.0):
 * area51
 * base 
 * CentOS
 * core
 * ganglia
 * hpc
 * kernel
 * python
 * sge
 * Updates-CentOS
 * kvm (Physical clusters only)
 * zfs (Physical clusters only)

(6.2: area51, base, ganglia, hpc, kernel, os, python, sge, webserver)

Other info:
 * Lawrence Geo:  N38.969  W95.245
 * Public Interface: assigned by Greg Smith for MAC address
   **Note** since we are not using DHCP, any MAC address assigned on creation is fine.
 * Private Interface:  (notyeti VMs: available internal 192.168.xxx.1, where
   xxx is the last quartet of the public IP address)
 * Gateway:  129.237.201.254 (Dyche 129.237.183.126)
 * DNS:  129.237.133.1
 * NTP server:  time.ku.edu
 * Auto-Partitioning


Enable www access
~~~~~~~~~~~~~~~~~
Follow procedure at http://central-7-0-x86-64.rocksclusters.org/roll-documentation/base/7.0/enable-www.html

Insert compute nodes
~~~~~~~~~~~~~~~~~~~~
Start insert-ethers process on the Frontend, then start each node.  Wait until 
each node has been recognized ( ) and accepted (*) in the insert-ethers
window before starting the next node.

For VM host cluster (w/o LM roll install)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Install screen before doing a security-update roll so it gets added to the 
distribution.  ::
    yumdownloader --resolve --enablerepo base screen.x86_64;
    rpm -i screen*.rpm

**************************
All KU-Lifemapper Clusters
**************************

Secure SSH
~~~~~~~~~~

**IFF** you do not have an SSH key, generate a private/public key for 
authentication (new ecdsa algorithm, 521 bit)::

    ssh-keygen -t ecdsa -b 521 -f .ssh/zeppobarks_ecdsa  -C "zeppobarks@gmail.com"
    
**IFF** you want to ssh from this machine to others, start the ssh agent, add
your private key to it, then copy your public key to the servers you want to access

Add public key to new (or existing) virtual frontend for key-based 
authentication from machines with your private key.  Make sure password 
authentication is still enabled (disabled with next step) for sshd before 
sending the key, or permission will be denied.::

    ssh-copy-id -i ~/.ssh/id_rsa.pub root@xxx.xxx.xxx.xxx

Turn off password authentication by editing the values in /etc/ssh/sshd_config::

    PasswordAuthentication no
    ChallengeResponseAuthentication no
    UsePAM no 
    
Then restart the sshd service::

    service sshd restart

Security updates
~~~~~~~~~~~~~~~~

Follow instructions at 
http://www.rocksclusters.org/new/2018/2018/01/04/updates-meltdown-spectre.html
Create a mirror with CentOS updates, using a nearby mirror from 
https://www.centos.org/download/mirrors/.  
**Note**: Make sure the URL constructed in "rocks create mirror" command points 
to an active update site. This command should bring back a variety of updates 
while creating the mirror.  The site constructed below differs from the URL in
the above instructions.
**Note**: Make sure HTTP is enabled.::

    # baseurl=http://centos.gbeservers.com/
    # osversion=7.4.1708
    # version=`date +%F`
    # rocks create mirror ${baseurl}/${osversion}/updates/x86_64/Packages/ rollname=Updates-CentOS-${osversion} version=${version}
    # rocks add roll Updates-CentOS-${osversion}-${version}*iso
    # rocks enable roll Updates-CentOS-${osversion} version=${version}
    # (cd /export/rocks/install; rocks create distro)
    # yum clean all; yum update
    
KU Production roll (unfinished)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Install the KU Production (kuprod) roll. Download iso and sha files, current
version is: 
* http://svc.lifemapper.org/dl/kuprod-1.0-0.x86_64.disk1.iso
