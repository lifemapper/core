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
Install utilities before doing a security-update roll so it gets added to the 
new distribution.  ::

* Screen
   Install screen from yum.  ::
    yumdownloader --resolve --enablerepo base screen.x86_64;
    rpm -i screen*.rpm

* VNCServer
   * Install VNCServer to use virt-manager and other graphic interfaces remotely. 
    yum install tigervnc-server
    
   * Start/Stop on server::
      vncserver :20 -geometry 1280x1024
      vncserver -kill :20

* (NO) VNCServer procedure not currently working, using instructions 
  at https://www.tecmint.com/install-and-configure-vnc-server-in-centos-7/::
    cp /lib/systemd/system/vncserver@.service  /etc/systemd/system/vncserver@:20.service
            
   * Edit config file
     * Add USER
     * Add "-geometry 1280x1024" to ExecStart command
     
   * Reload system config to pick up new config file
        # systemctl daemon-reload
        # systemctl start vncserver@:20
        # systemctl status vncserver@:20
        # systemctl enable vncserver@:20

    
    
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
    
    
Troubleshooting
~~~~~~~~~~~~~~~
* SGE causes sync config to fail:
  After adding the switch, the command "rocks sync config" failed.  After some 
  searching, I tried starting the SGE service, and everything worked fine::

    root@notyeti root]# rocks sync config
    error: commlib error: got select error (Connection refused)
    unable to send message to qmaster using port 536 on host "notyeti.local": got send error
    [root@notyeti root]# /etc/init.d/sgemaster.notyeti start
    Starting Grid Engine qmaster
    [root@notyeti root]# rocks sync config
    [root@notyeti root]# 

* Enabling www access failed 
  http://central-7-0-x86-64.rocksclusters.org/roll-documentation/base/7.0/enable-www.html 
    * Failed on "rocks sync host firewall localhost” b/c iptables service was not 
      running and could not be reloaded (or started)
    * Rebooted - just in case
    * Everything came up fine and "rocks sync host firewall localhost” worked
    * The iptables/no web service access problem came up again when I tried to 
      update the next time. Errors seemed to point to the opensm service as well

* User creation
    * The user I created on install (astewart) was created on the system, but I 
      was unable to login to the GUI with that account.  I could ssh to it, 
      and it showed that no home directory had been created. Deleted the user, 
      and added it again at the command prompt.  It created the home directory, 
      and I can login through the GUI
      
* Mouse - Switching the primary mouse button from left to right did not work,
  but after a reboot several weeks later it magically did work

* Enabling Auto-partition caused the creation of LVM partitions on NotYeti.  
  The command "rocks list partition notyeti" did not recognize these partitions.

New repositories
~~~~~~~~~~~~~~~~
http://repository.it4i.cz/mirrors/repoforge/redhat/el7/en/x86_64/rpmforge/RPMS/rpmforge-release-0.5.3-1.el7.rf.x86_64.rpm

KU Production roll (unfinished)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Install the KU Production (kuprod) roll. Download iso and sha files, current
version is: 
* http://svc.lifemapper.org/dl/kuprod-1.0-0.x86_64.disk1.iso
