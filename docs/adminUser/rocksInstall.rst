######
Notes:
######

********************
Fresh Rocks Install:
********************

Yum errors
~~~~~~~~~~~
* Opt python module "breaks" yum, must unload before using Yum commands::
     module load opt-python
     do something
     module unload opt-python
     yum something 
   
Need VNC for admin
~~~~~~~~~~~~~~~~~~  
* Install and start vncserver
  yum install tigervnc-server
  vncserver :20


Start/restart a virtual cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Make sure that the boot action is "os", configure cdrom to the kernel roll on 
the host system, then start or restart an existing cluster.  Open the 
"virt-manager" application in order to interact with the installation 
user-interface:: 

 * rocks set host boot <vm-name> action=os
 * rocks set host vm cdrom <vm-name> cdrom=<kernel iso>
 * rocks report host vm config <vm-name>
 * (opt) rocks run host <vm-name> "shutdown -h now"
 * rocks start host vm <vm-name>
 * virt-manager

If installing 6.2, boot with Rocks 6.2 kernel iso in the CDROM, but choose 
URL for installation image, then point to ::
  
  http://central-6-2-x86-64.rocksclusters.org/install/rolls/
  
Install the rolls:
~~~~~~~~~~~~~~~~~~

All rolls should come from the vm host machine (notyeti) to speed download time.
  
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

Configuration params/screens:
 * **Date/Time**
   * NTP server:  time.ku.edu
 * On **Network and Hostname**:
   * Fill out FQDN
   * Fill out only public interface, eth1, 
   * Method = Manual
   * IPv4 
     * assigned by Greg Smith for MAC address
     * Netmask: 255.255.255.0
     * Gateway:  129.237.201.254 (Dyche 129.237.183.126)
     * DNS:  129.237.133.1, 129.237.32.1
     * Search domain: (optional) lifemapper.org
   * IPv6 - link-local only
 * On **Cluster Private Network**  
   * Private Interface:  (notyeti VMs: available internal 192.168.xxx.1, where
     xxx is the last quartet of the public IP address)
 * **Cluster Config**
   * Lawrence Geo:  N38.969  W95.245
 * **Installation Destination**
   * Manual partitioning
   * Standard partitioning, Not LVM
   * > 10gb for /state/partition1



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
      
   * Tunnel ssh connection through local port (laptop)
      ssh -L 5920:localhost:5920 root@notyeti.lifemapper.org

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

Turn off password authentication by editing the values in /etc/ssh/sshd_config.
Note that "UsePAM no" is not supported by RHLinux::

    PasswordAuthentication no
    ChallengeResponseAuthentication no
    
Then restart the sshd service::

    service sshd restart
    
To add your ssh key to the ssh-agent on your local machine::
    eval "$(ssh-agent -s)"
    ssh-add ~/.ssh/id_rsa

To change eclipse to use ssh login with key::
    git remote set-url origin ssh://git@github.com/lifemapper/core.git
    git config user.email "aimee.stewart@ku.edu"
    git config user.name "zzeppozz"
    
Security updates ONLY for Rocks 7.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Follow instructions at 
http://www.rocksclusters.org/new/2018/2018/01/04/updates-meltdown-spectre.html
Create a mirror with CentOS updates, using a nearby mirror from 
https://www.centos.org/download/mirrors/.  
**Note**: Make sure the URL constructed in "rocks create mirror" command points 
to an active update site. This command should bring back a variety of updates 
while creating the mirror.  The site constructed below differs from the URL in
the above instructions.
**Note**: Make sure HTTP is enabled.::
    
    # baseurl=http://mirror.oss.ou.edu/centos
    # osversion=7
    # version=`date +%F`
    # rocks create mirror ${baseurl}/${osversion}/updates/x86_64/Packages/ rollname=Updates-CentOS-${osversion} version=${version}
    # rocks add roll Updates-CentOS-${osversion}-${version}*iso
    # rocks enable roll Updates-CentOS-${osversion} version=${version}
    # (cd /export/rocks/install; rocks create distro)
    # yum clean all; yum update
    

Rocks 7.0 tips
~~~~~~~~~~~~~~~~~~~~
* User creation
    * Don't
      
* Enabling Auto-partition causes LVM partitions - unsupported.  Use 
  standard partitions.

* Mouse - Switching the primary mouse button from left to right did not work,
  but after a reboot several weeks later it magically did work

Virtual clusters
~~~~~~~~~~~~~~~~~
* Install Vclusters with bootaction=os and cdrom pointing to kernel roll file on notyeti::

    1051  rocks list host boot
    1053  rocks set host boot notyeti-191 action=os
    1057  rocks set host vm cdrom notyeti-191 cdrom=/tank/data/rolls/kernel-7.0-0.x86_64.disk1.iso
    1058  rocks report host vm config notyeti-191
    1059  rocks list host vm status=1
    1060  rocks start host vm notyeti-191

* Clear cdrom before next boot
* make sure to "stop", then "start" vm after install::

    1022  rocks set host vm cdrom notyeti-191 cdrom=None
    1023  rocks report host vm config notyeti-191 
    
New repositories
~~~~~~~~~~~~~~~~
http://repository.it4i.cz/mirrors/repoforge/redhat/el7/en/x86_64/rpmforge/RPMS/rpmforge-release-0.5.3-1.el7.rf.x86_64.rpm

KU Production roll (unfinished)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Install the KU Production (kuprod) roll. Download iso and sha files, current
version is: 
* http://svc.lifemapper.org/dl/kuprod-1.0-0.x86_64.disk1.iso


  