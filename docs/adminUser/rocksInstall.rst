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

 * rocks set host boot notyeti-192 action=os
 * rocks set host vm cdrom notyeti-192 cdrom=/tank/data/rolls/kernel-7.0-0.x86_64.disk1.iso
 * rocks report host vm config notyeti-192
 * (opt) rocks run host notyeti-192 "shutdown -h now"
 * rocks start host vm notyeti-192
 * virt-manager

If installing 6.2, boot with Rocks 6.2 kernel iso in the CDROM, but choose 
URL for installation image, then point to ::
  
  http://central-6-2-x86-64.rocksclusters.org/install/rolls/
  
Steps for Rocks 7
~~~~~~~~~~~~~~~~~~
1. Docs at http://central-7-0-x86-64.rocksclusters.org/roll-documentation/base/7.0/install-frontend-7.html

1. Choose English/US Keyboard
1. Installation summary
   1. Localization: Choose Date/Time zone
   1. System: 
      1. Fill out FQDN
      1. Network and Hostname: Configure ONLY eth1 (public)
      1. IPv4 tab
         * Manual 
         * Address:  129.237.201.xxx, Netmask: 255.255.255.0, Gateway: 129.237.201.254 (Dyche 129.237.183.126)
         * DNS:  129.237.133.1, 129.237.32.1
      1. IPv6 tab
         * Link-local only
   1. Rocks Cluster Config
      1. Cluster Private Network
         1. IPv4 address: 192.168.xxx.1
   1. Cluster Configuration
      1. Add/edit fields
      1. Lawrence Geo:  N38.969  W95.245
      1. NTP server:  time.ku.edu
   1. Rolls selection:
      1. Change roll source to URL for host machine to speed download time.
      1. For all clusters, install the following (Rocks 7.0):
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
      1. For physical clusters add:
         * kvm
         * zfs
      1. Rocks6.2: area51, base, ganglia, hpc, kernel, os, python, sge, webserver
   1. Installation Destination
      1. Manual partitioning
      1. Standard partitioning, Not LVM, auto-create
      1. Change /home to  /state/partition1, make sure > 10gb    
    1. Begin installation
    1. User config
       1. Set up root password
       1. Do NOT set up user
    1. When completed and ready to reboot, reset cdrom to None
       * rocks set host vm cdrom <vm-name> cdrom=None
       * rocks report host vm config <vm-name> 




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
      
   * Connect using VNCViewer on laptop

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

Troubleshooting
~~~~~~~~~~~~~~~~
#. If nodes get stuck on re-install, check out tips at:: 
    https://lists.sdsc.edu/pipermail/npaci-rocks-discussion/2019-July/073004.html

    
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
    # yum clean all; yum update >> update.${version}.log
    

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


  