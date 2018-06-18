######
Notes:
######

********************
Fresh Rocks Install:
********************

Start/restart a virtual cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Make sure that the boot action is "os", configure cdrom to the kernel roll on 
the host system, then start or restart an existing cluster.  Open the 
"virt-manager" application in order to interact with the installation 
user-interface:: 

 * rocks set host boot <vm-name> action=os
 * (opt) rocks run host <vm-name> "shutdown -h now"
 * rocks start host vm <vm-name>
 * virt-manager

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
   * Fill out only public interface, 
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

* Two services often fail after install: named and httpd.  Problem is the 
  directories to hold PID files were not created.  
  * named: Missing dir /run/named failed to create because 'named' user 
    initiates the mkdir but does not have permission to create in /var/run.  
    Soln: Create dir with root and chown it.
  * httpd: Missing dir /run/httpd symlinked in /etc/httpd/
    Soln: Create dir /var/run/httpd.
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


history:
* Check DNS
 1012  ping www.ucsd.edu
 1013  cat /var/log/messages | grep DHCP
 1014  ping 192.168.131.252
 1015  ssh 192.168.131.252
 1017  rocks list host interface | grep 192.168.131.252

* Disable subnet manager opensm for InfiniBand
 1018  tail -n50 /var/log/messages
 1019  systemctl stop opensm
 1020  systemctl disable opensm

* See who (VMs) has accessed notyeti via http
 1021  grep rockscommand /var/log/messages
 1022  cd /var/log/httpd/
 1023  ll
 1024  tail access_log
 
 * Try to start httpd, figure out why failed
 1025  systemctl status httpd
 1026  systemctl stop httpd
 1027  systemctl start httpd
 1028  journalctl -xe
 1029  ll
 
 * grep process table for httpd
 1030  pgrep httpd
 1031  rocks list network
 1032  ip route show
 1033  systemctl status httpd
 1034  systemctl start httpd
 1035  cd /etc/httpd/
 1036  ll
 1037  ls /run
 
 * Missing directory, should have been created by systemd
 1038  mkdir /run/httpd
 1039  systemctl start httpd
 1040  systemctl status httpd
 1041  systemctl status named

* insert-ethers will fail if httpd is not running
 1042  insert-ethers
 1043  ~
 1044  systemctl start named
 1045  systemctl status named
 1046  systemctl stop httpd
 1047  insert-ethers
 1048  systemctl start httpd
 1049  insert-ethers

* Install Vclusters with bootaction=os and cdrom pointing to kernel roll file on notyeti
 1051  rocks list host boot
 1053  rocks set host boot notyeti-191 action=os
 1057  rocks set host vm cdrom notyeti-191 cdrom=/tank/data/rolls/kernel-7.0-0.x86_64.disk1.iso
 1058  rocks report host vm config notyeti-191
 1059  rocks list host vm status=1
 1060  rocks start host vm notyeti-191
 
* Clear cdrom before next boot
* make sure to "stop", then "start" vm after install
 1022  rocks set host vm cdrom notyeti-191 cdrom=None
 1023  rocks report host vm config notyeti-191 
 
* Check rocksdb 
 1024  systemctl status
 1025  systemctl status foundation-mysql
  
* Watch journal, live updating
    1  journalctl -xf

* httpd is not up
    2  systemctl status httpd
    3  systemctl restart httpd
   12  systemctl status httpd 
   14  mkdir /run/httpd
   15  systemctl start httpd 

* Disable unnecessary opensm, subnet manager for InfiniBand
    4  systemctl status opensm
    5  systemctl stop opensm
    6  systemctl disable opensm

* Note broken link to /run/httpd directory
    8  ll /etc/httpd/

* Add missing /var/run/named directory (journal showed mkdir failed, fix permissions for named user)
   11  mkdir /run/named
   16  systemctl status named
   17  systemctl start named
   18  systemctl status named
   19  chown -R named:named /run/named
   20  systemctl status named
   21  systemctl stop named
   22  systemctl start named
   
* Check other critical services, then reboot
   23  systemctl status dhcpd
   24  systemctl status foundation-mysql.service 
   25  shutdown -r now


* Also did not start on reboot
   72  systemctl  status zfs-import-scan.service 
   73  systemctl  start zfs-import-scan.service 
   74  systemctl  status zfs-import-scan.service 
   75  journalctl -xe

* VM Container did not boot with kickstart file, what's in them
   81  ls -lahtr /tftpboot/pxelinux/pxelinux.cfg/
   82  more /tftpboot/pxelinux/pxelinux.cfg/default 


* Is there a problem with MTU=1500?  No
   93  ping 192.168.131.1 -s 1500
   94  ping 192.168.131.1 -s 1800
   95  ping 192.168.131.254 -s 1800
   96  ping 192.168.131.254 
   97  ping 192.168.131.254 -s 1500
   98  ping 192.168.131.254 -s 1472
   99  ping 192.168.131.254 -s 1500

* Install tftp client for testing connection
  156  yum install tftp
  159  tftp --help
  160  tftp 192.168.131.1

* Look at messages again
  179  grep rockscommand /var/log/messages 
  
* Install vncserver
  186  yum install tigervnc-server
  187  vncserver :20

* Also did not start on reboot
  189  rocks run host uptime collate=yes
  190  rocks list host partition

* Why do attached machines not get kickstart file on host insert-ethers?
Value is retrieved from attribute Kickstart_PrivateKickstartCGI
  set on install.  Solution: fix it
* Checkout PXE boot configuration, all configurations had rocks-ks=em2 instead of cgi script
   81  ls -lahtr /tftpboot/pxelinux/pxelinux.cfg/
   82  more /tftpboot/pxelinux/pxelinux.cfg/default 
* rocks-ks was set to https://192.168.131.1/install/em2 instead of the cgi script.  
  135  tcpdump -v tcpdump -n -i eth0 port 69
  151  rocks list attr | grep CGI
  152  rocks set attr Kickstart_PrivateKickstartCGI sbin/kickstart.cgi
  153  rocks list attr | grep CGI

* Fix pxe boot config file generation, then start em up
  154  cd /export/rocks/install/rocks-dist/x86_64/build/nodes/
  155  cat core-pxe.xml | rocks report post attrs="$(rocks report host attr localhost pydict=true)" > output.txt
  156  vim output.txt 
  157  bash output.txt 
  158  insert-ethers 
  
* NAS install should be headless
  159  rocks set host installaction nas-0-0 action="install headless"
  160  rocks list host nas-0-0
  161  rocks set host boot nas-0-0 action=install
  162  ssh nas-0-0
  