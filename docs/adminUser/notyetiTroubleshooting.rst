################
Troubleshooting:
################

On FE
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
    
    * Soln: Create dir with root and chown it.
    
  * httpd: Missing dir /run/httpd symlinked in /etc/httpd/
  
    * Soln: Create dir /var/run/httpd.
    
* Enabling www access failed 
  http://central-7-0-x86-64.rocksclusters.org/roll-documentation/base/7.0/enable-www.html 
  
  * Failed on "rocks sync host firewall localhost” b/c iptables service was not 
    running and could not be reloaded (or started)
  * Rebooted - just in case
  * Everything came up fine and "rocks sync host firewall localhost” worked
  * The iptables/no web service access problem came up again when I tried to 
    update the next time. Errors seemed to point to the opensm service as well
      

On Compute Nodes/Dev Appliances 
~~~~~~~~~~~~~~~~~
User share and shared FS problems

#. Missing file /etc/411-security/shared.key on nodes. Copy 
   /etc/411-security/shared.key to all nodes::
   
   rocks sync host sharedkey compute

#. rpcbind keeps dying, leaving behind lock and pid files 
   (/var/run/rpcbind.lock, .pid)
   
#. Restart rpcbind with:: 

     service rpcbind restart
     service sec-channel restart
     service nfs restart
   
     cd /var/411
     make clean
     make force
     rocks sync users
     rocks run host compute "411get --all; service autofs restart"   

#. If lmwriter home dir is still unavailable, check status of rpcbind on nodes, restart if dead.

#. Test by logging into nodes as lmwriter user, home dir should be available with
   lmwriter r/w permissions

On Development Appliance
~~~~~~~~~~~~~~~~~~~~~~~~
* Rocks-7.0 repo was disabled, "yum repolist all" produced error 
  "Unable to open file $DISTRODIR/install/rocks-dis/x86_64"
  
  * Solved by editing /etc/yum.repos.d/rocks-local.repo to replace $DISTRODIR 
    with URL to host machine baseurl=http://129.237.201.131/install/rocks-dist/x86_64

* Repositories and RPMs unavailable for CentOS7

  * PGDG repo, Postgresql 9.1 libraries, PostGIS
  * HDF5 rpms
  
  
  
Try Me
~~~~~~
* Notyeti throws me off ssh when filling out install screen for VMs

  * check services are running, httpd, named, rocks-kvm-vlan, random number generator
      systemctl list-units
  * check status of dmesg
  * /var/log/secure
  * /var/log/messages
  * /var/log/fail2ban
  * directory listing through browser
  * time on machine - 
  * rocks list host attr | grep Timezone

On Development Appliance
~~~~~~~~~~~~~~~~~~~~~~~~
* Interrupt-remapping with bad chipset, workaround with KVM

  * Persist across reboots: https://wiki.debian.org/VGAPassthrough#Unsafe_interrupts_remapping: "If your 
    hardware doesn't support remapping of interruptions, you have to 
    enable the unsafe assignments. Create /etc/modprobe.d/kvm_iommu.conf with::
     options kvm allow_unsafe_assigned_interrupts=1
     
  * One-time only? https://gist.github.com/lisovy/1f737b1db2af55a153ea: run::
     echo 1 > /sys/module/kvm/parameters/allow_unsafe_assigned_interrupts
     
  * Redhat bug:  https://bugzilla.redhat.com/show_bug.cgi?id=715555 
    references both methods


Virtual cluster
~~~~~~~~~~~~~~~
* Error, missing boot files for vms::

    Mar 31 16:40:02 notyeti.lifemapper.org libvirtd[5594]: 
    2018-03-31 21:40:02.506+0000: 5601: error : 
    virSecurityDACSetOwnership:632 : 
    unable to stat: /boot/kickstart/default/initrd.img-7.0-x86_64: 
    No such file or directory

* Copied files from PXE boot location to other boot location::

   cp -p /tftpboot/pxelinux/vmlinuz-7.0-x86_64 /boot/kickstart/default/
   cp -p /tftpboot/pxelinux/initrd.img-7.0-x86_64 /boot/kickstart/default/


history:
-------- 

* Check DNS::

    1012  ping www.ucsd.edu
    1013  cat /var/log/messages | grep DHCP
    1014  ping 192.168.131.252
    1015  ssh 192.168.131.252
    1017  rocks list host interface | grep 192.168.131.252

* Disable subnet manager opensm for InfiniBand::

    1018  tail -n50 /var/log/messages
    1019  systemctl stop opensm
    1020  systemctl disable opensm

* See who (VMs) has accessed notyeti via http::

    1021  grep rockscommand /var/log/messages
    1022  cd /var/log/httpd/
    1023  ll
    1024  tail access_log
 
* Try to start httpd, figure out why failed::

    1025  systemctl status httpd
    1026  systemctl stop httpd
    1027  systemctl start httpd
    1028  journalctl -xe
 
* grep process table for httpd::

    1030  pgrep httpd
    1031  rocks list network
    1032  ip route show
    1033  systemctl status httpd
    1034  systemctl start httpd
    1035  cd /etc/httpd/
    1036  ll
    1037  ls /run
 
* Missing directories, should have been created by systemd
* Services fail
* insert-ethers will fail if httpd is not running::

    1038  mkdir /run/httpd
    1039  systemctl start httpd
    1040  systemctl status httpd
    1041  systemctl status named
    1042  insert-ethers
    1043  ~
    1044  systemctl start named
    1045  systemctl status named
    1046  systemctl stop httpd
    1047  insert-ethers
    1048  systemctl start httpd
    1049  insert-ethers
 
* Check rocksdb::

    1024  systemctl status
    1025  systemctl status foundation-mysql
  
* Watch journal, live updating::

    1  journalctl -xf

* httpd is not up::

    2  systemctl status httpd
    3  systemctl restart httpd
    12  systemctl status httpd 
    14  mkdir /run/httpd
    15  systemctl start httpd 

* Note broken link to /run/httpd directory::

    8  ll /etc/httpd/

* Add missing /var/run/named directory (journal showed mkdir failed, 
  fix permissions for named user)::
  
   11  mkdir /run/named
   16  systemctl status named
   17  systemctl start named
   18  systemctl status named
   19  chown -R named:named /run/named
   20  systemctl status named
   21  systemctl stop named
   22  systemctl start named
   
* Check other critical services, then reboot::

   23  systemctl status dhcpd
   24  systemctl status foundation-mysql.service 
   25  shutdown -r now

* Also did not start on reboot::

   72  systemctl  status zfs-import-scan.service 
   73  systemctl  start zfs-import-scan.service 
   74  systemctl  status zfs-import-scan.service 
   75  journalctl -xe

* VM Container did not boot with kickstart file, what's in them::

   81  ls -lahtr /tftpboot/pxelinux/pxelinux.cfg/
   82  more /tftpboot/pxelinux/pxelinux.cfg/default 


* Look at messages again::

    179  grep rockscommand /var/log/messages 
  
* Also did not start on reboot::

   189  rocks run host uptime collate=yes
   190  rocks list host partition

* Why do attached machines not get kickstart file on host insert-ethers?:
   * Value is retrieved from attribute Kickstart_PrivateKickstartCGI, set on install.  
   * Solution: fix it with "rocks set attr ..."
  
* Checkout PXE boot configuration, all configurations had rocks-ks=em2 instead of cgi script::

   81  ls -lahtr /tftpboot/pxelinux/pxelinux.cfg/
   82  more /tftpboot/pxelinux/pxelinux.cfg/default 
   
* rocks-ks was set to https://192.168.131.1/install/em2 instead of the cgi script::

   135  tcpdump -v tcpdump -n -i eth0 port 69
   151  rocks list attr | grep CGI
   152  rocks set attr Kickstart_PrivateKickstartCGI sbin/kickstart.cgi
   153  rocks list attr | grep CGI

* Fix pxe boot config file generation, then start em up::

   154  cd /export/rocks/install/rocks-dist/x86_64/build/nodes/
   155  cat core-pxe.xml | rocks report post attrs="$(rocks report host attr localhost pydict=true)" > output.txt
   156  vim output.txt 
   157  bash output.txt 
   158  insert-ethers 
  
* NAS install should be headless::

   159  rocks set host installaction nas-0-0 action="install headless"
   160  rocks list host nas-0-0
   161  rocks set host boot nas-0-0 action=install
   162  ssh nas-0-0
