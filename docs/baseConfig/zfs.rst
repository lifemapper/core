## Procedure on Yeti [slight differences on NotYeti]:
#### First build ZFS binary roll and install

     cd /opt/zfs/zfs-linux-roll-source/
     make binary-roll
     rocks add roll bfs-linux-*disk1.iso
     rocks enable roll zfs-linux
     (cd /export/rocks/install/; rocks create distro)
     rocks run roll zfs-linux > add-zfs.sh
     bash add-bfs.sh 2>&1 | tee add-bfs.out

#### Create ZFS pool from your disk

     zpool create -f tank /dev/sdb
     [zpool create -f tank /dev/sdb /dev/sdc]
     zpool list

#### Create volumes, slices, etc

     zfs create tank/lmcompute
     zfs set sharenfs=on tank/lmcompute   

#### Updated /etc/auto.master

     /share /etc/auto.share --timeout=1200
     /home  /etc/auto.home  --timeout=1200
     /data  /etc/auto.data  --timeout=1200

#### Added /etc/auto.data

     lmcompute yeti.local:/tank/lmcompute

#### Push /etc/auto.* to nodes and restart autofs
 
     rocks sync users
     service autofs restart
     rocks run host compute 'service autofs restart'
     [rocks run host vm-container 'service autofs restart']

#### Edit sharenfs attribute:

Previously used command "zfs set sharenfs=rw=@<private ip prefix>.0/24,sec=sys tank/lmcompute", but 
sharing did not work correctly without adding /etc/exportfs line and calling "exportfs -ra"

     zfs set sharenfs=rw=@<private ip prefix>.0/24,no_root_squash,async sec=sys tank/lmcompute

#### in /var/411 remake the maps and then force getting new files on compute nodes, plus resetart autofs

     make clean
     make force
     rocks sync users
     rocks run host compute "411get --all; service autofs restart"
     [rocks run host vm-container "411get --all; service autofs restart"]

#### [This may be unnecessary] add line to /etc/exportfs for filesystem(s) to export, then re-export manually 

Check with Nadya about this, but /var/log/messages showed "Jan 13 14:35:50 notyeti rpc.mountd[3418]: 
refused mount request from 192.168.202.253 for /tank/vms (/): no export entry".  This may have just
fixed the original incomplete sharenfs setting above (w/o "no_root_squash,async

     /tank/lmcompute <private ip prefix>.1(rw,async,no_root_squash) <private ip prefix>.0/255.255.255.0(rw,async)
     exportfs -ra

## Make sure to add to roll build anything that is not Yeti-specific


