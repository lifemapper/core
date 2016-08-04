######
Notes:
######

********************
Fresh Rocks Install:
********************

Install the rolls:
~~~~~~~~~~~~~~~~~~

Physical and Virtual clusters::
  
  * area51
  * base 
  * ganglia
  * hpc
  * kernel
  * os
  * python
  * sge (LmCompute)
  * webserver (LmServer)

Physical clusters::
  
  * kvm
  * zfs

Lawrence Geo:  N38.969  W95.245
Public Interface: assigned by Greg for MAC address
Private Interface:  (notyeti VMs: available internal 192.168.202.x on notyeti)
Gateway:  129.237.201.254
DNS:  129.237.133.1
Auto-Partitioning

Update Python Roll
~~~~~~~~~~~~~~~~~~
**If** the sqlite3 module is not available in python2.7:

* Get an updated python roll from pc-167.calit2.optiputer.net in /root/roll/ with sqlite3.so
* Copy the iso, then remove old rpm, add roll making sure it will override existing::

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

**Until the KUBI roll is ready** install the ku_security_updates script into 
the /etc/cron.daily/ directory. The script is at:

    wget https://raw.githubusercontent.com/lifemapper/kuprod/master/src/kuprod/ku_security_updates
    install 700 ku_security_updates /etc/cron.daily/
