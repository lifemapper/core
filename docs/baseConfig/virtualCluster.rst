## All KU-Lifemapper Virtual Clusters
Add public key to new virtual frontend for key-based authentication

    ssh-copy-id -i ~/.ssh/id_rsa.pub root@xxx.xxx.xxx.xxx

Turn off password authentication by editing the values in /etc/ssh/sshd_config, 
then restarting the sshd service:

    ChallengeResponseAuthentication no
    PasswordAuthentication no
    UsePAM no 

Open up http access http://yeti.lifemapper.org/roll-documentation/base/6.2/enable-www.html

## NotYeti-specifics

### Build VCs on NotYeti with SGE

Download from default Rocks server.  SGE was (mistakenly) not installed on NotYeti.

### Defer checksum from NIC to kernel  

From Nadya:  This will need to be added to all virtual frontends  that are built on juno and may be on
other  hosts that have similar NIC to juno (i.e. NotYeti)

Execute the following command on a new virtual frontend:

    ethtool -K eth0 tx off rx off 

Add to /etc/rc.local

    # This commands resets the checksum that is done on the NIC (defers 
    # it to kernel). When checksum is done on the NIC it introduces 
    # delays in the response communication between frontend and compute 
    # node and building of compute node fails.
    ethtool -K eth0 tx off rx off
