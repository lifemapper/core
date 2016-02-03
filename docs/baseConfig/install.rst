# Notes:
## Fresh Rocks Install:

Install the rolls:
* area51
* base 
* ganglia
* hpc
* java
* kernel
* kvm (on physical devel server)
* os
* python
* sge (LmCompute)
* webserver (LmServer)
* zfs (on physical devel server)

## Update Python Roll

**If** the sqlite3 module is not available in python2.7:

  * Get an updated python roll from pc-167.calit2.optiputer.net in /root/roll/ with sqlite3.so
  * Copy the iso, then remove old rpm, add roll making sure it will override existing:

        rpm -el opt-python-27-2.7.9-1.x86_64
        rocks add roll python-6.2….iso clean=1
        rocks enable roll python
        (cd /export/rocks/install; rocks create distro)
        yum clean all
        rocks run roll python > add-roll-python.sh
        bash add-roll-python.sh > add-roll-python.out 2>&1
