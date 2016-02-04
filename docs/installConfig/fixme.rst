-----------------------------------
Current problems with roll installs
-----------------------------------
For lifemapper-compute roll, substitute Github latest for installed files::
        
        wget https://raw.githubusercontent.com/pragmagrid/lifemapper-server/master/src/lmserver/patch-files/core-1.0.3.lw/LmCommon/common/apiquery.py
        cp apiquery.py /opt/lifemapper/LmCommon/common/apiquery.py

For lifemapper-server roll: substitute Github latest for installed files::

        wget https://raw.githubusercontent.com/pragmagrid/lifemapper-server/master/src/rocks-lifemapper/confDbconnect
        wget https://raw.githubusercontent.com/lifemapper/core/master/LmDbServer/dbsetup/createMALExtras.sql
        cp createMALExtras.sql /opt/lifemapper/LmDbServer/dbsetup/
        cp confDbconnect /opt/lifemapper/rocks/bin/confDbconnect
        
When installing both rolls on a single cluster, stash the config.ini installed
first, then use the replaced values in the stashed version to fill in @variables@ 
in the new version.  It is easier to install the LmCompute roll first, then 
LmServer as the LmServer roll has more variables.

After installing LmCompute:: 

    cp -p /opt/lifemapper/config/config.ini /tmp/
    
After installing LmServer, edit the latest config.ini and search for @
     
----------------------------------------
Current problems with code/configuration
----------------------------------------
#. Signup page on LM public site
#. Identify best way to handle compute local workspace/ JOB_OUTPUT_PATH
#. Clear off old install from Yeti nodes (old data is on /state/partition1; 
   delete all but kvm and lost+found)
#. Separate configuration files by roll, to avoid conflicts
#. Change pipeline into a service and/or lm command

