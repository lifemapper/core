1. Signup page on LM public site
1. Identify best way to handle compute local workspace/ JOB_OUTPUT_PATH
1. Clear off old install from Yeti nodes (old data is on /state/partition1; delete all but kvm and lost+found)

For lifemapper-server roll: substitute Github latest for installed files:

        wget https://raw.githubusercontent.com/pragmagrid/lifemapper-server/master/src/rocks-lifemapper/confDbconnect
        wget https://raw.githubusercontent.com/lifemapper/core/master/LmDbServer/dbsetup/createMALExtras.sql
        cp createMALExtras.sql /opt/lifemapper/LmDbServer/dbsetup/
        cp apiquery.py /opt/lifemapper/LmCommon/common/apiquery.py

For lifemapper-compute roll, substitute Github latest for installed files:
        
        wget https://raw.githubusercontent.com/pragmagrid/lifemapper-server/master/src/lmserver/patch-files/core-1.0.3.lw/LmCommon/common/apiquery.py
        cp confDbconnect /opt/lifemapper/rocks/bin/confDbconnect