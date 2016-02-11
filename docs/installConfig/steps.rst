########################
Lifemapper install steps
########################
The following details how to install both Lifemapper rolls, LmServer and 
LmCompute, on a single physical or virtual cluster.  Some steps may be 
unnecessary, or already complete, for your instance.

*******************
Initial Rocks Setup
*******************
#. Install and configure: 
   https://github.com/lifemapper/core/blob/master/docs/installConfig/install.rst
#. Optional troubleshooting steps for virtual clusters:  
   https://github.com/lifemapper/core/blob/master/docs/installConfig/virtualCluster.rst


**********************
LmCompute Roll Install
**********************
It is easiest to install LmCompute first, until some conflicts are resolved.

#. Add the roll to your cluster:  
   https://github.com/pragmagrid/lifemapper-compute/tree/kutest#adding-a-roll-to-a-live-frontend
#. **FIXME** Replace the following file with updated version on the frontend
   and on the nodes::
        
        wget https://raw.githubusercontent.com/pragmagrid/lifemapper-server/master/src/lmserver/patch-files/core-1.0.3.lw/LmCommon/common/apiquery.py
        /bin/cp -fp apiquery.py /opt/lifemapper/LmCommon/common/apiquery.py
        /bin/cp -fp apiquery.py /share/lm/temp
        rocks run host compute "/bin/cp -fp /share/lm/temp/apiquery.py /opt/lifemapper/LmCommon/common/apiquery.py"
        
#. **FIXME** Copy the /opt/lifemapper/config/config.ini and save it for updating
   the version installed by the LmServer roll.        

*********************
LmServer Roll Install
*********************
#. Add the roll to your cluster with the following, but delay the reboot:
   https://github.com/pragmagrid/lifemapper-server/tree/kutest#adding-a-roll-to-a-live-frontend
#. **FIXME** Before rebooting, replace the file with updated version::

        wget https://raw.githubusercontent.com/pragmagrid/lifemapper-server/master/src/rocks-lifemapper/confDbconnect
        cp confDbconnect /opt/lifemapper/rocks/bin/confDbconnect

#. **FIXME** Before rebooting, replace the following file with updated version::

        wget https://raw.githubusercontent.com/lifemapper/core/master/LmDbServer/dbsetup/createMALExtras.sql
        cp createMALExtras.sql /opt/lifemapper/LmDbServer/dbsetup/

#. Reboot as directed in the instructions
#. Configure, populate, test :
   https://github.com/pragmagrid/lifemapper-server/blob/master/docs/Using.rst
#. **FIXME** Edit the newly installed config/config.ini file to fill in the 
   LmCompute  @variables@ in the new LmServer version.  You can find the values  
   for these variables in the version of config.ini that you saved from the  
   LmCompute installation.  This may be only 2 variables: @JOB_SUBMITTER_TYPE@  
   and @JOB_CAPACITY@.
 
**********************
Finish LmCompute Setup
**********************
#. Configure and test: 
   https://github.com/pragmagrid/lifemapper-compute/tree/kutest#using-a-roll

****************
Using Lifemapper
****************
#. https://github.com/lifemapper/core/blob/master/docs/using/starting.rst

