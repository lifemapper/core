#!/bin/bash
#
# Download and install latest security updates for CentOS/Rocks
# using Rocks roll at https://github.com/rocksclusters/security-updates.
# Move this script to the /etc/cron.daily directory

#
# ..............................................................................
# Do this manually the first time.  
# For libraries which x86_64 depends on the i686 version (or vice versa):
#   - yum update --skip-broken --exclude nss-softokn-freebl.i686 (installed all but both nss-softokn-freebl)
#   - rpm -qa | grep nss-softokn-freebl (showed both versions oldest)
# Update dependency first, then x86_64 version
#   - yum update nss-softokn-freebl.i686 --setopt protected_multilib=false
#   - yum --enablerepo=base,updates install nss-softokn-freebl.x86_64
# Finally, make sure everything is covered:
#   - yum update --skip-broken
# ..............................................................................
yum update --skip-broken --exclude bind-9.8.2-0.30.rc1.el6_6.2.x86_64 bind-utils-9.8.2-0.30.rc1.el6_6.2.x86_64 nfs-utils-1.2.3-54.el6.x86_64
# ....................................
usage () 
{
    echo "Usage: $0"
    echo "This script is run by the superuser or a cron job. It will "
    echo "     install all security updates identified by a newly updated "
    echo "     security-update roll. "
    echo "   "
    echo "The output of the script is in /tmp/`/bin/basename $0`.log"
}


# ....................................
### define varibles
# ....................................
SetDefaults () {
   PREVDIR=pwd
   WORKINGDIR=/state/partition1/site-roll/rocks/src/roll/security-updates
   TO="aimee.stewart@ku.edu"
   idx=`expr index $HOSTNAME .`
   BASEHOSTNAME=${HOSTNAME:0:idx-1}
   # insert $HOSTNAME 
   SUBJECT="Security updates from $BASEHOSTNAME"
   VERSION=$(date +%F | tr - _)
   echo Version  is $VERSION

   LOG=/tmp/`/bin/basename $0`.log
   rm $LOG
   touch $LOG
   TimeStamp "# Start"
   echo Creating new security update roll for $BASEHOSTNAME
}

# ....................................
### time
# ....................................
TimeStamp () {
    echo $1 `/bin/date` >> $LOG
}

# ....................................
### Make and install new roll
# ....................................
PrepareRoll() {
   cd $WORKINGDIR
   make roll
   rocks add roll os-6.2-security-updates-$VERSION-0.x86_64.disk1.iso
   rocks enable roll os-security-updates   
   (cd /export/rocks/install; rocks create distro)
   yum clean all
}

# ....................................
### email logfile output
# ....................................
SendEmailAndReturn () {
   TimeStamp "# End"
   /usr/sbin/sendmail -i -F $BASEHOSTNAME  "$TO"  < $LOG
   cd $PREVDIR
}

# ....................................
### Apply the new updates
# ....................................
ApplyUpdates () {
   yum check-update  | tee -a $LOG
   yum update | tee -a $LOG
}

# ..............................................................................
### Main ###
# ..............................................................................
if [ $# -ne 0 ]; then
    usage
    exit 0
fi 

SetDefaults
PrepareRoll
ApplyUpdates
SendEmailAndReturn

