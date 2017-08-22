# Find installed rpms and users in
# /export/rocks/install/rocks-dist/x86_64/build/nodes/java*.xml
  
# Copy files to /root/ as a backup
   
# remove RPMs
RM="rpm -evl --quiet --nodeps"
$RM jdk
$RM antlr
$RM jogl
$RM java3d
$RM jboss
$RM apache-tomcat
$RM tomcat-connectors

$RM SUNWj6cfg
$RM SUNWj6dev
$RM SUNWj6dvx
$RM SUNWj6man
$RM SUNWj6rt
$RM SUNWj6rtx
$RM SUNWjavadb-client
$RM SUNWjavadb-common
$RM SUNWjavadb-core
$RM ROCKSantlr

$RM cruisecontrol
$RM apache-maven
$RM eclipse
        
# Users
userdel jboss
groupdel jboss
/bin/rm -f /var/spool/mail/jboss
userdel tomcat
groupdel tomcat
/bin/rm -f /var/spool/mail/tomcat
rocks sync users

# mod_jk configuration
rm -f /etc/httpd/conf.d/mod_jk.conf
rm -f /etc/httpd/modules/modules/mod_jk.so
rm -f /etc/httpd/conf/workers.properties
rm -f /var/log/httpd/mod_jk.log
rm -f /etc/httpd/conf/workers.properties

# Remove roll
rocks remove roll java
(cd /export/rocks/install; rocks create distro; yum clean all)
