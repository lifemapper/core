################
Troubleshooting:
################

YUM errors:
***********

On Development Appliance
~~~~~~~~~~~~~~~~~~~~~~~~
* Rocks-7.0 repo was disabled, "yum repolist all" produced error 
  "Unable to open file $DISTRODIR/install/rocks-dis/x86_64"
  * Solved by editing /etc/yum.repos.d/rocks-local.repo to replace $DISTRODIR 
    with URL to host machine baseurl=http://129.237.201.131/install/rocks-dist/x86_64

* Repositories and RPMs unavailable for CentOS7
  * PGDG repo, Postgresql 9.1 libraries, PostGIS
  * HDF4 and HDF5 rpms
  
* RPMForge repository included in installation points to defunct RPMForge repo
  * http://apt.sw.be/redhat/el6/en/x86_64/rpmforge/repodata/repomd.xml
  * Download latest from rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm
  * Get rpmforge from new location, edit name, disable by default::
     # RPMFORGEREPO=rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm
     # wget http://ftp.tu-chemnitz.de/pub/linux/dag/redhat/el6/en/x86_64/rpmforge/RPMS/$RPMFORGEREPO
     # rpm -Uvh rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm 
  * Save old repo::
     ### rpmforge.repo still shows old url
     # mv /etc/yum.repos.d/rpmforge.repo /etc/yum.repos.d/rpmforge.repo.save
  * Add repo from url, copy to rpmforge.repo, disable original and newly named version::
     # yum-config-manager --add-repo http://ftp.tu-chemnitz.de/pub/linux/dag/redhat/el6/en/x86_64/rpmforge
     ### disable
     # vim /etc/yum.repos.d/ftp.tu-chemnitz.de_pub_linux_dag_redhat_el6_en_x86_64_rpmforge.repo
     # cat /etc/yum.repos.d/ftp.tu-chemnitz.de_pub_linux_dag_redhat_el6_en_x86_64_rpmforge.repo > /etc/yum.repos.d/rpmforge.repo
     ### change [name], disable
     # vim /etc/yum.repos.d/rpmforge.repo
  * Possible missing dependencies for HDF4 according to 
    https://centos.pkgs.org/6/repoforge-x86_64/hdf4-4.2.6-1.el6.rf.x86_64.rpm.html:
    * libmfhdf.so.0()(64bit)
    * rtld(GNU_HASH)
      

Building a roll
~~~~~~~~~~~~~~~~~~
* Yum commands to inspect installed libs
  * look for lib:  "rpm -qa | grep somelib"
  