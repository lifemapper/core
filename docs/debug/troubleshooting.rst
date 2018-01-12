Troubleshooting
===============

  Unrecoverable Error in Server
     Often, the easiest way to see what is going on is to try to import the svc.py module in an interpreter prompt.  This will usually throw whatever error is causing the problem with CherryPy.
    
        .. code-block ::
          
          $ $PYTHON
          >>> from LmWebServer.services.common.svc import svc

     If this doesn't show the error, check permissions on log files, etc. and look at Apache logs

Handy commands
--------------

The `find` command is helpful for identifying old data that must be deleted,
or files that must be updated with a simple `sed` replace::

   $ find /opt/lifemapper -type f -name "*.py" -exec sed -i 's/old/new/g' {} \;
   $ find /share/lmserver/data/archive/kubi/000 -type f -name "*.csv" -exec ls -f {} \;
   $ find /share/lmserver/data/archive/kubi/000 -type f -name "*.map" -exec rm -vf {} \;

SGE Whoas
---------

  1/11/2018 - notyeti-192 had compute nodes that were not pulling jobs.  The problem was that they were not set up as execution hosts.  The following links proved to be useful:
  
   * https://biowiki.org/wiki/index.php/How_To_Administer_Sun_Grid_Engine
   * http://gridscheduler.sourceforge.net/howto/commontasks.html
