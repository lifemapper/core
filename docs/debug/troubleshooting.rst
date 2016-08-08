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

   $ find /opt/lifemapper -type f -name "*.py" -exec sed -i 's/Copyright (C) 2015/Copyright (C) 2016/g' {} \;
   $ find /share/lmserver/data/archive/kubi/000 -type f -name "*.csv" -exec ls -f {} \;
   $ find /share/lmserver/data/archive/kubi/000 -type f -name "*.map" -exec rm -vf {} \;
