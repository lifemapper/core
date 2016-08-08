Troubleshooting
===============

  Unrecoverable Error in Server
     Often, the easiest way to see what is going on is to try to import the svc.py module in an interpreter prompt.  This will usually throw whatever error is causing the problem with CherryPy.
    
        .. code-block ::
          
          $ $PYTHON
          >>> from LmWebServer.services.common.svc import svc

     If this doesn't show the error, check permissions on log files, etc. and look at Apache logs
