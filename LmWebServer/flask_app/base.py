"""The module provides a base Lifemapper service class
"""
from flask import session
import os

from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmServer.common.lmconstants import ARCHIVE_PATH 
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import WebLogger
from LmServer.db.borg_scribe import BorgScribe

# app = Flask(__name__)

# .............................................................................
class LmService:
    """This is the base Lifemapper service object

    This is the base Lifemapper service object that the services can inherit
    from.  It is responsible for getting a database connection and logger that
    can be used for the service.
    """

    # ..........................
    def __init__(self):
        """Constructor

        The constructor is only responsible for getting a logger, user and a
        scribe instance for the service.  We do that here in a simple base
        class in case we decide that we need to use a different mechanism (such
        as a CherryPy Tool)
        """
        log = WebLogger()
        self.scribe = BorgScribe(log)
        self.scribe.open_connections()
        self.log = log

    # ..........................
    def get_user(self, user_id=None):
        """Gets the user id for the service call.
    
        Gets the user id for the service call.  If urlUser is provided, try
        that first.  Then try the session and finally fall back to the
        PUBLIC_USER
        
        TODO: Save the username in the session
        """
        if user_id is None:
            self.get_user_id()
        usr = self.scribe.find_user(user_id)
        return usr

    # ..........................
    @classmethod
    def get_user_id(cls, url_user=None):
        """Gets the user id for the service call.
    
        Gets the user id for the service call.  If urlUser is provided, try
        that first.  Then try the session and finally fall back to the
        PUBLIC_USER
        
        TODO: Save the username in the session
        """
        # Check to see if we should use url user
        if url_user is not None:
            if url_user.lower() == 'public':
                return PUBLIC_USER
            if url_user.lower() == DEFAULT_POST_USER:
                return DEFAULT_POST_USER
        # Try to get the user from the session
        try:
            return session['username']
        except Exception:
            # Fall back to PUBLIC_USER
            return PUBLIC_USER
        
    # ................................
    @classmethod
    def get_user_dir(cls, user_id):
        """Get the user's workspace directory
    
        Todo:
            Change this to use something at a lower level.  This is using the
                same path construction as the getBoomPackage script
        """
        return os.path.join(ARCHIVE_PATH, user_id, 'uploads', 'biogeo')

    # ..........................
    @staticmethod
    def OPTIONS():
        """Common options request for all services (needed for CORS)
        """
        return
