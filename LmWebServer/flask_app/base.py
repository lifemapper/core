"""The module provides a base Lifemapper service class
"""
from flask import session
from flask_login._compat import text_type
import os

from LmCommon.common.lmconstants import DEFAULT_POST_USER
from LmServer.common.lmconstants import ARCHIVE_PATH 
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import WebLogger
from LmServer.common.lmuser import LMUser
from LmServer.db.borg_scribe import BorgScribe

# app = Flask(__name__)

class WebUser(LMUser):
    """Extends lmuser objects for flask-login"""

    # ................................
    def __init__(
        self, user_id, email, password, is_encrypted=False, first_name=None, last_name=None, 
        institution=None, addr_1=None, addr_2=None, addr_3=None, phone=None, mod_time=None):
        """Constructor

        Args:
            user_id: user chosen unique id
            email:  EMail address of user
            password: user chosen password
            first_name: The first name of this user
            last_name: The last name of this user
            institution: institution of user (optional)
            addr_1: Address, line 1, of user (optional)
            addr_2: Address, line 2, of user (optional)
            addr_3: Address, line 3, of user (optional)
            phone: Phone number of user (optional)
            mod_time: Last modification time of this object (optional)
        """
        LMUser.__init__(
            self, user_id, email, password, is_encrypted=is_encrypted, first_name=first_name, 
            last_name=last_name, institution=institution, addr_1=addr_1, addr_2=addr_2, addr_3=addr_3, 
            phone=phone, mod_time=mod_time)
        self._authenticated = False
        self._active = False
        
    # ..........................
    def is_authenticated(self):
        return self._authenticated
        
    # ..........................
    def is_active(self):
        if self.user_id in (PUBLIC_USER, DEFAULT_POST_USER):
            return False
        return True
        
    # ..........................
    def is_anonymous(self):
        if self.user_id in (PUBLIC_USER, DEFAULT_POST_USER):
            return True
        return False
        
    # ..........................
    def get_id(self):
        if self.user_id not in (PUBLIC_USER, DEFAULT_POST_USER):
            try:
                return text_type(self.user_id)
            except AttributeError:
                raise NotImplementedError('No `user_id` attribute - override `get_id`')
        return
        
        

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
        scribe instance for the service.  
        """
        log = WebLogger()
        self.scribe = BorgScribe(log)
        self.scribe.open_connections()
        self.log = log

    # ..........................
    def get_user(self, user_id=None):
        """Gets the user id for the service call.
    
        Gets the user id for the service call.  If user_id is provided, try
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
    def get_user_id(cls, user_id=None):
        """Gets the lmuser for the service call.
    
        Gets the user id for the service call.  If urlUser is provided, try
        that first.  Then try the session and finally fall back to the
        PUBLIC_USER
        
        TODO: Save the username in the session
        """
        # Check to see if we should use url user
        if user_id is not None:
            if user_id.lower() == 'public':
                return PUBLIC_USER
            if user_id.lower() == DEFAULT_POST_USER:
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
