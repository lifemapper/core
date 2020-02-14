"""Module containing a user class for Lifemapper
"""
import hashlib

from LmBackend.common.lmobj import LMObject
from LmServer.common.lmconstants import SALT


# .............................................................................
class LMUser(LMObject):

    # ................................
    def __init__(self, user_id, email, password, is_encrypted=False,
                     first_name=None, last_name=None, institution=None,
                     addr_1=None, addr_2=None, addr_3=None, phone=None, mod_time=None):
        """
        @summary Layer superclass constructor
        @param userid: user chosen unique id
        @param email:  EMail address of user
        @param password: user chosen password
        @param fullname: full name of user (optional)
        @param institution: institution of user (optional)
        @param addr1: Address, line 1, of user (optional)
        @param addr2: Address, line 1, of user (optional)
        @param addr3: Address, line 1, of user (optional)
        @param phone: Phone number of user (optional)
        @param mod_time: Last modification time of this object (optional)
        """
        LMObject.__init__(self)
        self.user_id = user_id
        self.email = email
        self.set_password(password, is_encrypted)
        self.first_name = first_name
        self.last_name = last_name
        self.institution = institution
        self.address_1 = addr_1
        self.address_2 = addr_2
        self.address_3 = addr_3
        self.phone = phone
        self.mod_time = mod_time

    # ................................
    def get_user_id(self):
        """
        @note: Function exists for consistency with ServiceObjects
        """
        return self.user_id

    # ................................
    def set_user_id(self, id):
        """
        @note: Function exists for consistency with ServiceObjects
        """
        self.user_id = id

    # ................................
    def check_password(self, passwd):
        return self._password == self._encrypt_password(passwd)

    # ................................
    def set_password(self, passwd, is_encrypted):
        if is_encrypted:
            self._password = passwd
        else:
            self._password = self._encrypt_password(passwd)

    # ................................
    def get_password(self):
        return self._password

    # ................................
    def _encrypt_password(self, passwd):
        h1 = hashlib.md5(passwd)
        h2 = hashlib.md5(SALT)
        h3 = hashlib.md5(''.join((h1.hexdigest(), h2.hexdigest())))
        return h3.hexdigest()

    # ................................
    def equals(self, other):
        result = (isinstance(other, LMUser) and
                     self.userid == other.userid)
        return result


# .............................................................................
class DbUser:

    # ................................
    def __init__(self, user, password):
        username = user
        password = password
