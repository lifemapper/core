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
        self.userid = userid
        self.email = email
        self.setPassword(password, isEncrypted)
        self.firstName = firstName
        self.lastName = lastName
        self.institution = institution
        self.address1 = addr1
        self.address2 = addr2
        self.address3 = addr3
        self.phone = phone
        self.mod_time = mod_time

    # ................................
    def getUserId(self):
        """
        @note: Function exists for consistency with ServiceObjects
        """
        return self.userid

    # ................................
    def setUserId(self, id):
        """
        @note: Function exists for consistency with ServiceObjects
        """
        self.userid = id

    # ................................
    def checkPassword(self, passwd):
        return self._password == self._encryptPassword(passwd)

    # ................................
    def setPassword(self, passwd, isEncrypted):
        if isEncrypted:
            self._password = passwd
        else:
            self._password = self._encryptPassword(passwd)

    # ................................
    def getPassword(self):
        return self._password

    # ................................
    def _encryptPassword(self, passwd):
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
