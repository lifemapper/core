"""Module containing a user class for Lifemapper
"""
import hashlib

from LmBackend.common.lmobj import LMObject
from LmCommon.common.lmconstants import ENCODING
from LmServer.common.lmconstants import SALT


# .............................................................................
class LMUser(LMObject):
    """Class representing user objects"""

    # ................................
    def __init__(self, user_id, email, password, is_encrypted=False,
                 first_name=None, last_name=None, institution=None,
                 addr_1=None, addr_2=None, addr_3=None, phone=None,
                 mod_time=None):
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
        """Returns the user id for this object."""
        return self.user_id

    # ................................
    def set_user_id(self, user_id):
        """Sets the user id for this object"""
        self.user_id = user_id

    # ................................
    def check_password(self, passwd):
        """Check that the provided password is the same"""
        return self._password == self._encrypt_password(passwd)

    # ................................
    def set_password(self, passwd, is_encrypted):
        """Sets the password for this user object"""
        if is_encrypted:
            self._password = passwd
        else:
            self._password = self._encrypt_password(passwd)

    # ................................
    def get_password(self):
        """Returns the password of this user object"""
        return self._password

    # ................................
    @staticmethod
    def _encrypt_password(passwd):
        if not isinstance(passwd, bytes):
            passwd = passwd.encode(ENCODING)
        salt = SALT
        if not isinstance(salt, bytes):
            salt = salt.encode(ENCODING)
        hash_1 = hashlib.md5(passwd)
        hash_2 = hashlib.md5(salt)
        hash_3 = hashlib.md5(''.join((hash_1.hexdigest(), 
                                      hash_2.hexdigest())).encode(ENCODING))
        return hash_3.hexdigest()

    # ................................
    def equals(self, other):
        """Returns true/false if this user equals another

        Args:
            other: a LMUser object to test for equality

        Returns:
            boolean indicating whether self equals other
        """
        return isinstance(other, LMUser) and self.user_id == other.user_id
