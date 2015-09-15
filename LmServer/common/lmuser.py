"""
@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

          Lifemapper Project, lifemapper [at] ku [dot] edu, 
          Biodiversity Institute,
          1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
   
          This program is free software; you can redistribute it and/or modify 
          it under the terms of the GNU General Public License as published by 
          the Free Software Foundation; either version 2 of the License, or (at 
          your option) any later version.
  
          This program is distributed in the hope that it will be useful, but 
          WITHOUT ANY WARRANTY; without even the implied warranty of 
          MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
          General Public License for more details.
  
          You should have received a copy of the GNU General Public License 
          along with this program; if not, write to the Free Software 
          Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
          02110-1301, USA.
"""
import hashlib

from LmServer.base.lmobj import LMObject
from LmServer.common.lmconstants import SALT

# .............................................................................
class LMUser(LMObject):
   
   def __init__(self, userid, email, password, isEncrypted=False, 
                firstName=None, lastName=None, institution=None, 
                addr1=None, addr2=None, addr3=None, phone=None, modTime=None):
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
      @param modTime: Last modification time of this object (optional)
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
      self.modTime = modTime
       
# ...............................................
   def getUserId(self):
      """
      @note: Function exists for consistency with ServiceObjects
      """
      return self.userid

   def setUserId(self, id):
      """
      @note: Function exists for consistency with ServiceObjects
      """
      self.userid = id

# ...............................................
   def checkPassword(self, passwd):
      return self._password == self._encryptPassword(passwd)
   
# ...............................................
   def setPassword(self, passwd, isEncrypted):
      if isEncrypted:
         self._password = passwd
      else:
         self._password = self._encryptPassword(passwd)
   
# ...............................................
   def getPassword(self):
      return self._password

   # ...............................................
   def _encryptPassword(self, passwd):
      h1 = hashlib.md5(passwd)
      h2 = hashlib.md5(SALT)
      h3 = hashlib.md5(''.join((h1.hexdigest(), h2.hexdigest())))
      return h3.hexdigest()
   
   # ...............................................
   def equals(self, other):
      result = (isinstance(other, LMUser) and
                self.userid == other.userid)
      return result
      
# .............................................................................
class DbUser:
   def __init__(self, user, password):
      username = user
      password = password