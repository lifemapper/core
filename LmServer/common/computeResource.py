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
from LmServer.base.lmobj import LMObject

# .............................................................................
class LMComputeResource(LMObject):
   
   def __init__(self, name, ipaddress, userId, ipMask=None, FQDN=None, 
                dbId=None, createTime=None, modTime=None, hbTime=None):
      """
      @summary ComputeResource constructor
      @param modTime: Last modification time of this object (optional)
      """
      LMObject.__init__(self)
      self._dbId = dbId
      self._userId = userId
      self.name = name
      self.ipAddress = ipaddress
      self.ipMask = ipMask
      self.FQDN = FQDN
      self.createTime = createTime
      self.modTime = modTime
      self.lastHeartbeat = hbTime
       
# ...............................................
   def getId(self):
      """
      @summary Returns the database id from the object table
      @return integer database id of the object
      """
      return self._dbId
   
   def setId(self, id):
      """
      @summary: Sets the database id on the object
      @param id: The database id for the object
      """
      self._dbId = id
   
# ...............................................
   def getUserId(self):
      """
      @summary Gets the User id
      @return The User id
      """
      return self._userId

   def setUserId(self, id):
      """
      @summary: Sets the user id on the object
      @param id: The user id for the object
      """
      self._userId = id
       
   # ...............................................
   def equals(self, other):
      result = (isinstance(other, LMComputeResource) and
                self.ipAddress == other.ipAddress)
      return result
      
# .............................................................................
# Read-Only Properties
# .............................................................................
   
   # The database id of the object
   id = property(getId, setId)
   
   # The user id of the object
   userId = property(getUserId, setUserId)
   
   