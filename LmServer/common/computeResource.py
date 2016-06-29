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
   
   def __init__(self, name, ipaddress, userId, ipSignificantBits=None, 
                createTime=None, 
                FQDN=None, dbId=None, modTime=None, hbTime=None):
      """
      @summary ComputeResource constructor
      @param modTime: Last modification time of this object (optional)
      """
      LMObject.__init__(self)
      self._dbId = dbId
      self._userId = userId
      self.name = name
      self.ipAddress = ipaddress
      self.ipSignificantBits = ipSignificantBits
      self.FQDN = FQDN
      # TODO: remove createTime
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
      
   # ...............................................
   def matchIP(self, ipAddress):
      """
      @summary: Checks to see if the provided IP address matches the network
                   specified by the compute resource
      @note: I used inner functions because they are not used anywhere else in
                the module or another module and it will be easier to split
                this out later if we want to
      """
      # ...................................
      def convertIPtoBinary(ip):
         """
         @summary: Converts an IP address (in octect form) to binary
         @note: Assumes IPV4
         """
         return ''.join(["{0:08b}".format(int(octet)) for octet in ip.split('.')])

      # ...................................
      def maskBinIp(binIP, sigbits):
         """
         @summary: Returns the number of binary digits from the IP address 
                      specified by the mask
         """
         if sigbits is None or sigbits == '' or sigbits == 0:
            sigbits = 32
         else:
            sigbits = int(sigbits)
         return binIP[:sigbits]

      binIpAddress = convertIPtoBinary(ipAddress)
      myMaskedIp = maskBinIp(convertIPtoBinary(self.ipAddress), 
                             self.ipSignificantBits)
      # Check that the provided IP address matches the masked IP address of the
      #    compute resource
      return binIpAddress.startswith(myMaskedIp)

# .............................................................................
# Read-Only Properties
# .............................................................................
   
   # The database id of the object
   id = property(getId, setId)
   
   # The user id of the object
   userId = property(getUserId, setUserId)
   
   
