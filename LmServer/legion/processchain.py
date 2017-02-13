"""
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
# import mx.DateTime
import os

from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMFileType)
# .........................................................................
class MFChain(ProcessObject):
# .............................................................................
   """
   """
# .............................................................................
   def __init__(self, userId, dlocation, priority, metadata,  
                status, statusModTime, mfChainId):
      """
      @summary Initialize a Makeflow chain process
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @param userId: Id for the owner of this process
      @param dlocation:
      @param priority:
      @param metadata: Dictionary of metadata key/values; uses class or 
                       superclass attribute constants META_* as keys
      @param mfChainId: Database unique identifier
      """
#       if status is not None and statusModTime is None:
#          statusModTime = mx.DateTime.utc().mjd
      self._dlocation = dlocation
      self._userId = userId
      self.priority = priority
      self.mfMetadata = {}
      self.loadMfMetadata(metadata)
      ProcessObject.__init__(self, objId=mfChainId, processType=None, parentId=None,
                             status=status, statusModTime=statusModTime)
      
# ...............................................
   def dumpMfMetadata(self):
      return super(MFChain, self)._dumpMetadata(self.mfMetadata)
 
# ...............................................
   def loadMfMetadata(self, newMetadata):
      self.mfMetadata = super(MFChain, self)._loadMetadata(newMetadata)

# ...............................................
   def addMfMetadata(self, newMetadataDict):
      self.mfMetadata = super(MFChain, self)._addMetadata(newMetadataDict, 
                                  existingMetadataDict=self.mtxColMetadata)


# .............................................................................
# Superclass methods overridden
## .............................................................................
   def setId(self, lyrid):
      """
      @summary: Sets the database id on the object, and sets the 
                SDMProjection.mapPrefix of the file if it is None.
      @param id: The database id for the object
      """
      ServiceObject.setId(self, lyrid)
      if lyrid is not None:
         self.name = self._earlJr.createLayername(projId=lyrid)
         if self._dlocation is None:
            filename = self.createLocalDLocation()
            if os.path.exists(filename):
               self._dlocation = filename
         
         self.title = '%s Projection %s' % (self.speciesName, str(lyrid))
         self._setMapPrefix()
   
# ...............................................
   def createLocalDLocation(self):
      """
      @summary: Create data location
      """
      dloc = None
      if self.getId() is not None:
         dloc = self._earlJr.createFilename(LMFileType.MF_DOCUMENT, 
                                       mfchainId=self.objId, usr=self._userId)
      return dloc

# ...............................................
   def getDLocation(self):
      self.setDLocation()
      return self._dlocation

   def setDLocation(self, dlocation=None):
      if self._dlocation is None and dlocation is None:
         dlocation = self.createLocalDLocation()
      self._dlocation = dlocation

# ...............................................
   def getUserId(self):
      """
      @summary Gets the User id
      @return The User id
      """
      return self._userId

   def setUserId(self, usr):
      """
      @summary: Sets the user id on the object
      @param usr: The user id for the object
      """
      self._userId = usr

