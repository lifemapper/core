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
import json 
import mx.DateTime
import os

from LmCommon.common.lmconstants import OutputFormat, JobStatus

from LmServer.base.lmobj import LMError, LMObject
from LmServer.base.serviceobject import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMFileType, 
               ID_PLACEHOLDER, LMServiceType, LMServiceModule)
# .........................................................................
class MFChain(ProcessObject):
# .............................................................................
   """
   """
# .............................................................................
   def __init__(self, userId, dlocation, priority, metadata,  
                status, statusModTime, mfChainId):
      """
      @summary Initialize the _ProjectionType class instance
      @param algorithm: Algorithm object for SDM model process
      @param modelScenario: : Scenario (environmental layer inputs) for 
             SDM model process
      @param modelMask: Mask for SDM model process
      @param projScenario: Scenario (environmental layer inputs) for 
             SDM project process
      @param projMask: Mask for SDM project process
      @param status: status of computation
      @param statusModTime: Time stamp in MJD for status modification.
      @param userId: Id for the owner of this projection
      @param projectionId: The projectionId for the database.  
      """
      if status is not None and statusModTime is None:
         statusModTime = mx.DateTime.utc().mjd
      self._dlocation = dlocation
      self.priority = priority
      self.metadata = metadata
      ProcessObject.__init__(self, objId=None, processType=None, parentId=None,
                             status=None, statusModTime=None)
      
      
# ...............................................
   def addMFMetadata(self, metadict):
      for key, val in metadict.iteritems():
         self.mfMetadata[key] = val
         
# ...............................................
   def dumpMFMetadata(self):
      metastring = None
      if self.mfMetadata:
         metastring = json.dumps(self.mfMetadata)
      return metastring

# ...............................................
   def loadMFMetadata(self, meta):
      """
      @note: Adds to dictionary or modifies values for existing keys
      """
      if meta is not None:
         if isinstance(meta, dict): 
            self.addMFMetadata(meta)
         else:
            try:
               metajson = json.loads(meta)
            except Exception, e:
               print('Failed to load JSON object from {} object {}'
                     .format(type(meta), meta))
            else:
               self.addMFMetadata(metajson)

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
