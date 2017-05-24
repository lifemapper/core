"""
@summary Module that contains the Model class
@author Aimee Stewart
@status Status: alpha
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
import mx.DateTime 
from LmBackend.common.lmobj import LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import ID_PLACEHOLDER

# .............................................................................
class ServiceObject(LMObject):
   """
   The ServiceObject class contains all of the information for subclasses
   to be exposed in a webservice. 
   """
   
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, userId, dbId, createTime, modTime, serviceType, moduleType=None,
                metadataUrl=None, parentMetadataUrl=None):
      """
      @summary Constructor for the abstract ServiceObject class
      @param userId: id for the owner of these data
      @param dbid: database id of the object 
      @param createTime: Create Time/Date, in Modified Julian Day (MJD) format
      @param modTime: Last modification Time/Date, in MJD format
      @param serviceType: REST Service Type
      @param moduleType: (optional) ModuleType (RAD/SDM) for base objects not 
                         identified by a module-specific class
      @param metadataUrl: (optional) URL for retrieving the metadata
      @param parentMetadataUrl: (optional) URL for retrieving the metadata of a
                                 parent container object
      """
      LMObject.__init__(self)
      self._earlJr = EarlJr()
      self._metadataUrl = metadataUrl
      self._parentMetadataUrl = parentMetadataUrl
      self.createTime = createTime
      self.modTime = modTime
      self._userId = userId
      self._dbId = dbId
      self.serviceType = serviceType
      if serviceType is None:
         raise Exception('Object %s does not have serviceType' % str(type(self)))
#       if moduleType is None:
#          raise Exception('Object %s does not have moduleType' % str(type(self)))
      self.moduleType = moduleType
      
      
# .............................................................................
# Public methods
# .............................................................................
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

# .............................................................................
# Private methods
# .............................................................................
   @property
   def metadataUrl(self):
      """
      @summary Return the SGUID (Somewhat Globally Unique IDentifier), 
               aka metadataUrl, for this object
      @return URL string representing a webservice request for this object
      """
      if self._metadataUrl is None:
         try: 
            self._metadataUrl = self.constructMetadataUrl() 
         except Exception, e:
            print str(e)
            pass
      return self._metadataUrl
   
   def setParentMetadataUrl(self, url):
      self._parentMetadataUrl = url
      
# ...............................................   
   def resetMetadataUrl(self):
      """
      @summary Gets the REST service URL for this object
      @return URL string representing a webservice request for metadata of this object
      """        
      self._metadataUrl = self.constructMetadataUrl()
      #TODO: Aimee - This was returning 'murl' which does not exist here.  
      #         Do you want this to return the metadata url or just set it?
      return self._metadataUrl
   
# ...............................................   
   def constructMetadataUrl(self):
      """
      @summary Gets the REST service URL for this object
      @return URL string representing a webservice request for metadata of this object
      """        
      objId = self.getId() 
      if objId is None:
         objId = ID_PLACEHOLDER
         
      murl = self._earlJr.constructLMMetadataUrl(self.serviceType, 
                                               objId,
                                               moduleType=self.moduleType,
                                               parentMetadataUrl=self._parentMetadataUrl)
      return murl
   
# ...............................................
   def getURL(self, format=None):
      """
      @summary Return a GET query for the Lifemapper WCS GetCoverage request
      @param format: optional string indicating the URL response format desired;
                     Supported formats are GDAL Raster Format Codes, available 
                     at http://www.gdal.org/formats_list.html, and keys in 
                        LmServer.common.lmconstants GDALFormatCodes.
      """
      dataurl = self.metadataUrl
      if format is not None:
         dataurl = '%s/%s' % (self.metadataUrl, format)
      return dataurl
# .............................................................................
# Read-0nly Properties
# .............................................................................
   
   # The database id of the object
   id = property(getId)
   
   # The user id of the object
   user = property(getUserId)
   
# .............................................................................
class ProcessObject(LMObject):
   """
   Class to hold information about a parameterized object for processing. 
   """
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, objId=None, processType=None, parentId=None,
                status=None, statusModTime=None, stage=None, stageModTime=None):
      """
      @param objId: Unique identifier for this parameterized object
      @param processType: Integer code LmCommon.common.lmconstants.ProcessType
      @param parentId: Id of container (i.e. RADBucket), if any, associated 
             with one instance of this parameterized object
      @param status: status of processing
      @param statusModTime: time of the latest status modification 
             in modified julian date format
      @param stage: stage of processing 
      @param stageModTime: time of the latest stage modification 
             in modified julian date format
      @note: The object with objId can be instantiated for each container, 
             all use the same base object, but will be subject to different 
             processes (for example PALayers intersected for every bucket)
      @todo: Revisit stage/stageModTime when job chaining is implemented
      """
      self.objId = objId
      self.processType = processType
      self.parentId = parentId
      self._status = status
      self._statusmodtime = statusModTime
      self._stage = stage
      self._stagemodtime = stageModTime
      
   # ...............................................
   @property
   def status(self):
      return self._status

   @property
   def statusModTime(self):
      return self._statusmodtime

   @property
   def stage(self):
      return self._stage

   @property
   def stageModTime(self):
      return self._stagemodtime

   # ...............................................
   def updateStatus(self, status, modTime=mx.DateTime.gmt().mjd, stage=None,
                    statusModTime=None, stageModTime=None):
      self._status = status
      self._statusmodtime = modTime
      if stage is not None and stage != self._stage:
         self._stage = stage
         self._stagemodtime = modTime
