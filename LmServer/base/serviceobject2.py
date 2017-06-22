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
from LmCommon.common.lmconstants import ProcessType
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import ID_PLACEHOLDER, ProcessTool
from LmServer.legion.cmd import MfRule

# .............................................................................
class ServiceObject(LMObject):
   """
   The ServiceObject class contains all of the information for subclasses
   to be exposed in a webservice. 
   """
   META_TITLE = 'title'
   META_AUTHOR = 'author'
   META_DESCRIPTION = 'description'
   META_KEYWORDS = 'keywords'
   
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, userId, dbId, serviceType, metadataUrl=None, 
                parentMetadataUrl=None, modTime=None):
      """
      @summary Constructor for the abstract ServiceObject class
      @param userId: id for the owner of these data
      @param dbId: database id of the object 
      @param serviceType: constant from LmServer.common.lmconstants.LMServiceType
      @param metadataUrl: URL for retrieving the metadata
      @param parentMetadataUrl: URL for retrieving the metadata of a
                                 parent container object
      @param modTime: Last modification Time/Date, in MJD format
      """
      LMObject.__init__(self)
      self._earlJr = EarlJr()

      self._userId = userId
      self._dbId = dbId
      self.serviceType = serviceType
      self._metadataUrl = metadataUrl
      self._parentMetadataUrl = parentMetadataUrl
      self.modTime = modTime
      if serviceType is None:
         raise Exception('Object %s does not have serviceType' % str(type(self)))      
      
# .............................................................................
# Public methods
# .............................................................................
   def getId(self):
      """
      @summary Returns the database id from the object table
      @return integer database id of the object
      """
      return self._dbId
   
   def setId(self, dbid):
      """
      @summary: Sets the database id on the object
      @param dbid: The database id for the object
      """
      self._dbId = dbid
   
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
      
   @property
   def parentMetadataUrl(self):
      return self._parentMetadataUrl 
      
# ...............................................   
   def resetMetadataUrl(self):
      """
      @summary Gets the REST service URL for this object
      @return URL string representing a webservice request for metadata of this object
      """        
      self._metadataUrl = self.constructMetadataUrl()
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
                           objId, parentMetadataUrl=self._parentMetadataUrl)
      return murl
   
# ...............................................
   def getURL(self, format=None):
      """
      @summary Return a GET query for the Lifemapper WCS GetCoverage request
      @param format: optional string indicating the URL response format desired;
                     Supported formats are GDAL Raster Format Codes, available 
                     at http://www.gdal.org/formats_list.html, and driver values 
                     in LmServer.common.lmconstants LMFormat GDAL formats.
      """
      dataurl = self.metadataUrl
      if format is not None:
         dataurl = '%s/%s' % (self.metadataUrl, format)
      return dataurl
   
   # ...............................................
   def updateModtime(self, modTime):
      self.modTime = modTime

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
                status=None, statusModTime=None):
      """
      @param objId: Unique identifier for this parameterized object
      @param processType: Integer code LmCommon.common.lmconstants.ProcessType
      @param parentId: Id of container, if any, associated with one instance of 
                       this parameterized object
      @param status: status of processing
      @param statusModTime: last status modification time in MJD format 
      @note: The object with objId can be instantiated for each container, 
             all use the same base object, but will be subject to different 
             processes (for example PALayers intersected for every bucket)
      """
      self.objId = objId
      self.processType = processType
      self.parentId = parentId
      self._status = status
      self._statusmodtime = statusModTime
      
   # ...............................................
   @property
   def status(self):
      return self._status

   @property
   def statusModTime(self):
      return self._statusmodtime

   # ...............................................
   def updateStatus(self, status, modTime):
      self._status = status
      self._statusmodtime = modTime

# # ...............................................
#    def _getUpdateSuccessFilename(self):
#       """
#       @summary: Return temporary filename to indicate completion of update 
#                 command.
#       """
#       prefix = self.processType
#       relFname = 'potato_{}.success'.format(self.objId)
#       return relFname

   # ...............................................
   def getUpdateRule(self, objId, status, successFileBasename, filesToCheck):
      """
      @summary: Creates a rule to test output files and update the database
      @param objId: Identifier used to query the database for this object
      @param status: Output value or file containing value of object process
             results.  Currently unused. 
      @param successFileBasename: basename of file which will be written to  
             indicate success of update operation 
      @param filesToCheck: List of files to be tested for validity.  
      """
      opts = []
      if status is not None:
         try:
            int(status)
            opts.append('-s {}'.format(status))
         except:
            opts.append('-f {}'.format(status))
      successFname = successFileBasename + '.success'
      # Assemble command - configured python and script
      args = ['LOCAL', '$PYTHON ', 
              ProcessTool.get(ProcessType.UPDATE_OBJECT)]
      # options
      args.extend(opts)
      # positional arguments
      args.extend([str(self.processType), str(objId), successFname])
      args.extend(filesToCheck)
      cmd = ' '.join(args)
      
      rule = MfRule(cmd, [successFname], dependencies=filesToCheck)
      return rule
