"""Module containing base service object classes
"""
from LmBackend.common.lmobj import LMObject
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import ID_PLACEHOLDER

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
    META_CITATION = 'citation'
    META_PARAMS = 'parameters'
    
# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, userId, dbId, serviceType, metadataUrl=None, 
                     parentMetadataUrl=None, parentId=None, modTime=None):
        """
        @summary Constructor for the abstract ServiceObject class
        @param userId: id for the owner of these data
        @param dbId: database id of the object 
        @param serviceType: constant from LmServer.common.lmconstants.LMServiceType
        @param metadataUrl: URL for retrieving the metadata
        @param parentMetadataUrl: URL for retrieving the metadata of a
                                            parent container object
        @param parentId: Id of container, if any, associated with one instance of 
                              this parameterized object
        @param modTime: Last modification Time/Date, in MJD format
        """
        LMObject.__init__(self)
        self._earlJr = EarlJr()

        self._userId = userId
        self._dbId = dbId
        self.serviceType = serviceType
        self._metadataUrl = metadataUrl
        # Moved from ProcessObject
        self.parentId = parentId
        self._parentMetadataUrl = parentMetadataUrl
        self.modTime = modTime
        if serviceType is None:
            raise Exception('Object %s does not have serviceType' % str(type(self)))        
        
# .............................................................................
# Public methods
# .............................................................................
    def get_id(self):
        """
        @summary Returns the database id from the object table
        @return integer database id of the object
        """
        return self._dbId
    
    def set_id(self, dbid):
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
            except Exception as e:
                print(str(e))
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
        objId = self.get_id() 
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
    id = property(get_id)
    
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
    def __init__(self, objId=None, processType=None, 
                     status=None, statusModTime=None):
        """
        @param objId: Unique identifier for this parameterized object
        @param processType: Integer code LmCommon.common.lmconstants.ProcessType
        @param status: status of processing
        @param statusModTime: last status modification time in MJD format 
        @note: The object with objId can be instantiated for each container, 
                 all use the same base object, but will be subject to different 
                 processes (for example PALayers intersected for every bucket)
        """
        self.objId = objId
        self.processType = processType
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


