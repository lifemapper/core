"""Module containing tree service object class
"""
import os

from lmpy import TreeWrapper

from LmBackend.common.lmobj import LMObject
from LmCommon.common.lmconstants import JSON_INTERFACE, DEFAULT_TREE_SCHEMA
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import LMServiceType, LMFileType

# .........................................................................
class Tree(TreeWrapper, ServiceObject):
    """Class to hold Tree data
    """
    # .............................................................................
    # Constructor
    # .............................................................................
    def __init__(self, name, metadata={}, dlocation=None, data=None,
                 schema=DEFAULT_TREE_SCHEMA,
                 metadataUrl=None, userId=None, gridsetId=None, treeId=None, 
                 modTime=None):
        """Constructor for the tree class.

        Args:
            name: The user-provided name of this tree
            dlocation: file of data for TreeWrapper base object
            treeId: dbId  for ServiceObject
        """
        ServiceObject.__init__(self, userId, treeId, LMServiceType.TREES, 
                               metadataUrl=metadataUrl, parentId=gridsetId, 
                               modTime=modTime)
        self.name = name
        self._dlocation = dlocation
        self.treeMetadata = {}
        self.loadTreeMetadata(metadata)

        # Read tree if available
        if dlocation is None:
            dlocation = self.getDLocation()

        # TODO (CJG): Make sure that this will load the tree appropriately
        if data is not None:
            self.get(data=data, schema=schema)
        elif dlocation is not None:
            if os.path.exists(dlocation):
                self.from_filename(dlocation)

    # ..............................
    def read(self, dlocation=None, schema=DEFAULT_TREE_SCHEMA):
        """
        @summary: Reads tree data, either from the dlocation or getDLocation
        """
        if dlocation is None:
            dlocation = self.getDLocation()
        self.get(path=dlocation, schema=schema)

    # ..............................
    def setTree(self, tree):
        """
        @summary: Sets the tree value to an instance of dendropy tree.  This 
                     should be used if you have a dendropy tree created by some
                     method other than reading a file directly
        @param tree: An instance of dendropy 
        """
        raise Exception('CJ - Implement this correctly')
      
    # ..............................
    def writeTree(self):
        """
        @summary: Writes the tree JSON to disk
        """
        dloc = self.getDLocation()
        self.write(path=dloc, schema=DEFAULT_TREE_SCHEMA)

    # ...............................................
    def getRelativeDLocation(self):
        """
        @summary: Return the relative filepath from object attributes
        @note: If the object does not have an ID, this returns None
        @note: This is to be pre-pended with a relative directory name for data  
               used by a single workflow/Makeflow 
        """
        basename = None
        self.setDLocation()
        if self._dlocation is not None:
            pth, basename = os.path.split(self._dlocation)
        return basename

    def createLocalDLocation(self):
        """
        @summary: Create an absolute filepath from object attributes
        @note: If the object does not have an ID, this returns None
        """
        dloc = self._earlJr.createFilename(LMFileType.TREE,  objCode=self.getId(), 
                                           usr=self.getUserId())
        return dloc

    def getDLocation(self):
        self.setDLocation()
        return self._dlocation
   
    def setDLocation(self, dlocation=None):
        """
        @summary: Set the _dlocation attribute if it is None.  Use dlocation
                  if provided, otherwise calculate it.
        @note: Does NOT override existing dlocation, use clearDLocation for that
        """
        if self._dlocation is None:
            if dlocation is None: 
                dlocation = self.createLocalDLocation()
            self._dlocation = dlocation

    def clearDLocation(self): 
        self._dlocation = None

    # ...............................................
    def dumpTreeMetadata(self):
        return LMObject._dump_metadata(self, self.treeMetadata)
 
    def loadTreeMetadata(self, newMetadata):
        self.treeMetadata = LMObject._load_metadata(self, newMetadata)

    def addTreeMetadata(self, newMetadataDict):
        self.treeMetadata = LMObject._add_metadata(self, newMetadataDict, 
                                  existingMetadataDict=self.treeMetadata)

    # ...............................................
    def getDataUrl(self, interface=JSON_INTERFACE):
        durl = self._earlJr.constructLMDataUrl(self.serviceType, self.getId(), 
                                               interface)
        return durl

