"""
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
import os

from LmBackend.common.lmobj import LMObject
from LmCommon.common.lmconstants import JSON_INTERFACE, DEFAULT_TREE_SCHEMA
from LmCommon.trees.lmTree import LmTree
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import LMServiceType, LMFileType
   
# .........................................................................
class Tree(LmTree, ServiceObject):
    """       
    Class to hold Tree data  
    """
    # .............................................................................
    # Constructor
    # .............................................................................
    def __init__(self, name, metadata={}, dlocation=None, schema=DEFAULT_TREE_SCHEMA,
                 metadataUrl=None, userId=None, gridsetId=None, treeId=None, 
                 modTime=None):
        """
        @summary Constructor for the Tree class.  
        @copydoc LmCommon.trees.lmTree.LmTree::fromFile()
        @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
        @param name: The user-provided name of this tree
        @param dlocation: file of data for LmTree base object
        @param treeId: dbId  for ServiceObject
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
           
        if dlocation is not None:
            if os.path.exists(dlocation):
                LmTree.__init__(self, filename=dlocation, schema=schema)
      
# ...............................................
# Properties
# ...............................................


    # ..............................
    def read(self, dlocation=None, schema=DEFAULT_TREE_SCHEMA):
        """
        @summary: Reads tree data, either from the dlocation or getDLocation
        """
        if dlocation is None:
            dlocation = self.getDLocation()
        
        LmTree.__init__(self, filename=dlocation, schema=schema)
   
    # ..............................
    def setTree(self, tree):
        """
        @summary: Sets the tree value to an instance of dendropy tree.  This 
                     should be used if you have a dendropy tree created by some
                     method other than reading a file directly
        @param tree: An instance of dendropy 
        """
        LmTree.__init__(self, tree=tree)
      
    # ..............................
    def writeTree(self):
        """
        @summary: Writes the tree JSON to disk
        """
        dloc = self.getDLocation()
        LmTree.writeTree(self, dloc)

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
        return LMObject._dumpMetadata(self, self.treeMetadata)
 
    def loadTreeMetadata(self, newMetadata):
        self.treeMetadata = LMObject._loadMetadata(self, newMetadata)

    def addTreeMetadata(self, newMetadataDict):
        self.treeMetadata = LMObject._addMetadata(self, newMetadataDict, 
                                  existingMetadataDict=self.treeMetadata)

    # ...............................................
    def getDataUrl(self, interface=JSON_INTERFACE):
        durl = self._earlJr.constructLMDataUrl(self.serviceType, self.getId(), 
                                               interface)
        return durl

