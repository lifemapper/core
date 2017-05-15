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
import os

from LmCommon.trees.lmTree import LmTree
from LmServer.base.lmobj import LMObject
from LmServer.base.serviceobject2 import ServiceObject
from LmServer.common.lmconstants import (LMServiceType, LMServiceModule, 
                                         LMFileType)
   
# .........................................................................
class Tree(LmTree, ServiceObject):
   """       
   Class to hold Tree data  
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, name, metadata={}, treeDict=None, dlocation=None,
                metadataUrl=None, userId=None, treeId=None, modTime=None):
      """
      @summary Constructor for the Tree class.  
      @copydoc LmCommon.trees.lmTree.LmTree::fromFile()
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @param name: The user-provided name of this tree
      @param treeDict: file or data (dictionary) for LmTree base object
      @param dlocation: file of data for LmTree base object
      @param treeId: dbId  for ServiceObject
      """
      ServiceObject.__init__(self, userId, treeId, LMServiceType.TREES, 
                             moduleType=LMServiceModule.LM, 
                             metadataUrl=metadataUrl, 
                             modTime=modTime)
      # TODO: Do we always want to read the file??
      #       Maybe just populate attributes saved in DB?
      if treeDict:
         LmTree.__init__(self, treeDict)
      else:
         LmTree.fromFile(dlocation)   
      self.name = name
      self._dlocation = dlocation
      self.treeMetadata = {}
      self.loadTreeMetadata(metadata)
         
      
# ...............................................
# Properties
# ...............................................

   
# .........................................................................
# Public Methods
# .........................................................................
   def readData(self):
      dloc = self._earlJr.createFilename(LMFileType.TREE,  objCode=self.getId(), 
                                         usr=self.getUserId())
      return dloc

# ...............................................
   def createLocalDLocation(self):
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
