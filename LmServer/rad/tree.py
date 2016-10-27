"""
@summary Module that contains the Tree class
@author Aimee Stewart
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
import json

from LmServer.base.serviceobject import ServiceObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule


# .............................................................................
class Tree(ServiceObject):
   """
   The Tree class contains tree data.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, tree, metadata={}, dlocation=None,
                hasBranchLengths=False, isUltraMetric=False, 
                userId=None, treeId=None, metadataUrl=None, modtime=None):
      """
      @param tree: JSON or Newick tree
      @param dlocation: file location of the array
      """
      ServiceObject.__init__(self, userId, treeId, modtime, modtime,
            LMServiceType.TREES, moduleType=LMServiceModule.RAD,
            metadataUrl=metadataUrl)
      self._tree = tree
      self.metadata = {}
      self.loadMetadata(metadata)
      self.hasBranchLengths = hasBranchLengths
      self.isUltraMetric = isUltraMetric
      self._dlocation = dlocation

# ...............................................
   def addMetadata(self, metadict):
      for key, val in metadict.iteritems():
         self.metadata[key] = val
         
   def dumpMetadata(self):
      metastring = None
      if self.metadata:
         metastring = json.dumps(self.metadata)
      return metastring

   def loadMetadata(self, meta):
      if isinstance(meta, dict): 
         self.addMetadata(meta)
      else:
         self.metadata = json.loads(meta)

# ..............................................................................
   def getDLocation(self): 
      return self._dlocation
   
   def setDLocation(self, dlocation):
      self._dlocation = dlocation

# ...............................................
   def clear(self):
      success, msg = self._deleteFile(self._dlocation, deleteDir=True)
      self._tree = None
