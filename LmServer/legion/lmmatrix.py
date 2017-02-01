"""
@summary Module that contains the Matrix class
@author Aimee Stewart
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
from LmCommon.common.lmconstants import MatrixType
from LmCommon.common.matrix import Matrix
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import LMServiceType,LMServiceModule


# .............................................................................
class LMMatrix(Matrix, ServiceObject, ProcessObject):
   """
   The Matrix class contains a 2-dimensional numeric matrix.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrix, headers=None,
                matrixType=MatrixType.PAM, 
                processType=None,
                metadata={},
                dlocation=None, 
                metadataUrl=None,
                userId=None,
                gridset=None, 
                matrixId=None,
                status=None, statusModTime=None):
      """
      @copydoc LmCommon.common.matrix.Matrix::__init__()
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @param matrix: data (numpy) array for Matrix base object
      @param matrixType: Constant from LmCommon.common.lmconstants.MatrixType
      @param metadata: dictionary of metadata using Keys defined in superclasses
      @param dlocation: file location of the array
      @param gridset: parent gridset of this MatrixupdateModtime
      @param matrixId: dbId  for ServiceObject
      """
      self.matrixType = matrixType
      self._dlocation = dlocation
      self.mtxMetadata = {}
      self.loadMtxMetadata(metadata)
      self._gridset = gridset
      # parent values
      gridsetUrl = gridsetId = None
      if gridset is not None:
         gridsetUrl = gridset.metadataUrl
         gridsetId = gridset.getId()
      Matrix.__init__(self, matrix, headers=headers)
      ServiceObject.__init__(self,  userId, matrixId, LMServiceType.MATRICES, 
                             moduleType=LMServiceModule.LM, 
                             metadataUrl=metadataUrl, 
                             parentMetadataUrl=gridsetUrl,
                             modTime=statusModTime)
      ProcessObject.__init__(self, objId=matrixId, processType=processType,
                             parentId=gridsetId, 
                             status=status, statusModTime=statusModTime)       

# ...............................................
   @classmethod
   def initFromParts(cls, baseMtx, gridset=None, 
                     processType=None, metadataUrl=None, userId=None,
                     status=None, statusModTime=None):
      mtxobj = LMMatrix(None, matrixType=baseMtx.matrixType, 
                        processType=processType, metadata=baseMtx.mtxMetadata,
                        dlocation=baseMtx.getDLocation(), 
                        columnIndices=baseMtx.getColumnIndices(),
                        columnIndicesFilename=baseMtx.getColumnIndicesFilename(),
                        metadataUrl=metadataUrl, userId=userId, gridset=gridset, 
                        matrixId=baseMtx.getMatrixId(), status=baseMtx.status, 
                        statusModTime=baseMtx.statusModTime)
      return mtxobj

   # ...............................................
   def updateStatus(self, status=None, metadata=None, modTime=None):
      """
      @summary: Updates matrixIndex, paramMetadata, and modTime.
      @param metadata: Dictionary of Matrix metadata keys/values; key constants  
                       are ServiceObject class attributes.
      @copydoc LmServer.base.serviceobject2.ProcessObject::updateStatus()
      @copydoc LmServer.base.serviceobject2.ServiceObject::updateModtime()
      @note: Missing keyword parameters are ignored.
      """
      if metadata is not None:
         self.loadMtxMetadata(metadata)
      ProcessObject.updateStatus(self, status, modTime=modTime)
      ServiceObject.updateModtime(self, modTime=modTime)

# ...............................................
   @property
   def gridsetName(self):
      name = None
      if self._gridset is not None:
         name = self._gridset.name
      return name
   
# ...............................................
   @property
   def gridsetId(self):
      gid = None
      if self._gridset is not None:
         gid = self._gridset.getId()
      return gid

# ...............................................
   @property
   def gridsetUrl(self):
      url = None
      if self._gridset is not None:
         url = self._gridset.metadataUrl
      return url

# ...............................................
   def getDLocation(self):
      return self._dlocation
   
   def setDLocation(self, dlocation):
      self._dlocation = dlocation

# ...............................................
   def getGridset(self):
      return self._gridset

# ...............................................
   def getShapegrid(self):
      return self._gridset.getShapegrid()   
   
# ...............................................
   def dumpMtxMetadata(self):
      return super(LMMatrix, self)._dumpMetadata(self.mtxMetadata)

# ...............................................
   def addMtxMetadata(self, newMetadataDict):
      self.mtxMetadata = super(LMMatrix, self)._addMetadata(newMetadataDict, 
                                  existingMetadataDict=self.mtxMetadata)

# ...............................................
   def loadMtxMetadata(self, newMetadata):
      self.mtxMetadata = super(LMMatrix, self)._loadMetadata(newMetadata)
