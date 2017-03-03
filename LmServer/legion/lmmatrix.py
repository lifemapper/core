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
import os

from LmCommon.common.lmconstants import MatrixType, ProcessType, LMFormat
from LmCommon.common.matrix import Matrix
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import (LMServiceType, LMServiceModule, 
                                         LMFileType)
from LmServer.common.localconstants import APP_PATH
from LmServer.makeflow.cmd import MfRule

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
                gcmCode=None, altpredCode=None, dateCode=None,
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
      @param gcmCode: Code for the Global Climate Model used to create these data
      @param altpredCode: Code for the alternate prediction (i.e. IPCC scenario 
             or Representative Concentration Pathways/RCPs) used to create 
             these data
      @param dateCode: Code for the time period for which these data are predicted.
      @param metadata: dictionary of metadata using Keys defined in superclasses
      @param dlocation: file location of the array
      @param gridset: parent gridset of this MatrixupdateModtime
      @param matrixId: dbId  for ServiceObject
      """
      self.matrixType = matrixType
      self._dlocation = dlocation
      self.gcmCode = gcmCode
      self.altpredCode = altpredCode
      self.dateCode = dateCode
      self.mtxMetadata = {}
      self.loadMtxMetadata(metadata)
      self._gridset = gridset
      # parent values
      gridsetUrl = gridsetId = None
      if gridset is not None:
         gridsetUrl = gridset.metadataUrl
         gridsetId = gridset.getId()
      self._potato = []
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
# ...............................................
   def createLocalDLocation(self):
      ftype = LMFileType.getMatrixFiletype(self.matrixType)
      dloc = self.earlJr.createFilename(ftype, gridsetId=self.parentId, 
                                        objCode=self.getId(), 
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

# ...............................................
   def addSpud(self, spudArfFname):
      self._potato.append(spudArfFname)
      
# ...............................................
   def getArfFilename(self):
      """
      @summary: Return temporary filename to indicate completion of spud 
                (single-species) MF.
      """
      relFname = 'potato_{}.arf'.format(self.getId())
      return relFname

# ...............................................
   def getMashedFilename(self):
      """
      @summary: Return temporary filename to indicate completion of spud 
                (single-species) MF.
      """
      relFname = 'mashed_{}{}'.format(self.getId(), LMFormat.CSV.ext)
      return relFname

# ...............................................
   def getTriageFilename(self):
      """
      @summary: Return temporary filename to indicate completion of spud 
                (single-species) MF.
      """
      relFname = 'potato_{}.arf'.format(self.getId())
      return relFname

# ...............................................
   def writeTriageInput(self, overwrite=False):
      """
      @summary:
      """
      potatoFname = self.getPotatoFilename()
      if os.path.exists(potatoFname):
         if overwrite:
            os.remove(potatoFname)
         else:
            raise LMError('File {} already exists'.format(potatoFname))
      try:
         f = open(potatoFname, 'w')
         for spudArf in self._potato:
            f.write(spudArf)
      except Exception, e:
         raise
      finally:
         f.close()
   
# ...............................................
   def _createMatrixRule(self, processType, dependentFnameList, targetFnameList, 
                         cmdArgs=[]):
      """
      @summary: Creates a MF Rule from parameters. 
      @note: This assumes a single target file
      """
      scriptFname = os.path.join(APP_PATH, ProcessType.getTool(processType))
      cmdArguments = [os.getenv('PYTHON'), scriptFname]
      cmdArguments.extend(cmdArgs)
      cmd = ' '.join(cmdArguments)
      rule = MfRule(cmd, targetFnameList, dependencies=dependentFnameList)
      return rule

# ...............................................
   def computeMe(self):
      """
      @summary: Creates a command to triage possible MatrixColumn inputs,
                assemble into a LMMatrix, then test and catalog results.
      """
      rules = []
      mashedFname = self.getMashedFilename()
      matrixOutputFname = self.getDLocation()
      # Triage "Mash the potato" rule 
      tRule = self._createMatrixRule(ProcessType.MF_TRIAGE, 
                                     [self.getPotatoFilename()],
                                     [mashedFname],
                                     cmdArgs=[self.getPotatoFilename(), 
                                              mashedFname])
      rules.append(tRule)
      # Assemble Matrix rule
      cRule = self._createMatrixRule(ProcessType.CONCATENATE_MATRICES, 
                     [mashedFname], [matrixOutputFname], 
                     cmdArgs=['--mashedPotato={}'.format(mashedFname),
                              '--axis=1', 
                              matrixOutputFname])
      rules.append(cRule)
      # Store Matrix Rule
      status = None
      successFileBasename, _ = os.path.splitext(matrixOutputFname)
      uRule = self.getUpdateRule(status, successFileBasename, [matrixOutputFname])
      rules.append(uRule)
      rules.append(cRule)
        
      return rules
