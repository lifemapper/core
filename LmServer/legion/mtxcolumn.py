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

from LmCommon.common.lmconstants import ProcessType, JobStatus, LMFormat
from LmCommon.common.matrix import Matrix
from LmServer.base.layer2 import _LayerParameters
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import LMServiceType
from LmServer.common.localconstants import APP_PATH
from LmServer.legion.cmd import MfRule

# .............................................................................
# .............................................................................
# .............................................................................
# TODO: This should inherit from LmCommon.common.matrix.Matrix
class MatrixColumn(Matrix, _LayerParameters, ServiceObject, ProcessObject):
   # Query to filter layer for intersect
   INTERSECT_PARAM_FILTER_STRING = 'filterString'
   # Attribute used in layer intersect
   INTERSECT_PARAM_VAL_NAME = 'valName'
   # Units of measurement for attribute used in layer intersect
   INTERSECT_PARAM_VAL_UNITS = 'valUnits'
   # Minimum spatial coverage for gridcell intersect computation
   INTERSECT_PARAM_MIN_PERCENT = 'minPercent'
   # Minimum percentage of acceptable value for PAM gridcell intersect computation 
   INTERSECT_PARAM_MIN_PRESENCE = 'minPresence'
   # Maximum percentage of acceptable value for PAM gridcell intersect computation 
   INTERSECT_PARAM_MAX_PRESENCE = 'maxPresence'

   # Types of GRIM gridcell intersect computation
   INTERSECT_PARAM_WEIGHTED_MEAN = 'weightedMean'
   INTERSECT_PARAM_LARGEST_CLASS = 'largestClass'
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrixIndex, matrixId, userId, 
                # inputs if this is connected to a layer and shapegrid 
                layer=None, shapegrid=None, intersectParams={}, 
                squid=None, ident=None,
                processType=None, 
                metadata={}, 
                matrixColumnId=None, 
                postToSolr=True,
                status=None, statusModTime=None):
      """
      @summary MatrixColumn constructor
      @copydoc LmServer.base.layer2._LayerParameters::__init__()
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @param matrixIndex: index for column within a matrix.  For the Global 
             PAM, assembled dynamically, this will be None.
      @param matrixId: 
      @param layer: layer input to intersect
      @param shapegrid: grid input to intersect 
      @param intersectParams: parameters input to intersect
      @param squid: species unique identifier for column
      @param ident: (non-species) unique identifier for column
      """
      _LayerParameters.__init__(self, userId, paramId=matrixColumnId, 
                                matrixIndex=matrixIndex, metadata=metadata, 
                                modTime=statusModTime)
      ServiceObject.__init__(self,  userId, matrixColumnId, 
                             LMServiceType.MATRIX_COLUMNS, 
                             modTime=statusModTime)
      ProcessObject.__init__(self, objId=matrixColumnId, processType=processType, 
                             parentId=matrixId, status=status, 
                             statusModTime=statusModTime)
      self.layer = layer
      self.shapegrid = shapegrid
      self.intersectParams = {}
      self.loadIntersectParams(intersectParams)
      self.squid = squid
      self.ident = ident
      self.postToSolr = postToSolr

# ...............................................
   def setId(self, mtxcolId):
      """
      @summary: Sets the database id on the object, and sets the 
                dlocation of the file if it is None.
      @param mtxcolId: The database id for the object
      """
      self.objId = mtxcolId

# ...............................................
   def getId(self):
      """
      @summary Returns the database id from the object table
      @return integer database id of the object
      """
      return self.objId
   
# ...............................................
   def getLayerId(self):
      if self.layer is not None:
         return self.layer.getId()
      return None
   
# ...............................................
   @property
   def displayName(self):
      try:
         dname = self.layer.displayName
      except:
         try:
            dname = self.layer.name
         except:
            dname = self.squid
      return dname

# ...............................................
   def dumpIntersectParams(self):
      return super(MatrixColumn, self)._dumpMetadata(self.intersectParams)
 
# ...............................................
   def loadIntersectParams(self, newIntersectParams):
      self.intersectParams = super(MatrixColumn, self)._loadMetadata(newIntersectParams)

# ...............................................
   def addIntersectParams(self, newIntersectParams):
      self.intersectParams = super(MatrixColumn, self)._addMetadata(newIntersectParams, 
                                  existingMetadataDict=self.intersectParams)
   
# ...............................................
   def updateStatus(self, status, matrixIndex=None, metadata=None, modTime=None):
      """
      @summary Update status, matrixIndex, metadata, modTime attributes on the 
               Matrix layer. 
      @copydoc LmServer.base.serviceobject2.ProcessObject::updateStatus()
      @copydoc LmServer.base.layer2._LayerParameters::updateParams()
      """
      ProcessObject.updateStatus(self, status, modTime=modTime)
      _LayerParameters.updateParams(self, matrixIndex=matrixIndex, 
                                    metadata=metadata, modTime=modTime)

# ...............................................
   def getTargetFilename(self):
      """
      @summary: Return temporary filename for output.
      """
      relFname = 'mtxcol_{}{}'.format(self.getId(), LMFormat.NUMPY.ext)
      return relFname

# ...............................................
   def computeMe(self, workDir=None):
      """
      @summary: Creates a command to intersect a layer and a shapegrid to 
                produce a MatrixColumn.
      """
      rules = []
      # Layer object may be an SDMProject
      if self.layer is not None:
         
         if workDir is None:
            workDir = ''
         
         targetDir = os.path.join(workDir, 
                        os.path.splitext(self.layer.getRelativeDLocation())[0])
         
         inputLayerFname = os.path.join(targetDir,
                                   os.path.basename(self.layer.getDLocation()))
            
         # Layer input
         dependentFiles = [inputLayerFname]
         try:
            status = self.layer.status
         except:
            status = JobStatus.COMPLETE

         if JobStatus.waiting(status):
            lyrRules = self.layer.computeMe(workDir=workDir)
            rules.extend(lyrRules)
            
         shpgrdRules = self.shapegrid.computeMe(workDir=workDir)
         rules.extend(shpgrdRules)
         
         dependentFiles.extend(self.shapegrid.getTargetFiles(workDir=workDir))
       
         options = ''
         if self.squid is not None:
            options = "--squid {0}".format(self.squid)
         elif self.ident is not None:
            options = "--ident {0}".format(self.ident)

         pavFname = os.path.join(targetDir, self.getTargetFilename())
         scriptFname = os.path.join(APP_PATH, ProcessType.getTool(self.processType))
         
         shapegridFile = os.path.join(workDir, 
                    os.path.splitext(self.shapegrid.getRelativeDLocation())[0],
                    os.path.basename(self.shapegrid.getDLocation()))
         
         cmdArguments = [os.getenv('PYTHON'), 
                         scriptFname,
                         options,
                         shapegridFile,  
                         inputLayerFname,
                         pavFname,
                         str(self.layer.resolution),
                         str(self.intersectParams[self.INTERSECT_PARAM_MIN_PRESENCE]),
                         str(self.intersectParams[self.INTERSECT_PARAM_MAX_PRESENCE]),
                         str(self.intersectParams[self.INTERSECT_PARAM_MIN_PERCENT])]
         cmd = ' '.join(cmdArguments)
         rules.append(MfRule(cmd, [pavFname], dependencies=dependentFiles))
         
         # Rule for Test/Update 
         status = None
         basename = os.path.join(targetDir, 'mtxcol_{}'.format(self.getId()))
         uRule = self.getUpdateRule(self.getId(), status, basename, [pavFname])
         rules.append(uRule)
         
         # TODO: Post to Solr
         if self.postToSolr:
            postXmlFilename = os.path.join(targetDir, 
                                   'mtxcol_solr_{}.xml'.format(self.getId()))
            solrPostArgs = [
               'LOCAL',
               '$PYTHON',
               os.path.join(APP_PATH, ProcessType.getTool(ProcessType.SOLR_POST)),
               pavFname,
               str(self.getId()), # PAV id
               str(self.layer.getId()), # Projection id
               str(self.parentId), # PAM id
               postXmlFilename
            ]
            solrCmd = ' '.join(solrPostArgs)
            solrRule = MfRule(solrCmd, [postXmlFilename], 
                              dependencies=[pavFname])
            rules.append(solrRule)
         

      return rules

