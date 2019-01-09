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
import glob
import mx.DateTime
import os

from LmBackend.command.common import SystemCommand, ChainCommand
from LmBackend.command.server import (IndexPAVCommand, LmTouchCommand, 
                                                  StockpileCommand)
from LmBackend.command.single import (GrimRasterCommand, 
                                          IntersectVectorCommand, IntersectRasterCommand)

from LmCommon.common.lmconstants import ProcessType, JobStatus, LMFormat
from LmCommon.common.matrix import Matrix

from LmServer.base.layer2 import _LayerParameters
from LmServer.base.serviceobject2 import ProcessObject, ServiceObject
from LmServer.common.lmconstants import LMServiceType

# .............................................................................
# .............................................................................
# .............................................................................
# TODO: This should inherit from LmCommon.common.matrix.Matrix
class MatrixColumn(Matrix, _LayerParameters, ServiceObject, ProcessObject):
    # Query to filter layer for intersect
    INTERSECT_PARAM_FILTER_STRING = 'filterString'
    # Attribute used in layer intersect
    INTERSECT_PARAM_VAL_NAME = 'valName'
    # Value of feature for layer intersect (for biogeographic hypotheses)
    INTERSECT_PARAM_VAL_VALUE = 'valValue'
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
                     layer=None, layerId=None, 
                     shapegrid=None, shapeGridId=None, 
                     intersectParams={}, 
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
                                      LMServiceType.MATRIX_COLUMNS, parentId=matrixId, 
                                      modTime=statusModTime)
        ProcessObject.__init__(self, objId=matrixColumnId, processType=processType, 
                                      status=status, statusModTime=statusModTime)
        self.layer = layer
        self._layerId = layerId
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
        elif self._layerId is not None:
            return self._layerId
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
    def updateStatus(self, status, matrixIndex=None, metadata=None, modTime=mx.DateTime.gmt().mjd):
        """
        @summary Update status, matrixIndex, metadata, modTime attributes on the 
                    Matrix layer. 
        @copydoc LmServer.base.serviceobject2.ProcessObject::updateStatus()
        @copydoc LmServer.base.layer2._LayerParameters::updateParams()
        """
        ProcessObject.updateStatus(self, status, modTime)
        _LayerParameters.updateParams(self, modTime, matrixIndex=matrixIndex, 
                                                metadata=metadata)

# ...............................................
    def getTargetFilename(self):
        """
        @summary: Return temporary filename for output.
        """
        relFname = 'mtxcol_{}{}'.format(self.getId(), LMFormat.MATRIX.ext)
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
            
            lyrStatusFname = '{}.status'.format(
                                            os.path.splitext(inputLayerFname)[0])
            # Layer input
            try:
                status = self.layer.status
            except:
                status = JobStatus.COMPLETE

            # TODO: Remove this, layer does not have this method
            if JobStatus.waiting(status):
                lyrRules = self.layer.computeMe(workDir=workDir)
                rules.extend(lyrRules)
            else:
                outfname = os.path.join(os.path.dirname(inputLayerFname), 'touch.out')
                
                touchCmd = LmTouchCommand(outfname)
                
                # Check for vector layers
                if self.layer.ogrType is not None:
                    origVectorFiles = glob.glob('{}*'.format(
                        os.path.splitext(self.layer.getDLocation())[0]))
                    vectorFiles = [os.path.join(
                          targetDir, os.path.basename(fn)) for fn in origVectorFiles]

                    cpCmd = SystemCommand('cp', ' '.join([self.layer.getDLocation(), 
                                                                  inputLayerFname]), 
                                                 #inputs=origVectorFiles, 
                                                 outputs=vectorFiles)
                else:
                    cpCmd = SystemCommand('cp', ' '.join([self.layer.getDLocation(), 
                                                                  inputLayerFname]), 
                                                 #inputs=[self.layer.getDLocation()], 
                                                 outputs=[inputLayerFname])

                touchAndCopyCmd = ChainCommand([touchCmd, cpCmd])
                rules.append(touchAndCopyCmd.getMakeflowRule(local=True))
                
            shpgrdRules = self.shapegrid.computeMe(workDir=workDir)
            rules.extend(shpgrdRules)
            
            
            shapegridFile = os.path.join(workDir, 
                          os.path.splitext(self.shapegrid.getRelativeDLocation())[0],
                          os.path.basename(self.shapegrid.getDLocation()))

            pavFname = os.path.join(targetDir, self.getTargetFilename())

            statusFile = None
            
            if self.processType == ProcessType.INTERSECT_RASTER_GRIM:
                try:
                    minPercent = self.intersectParams[
                        self.INTERSECT_PARAM_MIN_PERCENT]
                except:
                    minPercent = None
                    
                intCmd = GrimRasterCommand(shapegridFile, inputLayerFname,
                                                    pavFname, 
                                                    minPercent=minPercent, ident=self.ident)
            elif self.layer.ogrType is not None:
                intCmd = IntersectVectorCommand(shapegridFile, inputLayerFname,
                                        pavFname, 
                                        self.intersectParams[
                                            self.INTERSECT_PARAM_VAL_NAME],
                                        self.intersectParams[
                                            self.INTERSECT_PARAM_MIN_PRESENCE],
                                        self.intersectParams[
                                            self.INTERSECT_PARAM_MAX_PRESENCE],
                                        self.intersectParams[
                                            self.INTERSECT_PARAM_MIN_PERCENT],
                                        squid=self.squid)
                intCmd.inputs.extend(vectorFiles)
                
            else:
                statusFile = os.path.join(
                    targetDir, 'mtxcol_{}.status'.format(self.getId()))
                intCmd = IntersectRasterCommand(
                    shapegridFile, inputLayerFname, pavFname, 
                    self.intersectParams[self.INTERSECT_PARAM_MIN_PRESENCE],
                    self.intersectParams[self.INTERSECT_PARAM_MAX_PRESENCE],
                    self.intersectParams[self.INTERSECT_PARAM_MIN_PERCENT],
                    squid=self.squid,
                    layerStatusFilename=lyrStatusFname,
                    statusFilename=statusFile)
                
            intCmd.inputs.extend(self.shapegrid.getTargetFiles(workDir=workDir))
            rules.append(intCmd.getMakeflowRule())
            
            # Rule for Test/Update 
            successFilename = os.path.join(targetDir, 
                                                'mtxcol_{}.success'.format(self.getId()))
            spCmd = StockpileCommand(self.processType, self.getId(), 
                                             successFilename, [pavFname], 
                                             statusFilename=statusFile)
            rules.append(spCmd.getMakeflowRule(local=True))
            
            # TODO: Post to Solr
            if self.postToSolr:
                postXmlFilename = os.path.join(targetDir, 
                                              'mtxcol_solr_{}.xml'.format(self.getId()))
                solrCmd = IndexPAVCommand(pavFname, self.getId(), 
                                                  self.layer.getId(), self.parentId, 
                                                  postXmlFilename)
                rules.append(solrCmd.getMakeflowRule(local=True))
        
        return rules

