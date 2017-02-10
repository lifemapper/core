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
import json
import os

from LmCommon.common.lmconstants import ProcessType, JobStatus
from LmServer.base.layer2 import Raster, Vector, _LayerParameters
from LmServer.base.serviceobject2 import ProcessObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule
from LmServer.makeflow.cmd import MfRule

# .............................................................................
# .............................................................................
# .............................................................................
class MatrixColumn(_LayerParameters, ProcessObject):
   # Query to filter layer for intersect
   INTERSECT_PARAM_FILTER_STRING = 'filterString'
   # Attribute used in layer intersect
   INTERSECT_PARAM_VAL_NAME = 'valName'
   # Units of measurement for attribute used in layer intersect
   INTERSECT_PARAM_VAL_UNITS = 'valUnits'
   # Minimum spatial coverage for gridcell intersect computation
   INTERSECT_PARAM_MIN_PERCENT = 'minPercent'
   # Types of GRIM gridcell intersect computation
   INTERSECT_PARAM_WEIGHTED_MEAN = 'weightedMean'
   INTERSECT_PARAM_LARGEST_CLASS = 'largestClass'
   # Minimum percentage of acceptable value for PAM gridcell intersect computation 
   INTERSECT_PARAM_MIN_PRESENCE = 'minPresence'
   # Maximum percentage of acceptable value for PAM gridcell intersect computation 
   INTERSECT_PARAM_MAX_PRESENCE = 'maxPresence'
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrixIndex, matrixId, userId, 
                # inputs if this is connected to a layer and shapegrid 
                layer=None, shapegrid=None, intersectParams={}, 
                colDLocation=None, squid=None, ident=None,
                processType=None, metadata={}, matrixColumnId=None, 
                status=None, statusModTime=None):
      """
      @summary MatrixColumn constructor
      @copydoc LmServer.base.layer2._LayerParameters::__init__()
      @copydoc LmServer.base.serviceobject2.ProcessObject::__init__()
      @param layer: layer input to intersect
      @param shapegrid: grid input to intersect 
      @param intersectParams: parameters input to intersect
      @param colDLocation: location of MatrixColumn (vector) 
      @param squid: species unique identifier for column
      @param ident: (non-species) unique identifier for column
      """
      _LayerParameters.__init__(self, userId, paramId=matrixColumnId, 
                                matrixIndex=matrixIndex, metadata=metadata, 
                                modTime=statusModTime)
      ProcessObject.__init__(self, objId=matrixColumnId, processType=processType, 
                             parentId=matrixId, status=status, 
                             statusModTime=statusModTime)
      self.layer = layer
      self.shapegrid = shapegrid
      self.intersectParams = {}
      self.loadIntersectParams(intersectParams)
      self._colDLocation = None
      self.setColumnDLocation(colDLocation, statusModTime)
      self.squid = squid
      self.ident = ident

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
   def getColumnDLocation(self):
      return self._colDLocation
   
   def setColumnDLocation(self, colDLocation, modTime):
      self._colDLocation = colDLocation
      self.paramModTime = modTime
   
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
   def computeMe(self):
      """
      @summary: Creates a command to intersect a layer and a shapegrid to 
                produce a MatrixColumn.
      """
      rules = []
      # Layer object may be an SDMProject
      if self.layer is not None:
         # Layer input
         dependentFiles = [self.layer.getDLocation()]
         try:
            status = self.layer.status
         except:
            status = JobStatus.COMPLETE
         if JobStatus.waiting(status):
            lyrRules = self.layer.computeMe()
            rules.extend(lyrRules)
            
         # Shapegrid input
         dependentFiles.append(self.shapegrid.getDLocation())
         if JobStatus.waiting(self.shapegrid.status):
            shpgrdRules = self.layer.computeMe()
            rules.extend(shpgrdRules)
       
         options = ''
         if self.squid is not None:
            options = "--squid={0}".format(self.squid)
         elif self.ident is not None:
            options = "--ident={0}".format(self.ident)
         pavFname = self.getColumnDLocation()
         
         cmdArguments = [os.getenv('PYTHON'), 
                         ProcessType.getJobRunner(self.processType), 
                         self.shapegrid.getDLocation(), 
                         self.getDLocation(),
                         pavFname,
                         self.resolution,
                         self.intersectParams[self.INTERSECT_PARAM_MIN_PRESENCE],
                         self.intersectParams[self.INTERSECT_PARAM_MAX_PRESENCE],
                         self.intersectParams[self.INTERSECT_PARAM_MIN_PERCENT],
                         options ]

      cmd = ' '.join(cmdArguments)
      rules.append(MfRule(cmd, [pavFname], dependencies=[dependentFiles]))
        
      return rules


# # .............................................................................
# # .............................................................................
# class MatrixVector(MatrixColumn, Vector):
# # .............................................................................
# # Constructor
# # .............................................................................   
#    def __init__(self, matrixIndex, matrixId, userId, 
#                 # Vector
#                 name, epsgcode, lyrId=None, 
#                 squid=None, verify=None, dlocation=None, 
#                 lyrMetadata={}, dataFormat=None, ogrType=None,
#                 valUnits=None, valAttribute=None, 
#                 nodataVal=None, minVal=None, maxVal=None, 
#                 mapunits=None, resolution=None, bbox=None, svcObjId=None, 
#                 serviceType=LMServiceType.MATRIX_LAYERS, 
#                 moduleType=LMServiceModule.LM,
#                 metadataUrl=None, parentMetadataUrl=None, modTime=None,
#                 featureCount=0, featureAttributes={}, features={}, 
#                 fidAttribute=None,
#                 # MatrixColumn
#                 shapegrid=None, intersectParams={}, 
#                 colDLocation=None, squid=None, ident=None,
#                 # ProcessType could be INTERSECT_VECTOR or INTERSECT_VECTOR_GRIM (later)
#                 processType=ProcessType.INTERSECT_RASTER, mtxcolMetadata={}, 
#                 matrixColumnId=None, status=None, statusModTime=None):
#       """
#       @summary MatrixVector constructor
#       @copydoc LmServer.legion.mtxcolumn.MatrixColumn::__init__()
#       @copydoc LmServer.base.layer2.Vector::__init__()
#       """
#       # ...................
#       MatrixColumn.__init__(self, matrixIndex, matrixId, userId, layerId=lyrId, 
#                    processType=processType, 
#                    metadata=mtxcolMetadata, intersectParams=intersectParams, 
#                    squid=squid, ident=ident, matrixColumnId=matrixColumnId, 
#                    status=status, statusModTime=statusModTime)
#       Vector.__init__(self, name, userId, epsgcode, lyrId=lyrId, squid=squid, 
#                       verify=verify, dlocation=dlocation, metadata=lyrMetadata, 
#                       dataFormat=dataFormat, ogrType=ogrType, valUnits=valUnits, 
#                       valAttribute=valAttribute, nodataVal=nodataVal, 
#                       minVal=minVal, maxVal=maxVal, mapunits=mapunits, 
#                       resolution=resolution, bbox=bbox, svcObjId=matrixColumnId, 
#                       serviceType=serviceType, moduleType=moduleType,
#                       metadataUrl=metadataUrl, 
#                       parentMetadataUrl=parentMetadataUrl, modTime=modTime,
#                       featureCount=featureCount, 
#                       featureAttributes=featureAttributes, features=features, 
#                       fidAttribute=fidAttribute)
#          
# # ...............................................
#    @classmethod
#    def initFromParts(cls, mtxColumn, vector):
#       mtxVct = MatrixVector(mtxColumn.getMatrixIndex(), mtxColumn.parentId, 
#                   vector.getUserId(), vector.name, vector.epsgcode, 
#                   lyrId=vector.getId(), squid=vector.squid, 
#                   verify=vector.verify, dlocation=vector.getDLocation(),
#                   lyrMetadata=vector.lyrMetadata, dataFormat=vector.dataFormat, 
#                   ogrType=vector.gdalType, valUnits=vector.valUnits,
#                   valAttribute=vector.getValAttribute(), 
#                   nodataVal=vector.nodataVal, minVal=vector.minVal, 
#                   maxVal=vector.maxVal, mapunits=vector.mapUnits, 
#                   resolution=vector.resolution, bbox=vector.bbox,
#                   # Join table for MatrixColumn Process Object
#                   processType=mtxColumn.processType, 
#                   mtxcolMetadata=mtxColumn.paramMetadata, 
#                   intersectParams=mtxColumn.intersectParams, 
#                   ident=mtxColumn.ident, matrixColumnId=mtxColumn.getParamId(), 
#                   status=mtxColumn.status, statusModTime=mtxColumn.statusModTime)
#       return mtxVct
# 
# # .............................................................................
# class MatrixRaster(MatrixColumn, Raster):
# # .............................................................................
# # Constructor
# # .............................................................................   
#    def __init__(self, matrixIndex, matrixId, userId, 
#                 # Raster
#                 name, epsgcode, lyrId=None,
#                 squid=None, verify=None, dlocation=None, 
#                 lyrMetadata={}, dataFormat=None, gdalType=None, 
#                 valUnits=None, nodataVal=None, minVal=None, maxVal=None, 
#                 mapunits=None, resolution=None, bbox=None, svcObjId=None, 
#                 serviceType=LMServiceType.MATRIX_LAYERS, 
#                 moduleType=LMServiceModule.LM,
#                 metadataUrl=None, parentMetadataUrl=None, modTime=None,
#                 # MatrixColumn
#                 shapegrid=None, intersectParams={}, 
#                 colDLocation=None, squid=None, ident=None,
#                 # process type could be INTERSECT_RASTER or INTERSECT_RASTER_GRIM
#                 processType=ProcessType.INTERSECT_RASTER, mtxcolMetadata={}, 
#                 matrixColumnId=None, status=None, statusModTime=None):
#       """
#       @summary MatrixRaster constructor
#       @copydoc LmServer.legion.mtxcolumn.MatrixColumn::__init__()
#       @copydoc LmServer.base.layer2.Raster::__init__()
#       """
#       # ...................
#       MatrixColumn.__init__(self, matrixIndex, matrixId, userId, layerId=lyrId,
#                    processType=processType, 
#                    metadata=mtxcolMetadata, intersectParams=intersectParams, 
#                    squid=squid, ident=ident, matrixColumnId=matrixColumnId, 
#                    status=status, statusModTime=statusModTime)
#       Raster.__init__(self, name, userId, epsgcode, lyrId=lyrId, 
#                 squid=squid, verify=verify, dlocation=dlocation, 
#                 metadata=lyrMetadata, dataFormat=dataFormat, gdalType=gdalType, 
#                 valUnits=valUnits, nodataVal=nodataVal, minVal=minVal, 
#                 maxVal=maxVal, mapunits=mapunits, resolution=resolution, 
#                 bbox=bbox, svcObjId=matrixColumnId, 
#                 serviceType=serviceType, moduleType=moduleType,
#                 metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl, 
#                 modTime=modTime)
# 
# # ...............................................
#    @classmethod
#    def initFromParts(cls, mtxColumn, raster):
#       mtxRst = MatrixRaster(mtxColumn.getMatrixIndex(), mtxColumn.parentId, 
#                   raster.getUserId(), raster.name, raster.epsgcode, 
#                   lyrId=raster.getId(), squid=raster.squid, 
#                   verify=raster.verify, dlocation=raster.getDLocation(),
#                   lyrMetadata=raster.lyrMetadata, dataFormat=raster.dataFormat, 
#                   gdalType=raster.gdalType, valUnits=raster.valUnits, 
#                   nodataVal=raster.nodataVal, minVal=raster.minVal, 
#                   maxVal=raster.maxVal, mapunits=raster.mapUnits, 
#                   resolution=raster.resolution, bbox=raster.bbox,
#                   # Join table for MatrixColumn Process Object
#                   processType=mtxColumn.processType, 
#                   mtxcolMetadata=mtxColumn.paramMetadata, 
#                   intersectParams=mtxColumn.intersectParams, 
#                   ident=mtxColumn.ident, matrixColumnId=mtxColumn.getParamId(), 
#                   status=mtxColumn.status, statusModTime=mtxColumn.statusModTime)
#       return mtxRst
