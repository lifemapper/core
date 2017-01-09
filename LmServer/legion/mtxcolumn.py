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

from LmCommon.common.lmconstants import ProcessType
from LmServer.base.layer2 import Raster, Vector, _LayerParameters
from LmServer.base.lmobj import LMError, LMObject
from LmServer.base.serviceobject2 import ProcessObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule

# .............................................................................
# .............................................................................
# .............................................................................
class MatrixColumn(_LayerParameters, ProcessObject):
   INTERSECT_PARAM_FILTER_STRING = 'filterString'
   INTERSECT_PARAM_VAL_NAME = 'valName'
   INTERSECT_PARAM_VAL_UNITS = 'valUnits'
   INTERSECT_PARAM_MIN_PERCENT = 'minPercent'
   INTERSECT_PARAM_WEIGHTED_MEAN = 'weightedMean'
   INTERSECT_PARAM_LARGEST_CLASS = 'largestClass' 
   INTERSECT_PARAM_MIN_PRESENCE = 'minPresence'
   INTERSECT_PARAM_MAX_PRESENCE = 'maxPresence'
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrixIndex, matrixId, userId, 
                processType=ProcessType.RAD_INTERSECT, 
                metadata={}, intersectParams={}, squid=None, ident=None,
                matrixColumnId=None, status=None, statusModTime=None):
      """
      @note:  intersectParameters may include keywords:
         filterString: to filter layer for intersect
         valName: for attribute used in layer intersect
         valUnits: units of attribute used in layer intersect
         minPercent: minimum spatial coverage for gridcell intersect computation
         weightedMean: for GRIM gridcell intersect computation
         largestClass: for GRIM gridcell intersect computation
         minPresence: for PAM binary gridcell intersect computation
         maxPresence: for PAM binary gridcell intersect computation
      """
      _LayerParameters.__init__(self, matrixIndex, None, userId, matrixColumnId,
                                metadata=metadata)
      ProcessObject.__init__(self, objId=matrixColumnId, processType=processType, 
                             parentId=matrixId, status=status, 
                             statusModTime=statusModTime)
      self.squid = squid
      self.ident = ident
      self.intersectParams = {}
      self.loadIntersectParams(intersectParams)
      self.loadParamMetadata(metadata)

# ...............................................
   def dumpIntersectParams(self):
      return LMObject._dumpMetadata(self, self.intersectParams)
 
# ...............................................
   def loadIntersectParams(self, newIntersectParams):
      self.intersectParams = LMObject._loadMetadata(self, newIntersectParams)

# ...............................................
   def addIntersectParams(self, newIntersectParams):
      self.intersectParams = LMObject._addMetadata(self, newIntersectParams, 
                                  existingMetadataDict=self.intersectParams)


# .............................................................................
# .............................................................................

# .............................................................................
# .............................................................................
class MatrixVector(MatrixColumn, Vector):
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, matrixIndex, matrixId, userId, 
                # Vector
                name, epsgcode, lyrId=None, 
                squid=None, verify=None, dlocation=None, 
                lyrMetadata={}, dataFormat=None, ogrType=None,
                valUnits=None, valAttribute=None, 
                nodataVal=None, minVal=None, maxVal=None, 
                mapunits=None, resolution=None, 
                bbox=None,
                svcObjId=None, serviceType=LMServiceType.LAYERS, 
                moduleType=LMServiceModule.LM,
                metadataUrl=None, parentMetadataUrl=None, modTime=None,
                featureCount=0, featureAttributes={}, features={}, 
                fidAttribute=None,
                # MatrixColumn
                processType=None, mtxcolMetadata={}, intersectParams={}, 
                ident=None, matrixColumnId=None, status=None, statusModTime=None):
                
      # ...................
      MatrixColumn(matrixIndex, matrixId, userId, 
                   processType=processType, 
                   metadata=mtxcolMetadata, intersectParams=intersectParams, 
                   squid=squid, ident=ident, matrixColumnId=matrixColumnId, 
                   status=status, statusModTime=statusModTime)
      Vector.__init__(self, name, userId, epsgcode, lyrId=lyrId, squid=squid, 
                      verify=verify, dlocation=dlocation, metadata=lyrMetadata, 
                      dataFormat=dataFormat, ogrType=ogrType, valUnits=valUnits, 
                      valAttribute=valAttribute, nodataVal=nodataVal, 
                      minVal=minVal, maxVal=maxVal, mapunits=mapunits, 
                      resolution=resolution, bbox=bbox, svcObjId=matrixColumnId, 
                      serviceType=LMServiceType.MATRIX_LAYERS, 
                      moduleType=LMServiceModule.LM,
                      metadataUrl=metadataUrl, 
                      parentMetadataUrl=parentMetadataUrl, modTime=modTime,
                      featureCount=featureCount, 
                      featureAttributes=featureAttributes, features=features, 
                      fidAttribute=fidAttribute)

      
   
# .............................................................................
# Private methods
# .............................................................................

# .............................................................................  
         
# .............................................................................
class MatrixRaster(MatrixColumn, Raster):
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, matrixIndex, matrixId, userId, 
                # Raster
                name, epsgcode, lyrId=None, 
                squid=None, verify=None, dlocation=None, 
                lyrMetadata={}, dataFormat=None, gdalType=None, 
                valUnits=None, nodataVal=None, minVal=None, maxVal=None, 
                mapunits=None, resolution=None, 
                bbox=None,
                svcObjId=None, serviceType=LMServiceType.LAYERS, 
                moduleType=LMServiceModule.LM,
                metadataUrl=None, parentMetadataUrl=None, modTime=None,
                # MatrixColumn
                processType=None, mtxcolMetadata={}, intersectParams={}, 
                ident=None, matrixColumnId=None, status=None, statusModTime=None):

      # ...................
      MatrixColumn(matrixIndex, matrixId, userId, 
                   processType=processType, 
                   metadata=mtxcolMetadata, intersectParams=intersectParams, 
                   squid=squid, ident=ident, matrixColumnId=matrixColumnId, 
                   status=status, statusModTime=statusModTime)
      Raster.__init__(self, name, userId, epsgcode, lyrId=lyrId, 
                squid=squid, verify=verify, dlocation=dlocation, 
                metadata=lyrMetadata, dataFormat=dataFormat, gdalType=gdalType, 
                valUnits=valUnits, nodataVal=nodataVal, minVal=minVal, 
                maxVal=maxVal, mapunits=mapunits, resolution=resolution, 
                bbox=bbox, svcObjId=matrixColumnId, 
                serviceType=LMServiceType.MATRIX_LAYERS, 
                moduleType=LMServiceModule.LM,
                metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl, 
                modTime=modTime)

