"""
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

from LmCommon.common.lmconstants import MATRIX_LAYERS_SERVICE
from LmServer.base.layer import Raster, Vector, _LayerParameters
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ProcessObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule

# .............................................................................
# .............................................................................
# .............................................................................
class _MatrixColumnParameters(_LayerParameters):
# .............................................................................
   """
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrixIndex, 
                metadata={}, intersectParams={}, squid=None, ident=None,
                matrixColumnId=None, userId=None, matrixId=None,
                status=None, statusModTime=None):
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
      self.intersectParams = {}
      self.loadIntersectParams(intersectParams)

# ...............................................
   def addIntersectParams(self, paramsdict):
      for key, val in paramsdict.iteritems():
         self.intersectParams[key] = val
         
   def dumpIntersectParams(self):
      metastring = None
      if self.intersectParams:
         metastring = json.dumps(self.intersectParams)
      return metastring

   def loadIntersectParams(self, meta):
      if isinstance(meta, dict): 
         self.addIntersectParams(meta)
      else:
         self.intersectParams = json.loads(meta)


# .............................................................................
# .............................................................................
class MatrixColumn(_MatrixColumnParameters, ProcessObject):
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, matrixIndex, 
                mtxcolMetadata={}, intersectParams={}, squid=None, ident=None,
                matrixLayerId=None, userId=None, matrixId=None,
                status=None, statusModTime=None,
                valUnits=None,
                lyrId=None, lyrUserId=None, bucketId=None,
                verify=None, squid=None, modTime=None, metadataUrl=None):
      # ...................
      _MatrixColumnParameters.__init__(self, matrixIndex, 
                metadata=mtxcolMetadata, intersectParams=intersectParams, 
                squid=squid, ident=ident,
                matrixLayerId=matrixLayerId, userId=userId, matrixId=matrixId,
                status=status, statusModTime=statusModTime)
      ProcessObject.__init__(self, objId=matrixLayerId, parentId=bucketId, 
                status=status, statusModTime=statusModTime)

# .............................................................................
# .............................................................................
class MatrixVector(_MatrixColumnParameters, Vector, ProcessObject):
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, matrixIndex, 
                mtxcolMetadata={}, intersectParams={}, squid=None, ident=None,
                matrixLayerId=None, userId=None, matrixId=None,
                status=None, statusModTime=None,
                # Vector
                name=None, lyrMetadata={}, bbox=None, dlocation=None, 
                mapunits=None, resolution=None, epsgcode=None,
                ogrType=None, dataFormat=None, 
                featureCount=0, featureAttributes={}, features={}, fidAttribute=None, 
                valUnits=None,
                lyrId=None, lyrUserId=None, bucketId=None,
                verify=None, squid=None, modTime=None, metadataUrl=None):
      # ...................
      _MatrixColumnParameters.__init__(self, matrixIndex, 
                metadata=mtxcolMetadata, intersectParams=intersectParams, 
                squid=squid, ident=ident,
                matrixLayerId=matrixLayerId, userId=userId, matrixId=matrixId,
                status=status, statusModTime=statusModTime)
      Vector.__init__(self, metadata=lyrMetadata, name=name, bbox=bbox, 
                      dlocation=dlocation,
                      mapunits=mapunits, resolution=resolution, 
                      epsgcode=epsgcode, ogrType=ogrType, ogrFormat=dataFormat,
                      featureAttributes=featureAttributes, features=features,
                      fidAttribute=fidAttribute, 
                      valUnits=valUnits,
                      svcObjId=matrixLayerId, lyrId=lyrId, lyrUserId=lyrUserId,
                      modTime=modTime, metadataUrl=metadataUrl,
                      serviceType=LMServiceType.MATRIX_LAYERS, 
                      moduleType=LMServiceModule.RAD)
      ProcessObject.__init__(self, objId=matrixLayerId, parentId=bucketId, 
                status=status, statusModTime=statusModTime)
      
   
# .............................................................................
# Private methods
# .............................................................................

# .............................................................................  
         
# .............................................................................
class MatrixRaster(_MatrixColumnParameters, Raster, ProcessObject):
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, matrixIndex,
                mtxcolMetadata={}, intersectParams={}, squid=None, ident=None,
                matrixLayerId=None, userId=None, matrixId=None,
                status=None, statusModTime=None,
                # Raster
                lyrMetadata={}, name=None,  
                minVal=None, maxVal=None, nodataVal=None, valUnits=None,
                bbox=None, dlocation=None, 
                gdalType=None, dataFormat=None, 
                mapunits=None, resolution=None, epsgcode=None,
                lyrId=None, lyrUserId=None, bucketId=None,
                verify=None, squid=None,
                modTime=None, metadataUrl=None):
      # ...................
      _MatrixColumnParameters.__init__(self, matrixIndex, 
                metadata=mtxcolMetadata, intersectParams=intersectParams, 
                squid=squid, ident=ident,
                matrixLayerId=matrixLayerId, userId=userId, matrixId=matrixId,
                status=status, statusModTime=statusModTime)
      Raster.__init__(self, metadata=lyrMetadata, name=name,
                      minVal=minVal, maxVal=maxVal, nodataVal=nodataVal, 
                      valUnits=valUnits, bbox=bbox, dlocation=dlocation, 
                      gdalType=gdalType, gdalFormat=dataFormat,  
                      mapunits=mapunits, resolution=resolution, epsgcode=epsgcode,
                      svcObjId=matrixLayerId, lyrId=lyrId, lyrUserId=lyrUserId, 
                      verify=verify, squid=squid, modTime=modTime, 
                      metadataUrl=metadataUrl,
                      serviceType=LMServiceType.MATRIX_LAYERS, 
                      moduleType=LMServiceModule.RAD)
      ProcessObject.__init__(self, objId=matrixLayerId, parentId=bucketId, 
                status=status, statusModTime=statusModTime)

