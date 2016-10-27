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
class _MatrixLayerParameters(_LayerParameters):
# .............................................................................
   """
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrixIndex, 
                metadata={}, intersectParams={}, squid=None, ident=None,
                matrixLayerId=None, userId=None, matrixId=None,
                status=None, statusModTime=None):
      """
      @summary Initialize the _PresenceAbsence class instance
      @param matrixIndex: Index of the position in presenceAbsence matrix for a 
                      particular layerset.  If this PresenceAbsenceLayer is not
                      a member of a PresenceAbsenceLayerset, this value is -1.
      @param attrPresence: Field name of the attribute denoting presence
      @param minPresence: Minimum data value identifying presence.
      @param maxPresence: Maximum data value identifying presence.
      @param percentPresence: Percent of presence within a cell for it to be 
                        considered present.
      @param attrAbsence: Field name of the attribute denoting absence
      @param minAbsence: Minimum data value identifying absence.
      @param maxAbsence: Maximum data value identifying absence.
      @param percentAbsence:  Percent of absence within a cell for it to be 
                        considered present.
      @param paUserId: Id for the owner of these data
      @param paId: The presenceAbsenceId for the database.  Used to get the 
                   PresenceAbsenceValues for intersecting with ShapeGrid.
      """
      _LayerParameters.__init__(self, matrixIndex, None, userId, matrixLayerId)
      self.mtxlyrMetadata = {}
      self.loadMtxLyrMetadata(metadata)
      self.self.intparamsMetadata = {}
      self.loadIntersectParams(intersectParams)

# ...............................................
   def addMtxLyrMetadata(self, metadict):
      for key, val in metadict.iteritems():
         self.mtxlyrMetadata[key] = val
         
   def dumpMtxLyrMetadata(self):
      metastring = None
      if self.mtxlyrMetadata:
         metastring = json.dumps(self.metadata)
      return metastring

   def loadMtxLyrMetadata(self, meta):
      if isinstance(meta, dict): 
         self.addMtxLyrMetadata(meta)
      else:
         self.mtxlyrMetadata = json.loads(meta)

# ...............................................
   def addIntersectParams(self, paramsdict):
      for key, val in paramsdict.iteritems():
         self.intparamsMetadata[key] = val
         
   def dumpIntersectParams(self):
      metastring = None
      if self.intparamsMetadata:
         metastring = json.dumps(self.intparamsMetadata)
      return metastring

   def loadIntersectParams(self, meta):
      if isinstance(meta, dict): 
         self.addIntersectParams(meta)
      else:
         self.intparamsMetadata = json.loads(meta)


# .............................................................................
# PresenceAbsenceVector class (inherits from _PresenceAbsence, Vector)
# .............................................................................
class MatrixVector(_MatrixLayerParameters, Vector, ProcessObject):
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, matrixIndex, 
                mtxlyrMetadata={}, intersectParams={}, squid=None, ident=None,
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
      _MatrixLayerParameters.__init__(self, matrixIndex, 
                metadata=mtxlyrMetadata, intersectParams=intersectParams, 
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
class MatrixRaster(_MatrixLayerParameters, Raster, ProcessObject):
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, matrixIndex,
                mtxlyrMetadata={}, intersectParams={}, squid=None, ident=None,
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
      _MatrixLayerParameters.__init__(self, matrixIndex, 
                metadata=mtxlyrMetadata, intersectParams=intersectParams, 
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

