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
from LmCommon.common.lmconstants import PRESENCEABSENCE_LAYERS_SERVICE
from LmServer.base.layer import Raster, Vector, _LayerParameters
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ProcessObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule

# .............................................................................
# .............................................................................
# .............................................................................
class _PresenceAbsence(_LayerParameters):
# .............................................................................
   """
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrixIndex, 
                attrPresence, minPresence, maxPresence, percentPresence, 
                attrAbsence, minAbsence, maxAbsence, percentAbsence, 
                paUserId, paId, attrFilter=None, valueFilter=None):
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
      _LayerParameters.__init__(self, matrixIndex, None, paUserId, paId, 
                                attrFilter=attrFilter, valueFilter=valueFilter)
      self._attrPresenceIdx = None
      self._attrAbsenceIdx = None
      if (attrPresence is None or minPresence is None 
          or maxPresence is None or percentPresence is None):
         raise LMError(currargs='attrPresence, minPresence, maxPresence, percentPresence are required parameters')
      self.attrPresence = attrPresence
      self.minPresence = minPresence
      self.maxPresence = maxPresence 
      self.percentPresence = percentPresence
      self.attrAbsence = attrAbsence
      self.minAbsence = minAbsence
      self.maxAbsence = maxAbsence
      self.percentAbsence = percentAbsence      

# .............................................................................
# PresenceAbsenceVector class (inherits from _PresenceAbsence, Vector)
# .............................................................................
class PresenceAbsenceVector(_PresenceAbsence, Vector, ProcessObject):
   """
   Class to hold information about a PresenceAbsenceVector dataset.  
   """
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, name, title=None, bbox=None,  
                startDate=None, endDate=None, mapunits=None, resolution=None, 
                epsgcode=None, dlocation=None, metalocation=None, 
                ogrType=None, dataFormat=None,
                featureAttributes={}, features={}, fidAttribute=None,
                valAttribute=None, valUnits=None, isCategorical=False,
                description=None, keywords=None, 
                matrixIndex=-1, attrPresence=None, minPresence=None, 
                maxPresence=None, percentPresence=None, attrAbsence=None, 
                minAbsence=None, maxAbsence=None, percentAbsence=None,                
                paUserId=None, paId=None, lyrId=None, lyrUserId=None, paLyrId=None,
                bucketId=None, status=None, statusModTime=None,
                createTime=None, modTime=None, metadataUrl=None):
      """
      @summary Vector constructor, inherits from _Layer
      @param name: Short name, used for layer name in mapfiles
      @param title: Human readable identifier
      @param bounds : geographic boundary of the layer as a string in the format 
                     'minX, minY, maxX, maxY'
      @param startDate: first valid date of the data 
      @param endDate: last valid date of the data
      @param mapunits: mapunits of measurement. These are keywords as used in 
                    mapserver, choice of [feet|inches|kilometers|meters|miles|dd],
                    described in http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
      @param resolution: resolution of the data
      @param keywords: sequence of keywords
      @param epsgcode: integer representing the native EPSG code of this layer
      @param dlocation: Data location interpretable by OGR.  If this is a 
                    shapefile, dlocation should be .shp file with the absolute 
                    path including filename with extension. 
      @param metalocation: File location of metadata associated with this layer.  
      @param ogrType: OGR geometry type (Integer constants corresponding to 
                    ogr.wkbPoint, ogr.wkbPolygon, etc)
      @param featureCount: number of features in this layer.  This is stored in
                    database and may be populated even if the features are not.
      @param featureAttributes: Dictionary with key attributeName and value
                    attributeFeatureType (ogr.OFTString, ogr.OFTReal, etc) for 
                    the features in this dataset 
      @param features: Dictionary of features, where the key is the FID (feature 
                    identifier) and the value is a list of attribute values.  
                    The position of each value in the list corresponds to the 
                    key index in featureAttributes. 
      @param valAttribute: Attribute to be classified when mapping this layer 
      @param modTime: time/date last modified
      @param description: description of the layer
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
      @param userId: Id for the owner of the AncillaryValues defined for this 
                    layer data
      @param ancId: The ancillaryId for the database.  Used to get the 
                    AncillaryValues for intersecting with ShapeGrid.
      @param lyrId: The LayerId for the database (used to get the ServiceObject)
      @param lyrUserId: Id for the owner of the Layer with AncillaryValues 
                    defined on it.
      @param paLyrId: Unique (join) ID for the presenceAbsenceLayer in the experiment
      """
#       self._paLayerId = paLyrId
      _PresenceAbsence.__init__(self, matrixIndex, attrPresence, minPresence, 
                                     maxPresence, percentPresence, attrAbsence, 
                                     minAbsence, maxAbsence, percentAbsence,
                                     modTime, paUserId, paId)
      Vector.__init__(self, name=name, title=title, bbox=bbox, 
                      dlocation=dlocation, metalocation=metalocation,
                      startDate=startDate, endDate=endDate, 
                      mapunits=mapunits, resolution=resolution, 
                      epsgcode=epsgcode, ogrType=ogrType, ogrFormat=dataFormat,
                      featureAttributes=featureAttributes, features=features,
                      fidAttribute=fidAttribute, valAttribute=valAttribute, 
                      valUnits=valUnits, isCategorical=isCategorical,
                      keywords=keywords, description=description, 
                      svcObjId=paLyrId, lyrId=lyrId, lyrUserId=lyrUserId,
                      createTime=createTime, modTime=modTime, metadataUrl=metadataUrl,
                      serviceType=LMServiceType.PRESENCEABSENCE_LAYERS, 
                      moduleType=LMServiceModule.RAD)
      ProcessObject.__init__(self, objId=paLyrId, parentId=bucketId, 
                status=status, statusModTime=statusModTime)
      
# ...............................................
   @classmethod
   def initFromParts(cls, paParam, vlayer, procObj):
      """
      @param paParam: _PresenceAbsence object with parameters for this layer
      @param vlayer: Vector layer object
      @param procObj: ProcessObject
      """
      paLyr = PresenceAbsenceVector(vlayer.name, title=vlayer.title, 
                 bbox=vlayer.bbox,  startDate=vlayer.startDate, 
                 endDate=vlayer.endDate, mapunits=vlayer.mapUnits, 
                 resolution=vlayer.resolution, epsgcode=vlayer.epsgcode, 
                 dlocation=vlayer.getDLocation(), 
                 metalocation=vlayer.getMetaLocation(), 
                 ogrType=vlayer.ogrType, dataFormat=vlayer.dataFormat,
                 featureAttributes=vlayer.getFeatureAttributes(), 
                 features=vlayer.getFeatures(), fidAttribute=vlayer.fidAttribute,
                 valAttribute=vlayer.getValAttribute(), valUnits=vlayer.valUnits, 
                 isCategorical=vlayer.isCategorical,
                 description=vlayer.description, keywords=vlayer.keywords, 
                 matrixIndex=paParam.getMatrixIndex(), 
                 attrPresence=paParam.attrPresence, minPresence=paParam.minPresence, 
                 maxPresence=paParam.maxPresence, 
                 percentPresence=paParam.percentPresence, 
                 attrAbsence=paParam.attrAbsence, 
                 minAbsence=paParam.minAbsence, maxAbsence=paParam.maxAbsence, 
                 percentAbsence=paParam.percentAbsence,                
                 paUserId=paParam.getParametersUserId(), 
                 paId=paParam.getParametersId(), lyrId=vlayer.getLayerId(), 
                 lyrUserId=vlayer.getLayerUserId(), paLyrId=procObj.objId,
                 bucketId=procObj.parentId, 
                 status=procObj.status, statusModTime=procObj.statusModTime,
                 createTime=vlayer.createTime, modTime=vlayer.modTime, 
                 metadataUrl=vlayer.metadataUrl)
      return paLyr
   
   
# .............................................................................
# Private methods
# .............................................................................
   def _setPresenceIndex(self):
      if (self._attrPresenceIdx is None and self._featureAttributes 
          and self.attrPresence):
         for idx, (colname, coltype) in self._featureAttributes.iteritems():
            if colname == self.attrPresence:
               self._attrPresenceIdx = idx

   def _getPresenceIndex(self):
      if self._attrPresenceIdx is None:
         self._setPresenceIndex()
      return self._attrPresenceIdx

# ...............................................
   def _setAbsenceIndex(self):
      if (self._attrAbsenceIdx is None and self._featureAttributes 
          and self.attrAbsence):
         for idx, (colname, coltype) in self._featureAttributes.iteritems():
            if colname == self.attrAbsence:
               self._attrAbsenceIdx = idx

   def _getAbsenceIndex(self):
      if self._attrAbsenceIdx is None:
         self._setAbsenceIndex()
      return self._attrAbsenceIdx

# .............................................................................  
         
# .............................................................................
# PresenceAbsenceVector class (inherits from _PresenceAbsence, Vector)
# .............................................................................
class PresenceAbsenceRaster(_PresenceAbsence, Raster, ProcessObject):
   """
   Class to hold information about a PresenceAbsence Vector dataset. 
   """
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, name, title=None, bbox=None, 
                startDate=None, endDate=None, mapunits=None, resolution=None,  
                epsgcode=None, dlocation=None, metalocation=None, valUnits=None,
                isCategorical=None, gdalType=None, dataFormat=None, 
                keywords=None, description=None, matrixIndex=-1,
                attrPresence=None, minPresence=None, maxPresence=None, 
                percentPresence=None, 
                attrAbsence=None, minAbsence=None, maxAbsence=None, 
                percentAbsence=None, 
                paUserId=None, paId=None, lyrUserId=None, lyrId=None, paLyrId=None,
                bucketId=None, status=None, statusModTime=None,
                createTime=None, modTime=None, metadataUrl=None):
      """
      @summary PresenceAbsenceRaster constructor, inherits from Raster and 
               _PresenceAbsence
      @summary Layer superclass constructor
      @param name: Short name, used for layer name in mapfiles
      @param title: Human readable identifier
      @param bbox : geographic boundary of the layer in one of 3 formats:
                    - a sequence in the format [minX, minY, maxX, maxY]
                    - a string in the format 'minX, minY, maxX, maxY'
                    - a Bounding Box object
      @param startDate: first valid date of the data 
      @param endDate: last valid date of the data
      @param mapunits: mapunits of measurement. These are keywords as used in 
                    mapserver, choice of [feet|inches|kilometers|meters|miles|dd],
                    described in http://mapserver.gis.umn.edu/docs/reference/mapfile/mapObj)
      @param resolution: resolution of the data - pixel size in @units
      @param epsgcode: integer representing the native EPSG code of this layer
      @param dlocation: Data location interpretable by OGR.  If this is a 
                    shapefile, dlocation should be .shp file with the absolute 
                    path including filename with extension. 
      @param metalocation: File location of metadata associated with this layer. 
      @param valUnits: Units for layer values  
      @param isCategorical: Flag indicating grouped data values with no 
             intrinsic ordering
      @param gdalType: GDAL geometry type (Integer constants corresponding to 
                    gdalconst.Int16, gdalconst.GDT_Float32, etc)
      @param dataFormat: GDAL Raster Format code 
                        (http://www.gdal.org/formats_list.html)
      @param keywords: comma delimited list of keywords
      @param description: description of the layer
      @param matrixIndex: Index of the position in presenceAbsence matrix for a 
                      particular layerset.  If this PresenceAbsenceLayer is not
                      a member of a PresenceAbsenceLayerset, this value is -1.
      @param modTime: time/date last modified in Modified Julian Date format
      @param paUserId: Id for the owner of the Parameter values defined for  
                    this layer data
      @param lyrId: The LayerId for the database 
      @param lyrUserId: Id for the owner of the Layer 

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
      @param paId: The PresenceAbsenceId for the database.  Used to get the 
                    PresenceAbsence values for intersecting with ShapeGrid.
      @param paUserId: Id for the owner of the Layer with PresenceAbsence 
                    defined on it.
      @param paLyrId: Unique (join) ID for the presenceAbsenceLayer in the experiment
      """
      _PresenceAbsence.__init__(self, matrixIndex, attrPresence, minPresence, 
                                     maxPresence, percentPresence, attrAbsence, 
                                     minAbsence, maxAbsence, percentAbsence,
                                     paUserId, paId)
      Raster.__init__(self, name=name, title=title, bbox=bbox, 
                      startDate=startDate, endDate=endDate, mapunits=mapunits, 
                      resolution=resolution, epsgcode=epsgcode, 
                      dlocation=dlocation, metalocation=metalocation, 
                      valUnits=valUnits, isCategorical=isCategorical,
                      gdalType=gdalType, gdalFormat=dataFormat, keywords=keywords,
                      description=description, 
                      svcObjId=paLyrId, lyrId=lyrId, lyrUserId=lyrUserId, 
                      createTime=createTime, modTime=modTime, metadataUrl=metadataUrl,
                      serviceType=LMServiceType.PRESENCEABSENCE_LAYERS, 
                      moduleType=LMServiceModule.RAD)
      ProcessObject.__init__(self, objId=paLyrId, parentId=bucketId, 
                status=status, statusModTime=statusModTime)

# ...............................................
   @classmethod
   def initFromParts(cls, paParam, rlayer, procObj):
      """
      @param paParam: _PresenceAbsence object with parameters for this layer
      @param rlayer: Raster layer object
      @param procObj: ProcessObject
      """
      paLyr = PresenceAbsenceRaster(rlayer.name, title=rlayer.title, 
                 bbox=rlayer.bbox,  startDate=rlayer.startDate, 
                 endDate=rlayer.endDate, mapunits=rlayer.mapUnits, 
                 resolution=rlayer.resolution, epsgcode=rlayer.epsgcode, 
                 isCategorical=rlayer.isCategorical, 
                 dlocation=rlayer.getDLocation(), 
                 metalocation=rlayer.getMetaLocation(), 
                 gdalType=rlayer.gdalType, dataFormat=rlayer.dataFormat,
                 
                 description=rlayer.description, keywords=rlayer.keywords, 
                 matrixIndex=paParam.getMatrixIndex(), 
                 attrPresence=paParam.attrPresence, minPresence=paParam.minPresence, 
                 maxPresence=paParam.maxPresence, 
                 percentPresence=paParam.percentPresence, 
                 attrAbsence=paParam.attrAbsence, 
                 minAbsence=paParam.minAbsence, maxAbsence=paParam.maxAbsence, 
                 percentAbsence=paParam.percentAbsence,                
                 paUserId=paParam.getParametersUserId(), 
                 paId=paParam.getParametersId(), lyrId=rlayer.getLayerId(), 
                 lyrUserId=rlayer.getLayerUserId(), paLyrId=procObj.objId,
                 bucketId=procObj.parentId, 
                 status=procObj.status, statusModTime=procObj.statusModTime,
                 createTime=rlayer.createTime, modTime=rlayer.modTime, 
                 metadataUrl=rlayer.metadataUrl)
      return paLyr

