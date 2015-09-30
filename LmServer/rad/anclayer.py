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
from LmCommon.common.lmconstants import DEFAULT_EPSG, ANCILLARY_LAYERS_SERVICE

from LmServer.base.layer import _LayerParameters, Vector, Raster
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ProcessObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule

# .............................................................................
class _AncillaryValue(_LayerParameters):
# .............................................................................
   """
   @todo: Update string formatting when python 2.5 is gone
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, matrixIndex, attrValue, weightedMean, largestClass, minPercent, 
                ancUserId, ancId, attrFilter=None, valueFilter=None):
      """
      @summary Initialize the _AncillaryValue class instance
      @param attrValue: Field name of the attribute with the Value of interest
      @param weightedMean: Boolean value identifying whether to use Weighted 
                           Mean to calculate cell value.
      @param largestClass: Boolean value identifying whether to use the Largest
                           Class in a cell to identify a cell's value.  The 
                           parameter minPercent identifies a lower limit to the
                           percentage area of a cell which must be covered by 
                           this class for the class to be used as the 
                           largestClass.
      @param minPercent: Minimum area of a cell which must be covered by a class
                         for it to qualify as the largestClass.  If no class 
                         has the minimum percent, the cell is classified as 
                         NO DATA.
      @param ancUserId: Id for the owner of the AncillaryValues defined for this 
                    layer data
      @param ancId: The ancillaryId for the database.  Used to get the 
                    AncillaryValues for intersecting with ShapeGrid.
      """
      _LayerParameters.__init__(self, matrixIndex, None, ancUserId, ancId, 
                                attrFilter=attrFilter, valueFilter=valueFilter)
      self._attrValueIdx = None
      if (attrValue is None or (weightedMean is None 
          and largestClass is None) or minPercent is None):
         raise LMError(currargs='attrValue, weightedMean, largestClass, minPercent are required parameters')
      self.attrValue = attrValue
      self.weightedMean = weightedMean
      self.largestClass = largestClass
      self.minPercent = minPercent
             

# .............................................................................
# AncillaryVector class (inherits from _AncillaryValue, Vector)
# .............................................................................
class AncillaryVector(_AncillaryValue, Raster, ProcessObject):
   """
   Class to hold information about a AncillaryVector dataset.  
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
                matrixIndex=-1, 
                attrValue=None, weightedMean=False, largestClass=False, 
                minPercent=None, 
                ancUserId=None, ancId=None, lyrId=None, lyrUserId=None, ancLyrId=None,
                bucketId=None, status=None, statusModTime=None,
                createTime=None, modTime=None, metadataUrl=None,
                serviceType=LMServiceType.ANCILLARY_LAYERS, 
                moduleType=LMServiceModule.RAD):
      """
      @copydoc Vector::__init__
      @copydoc _AncillaryValue::__init__
      @param ancLyrId: Unique (join) ID for the ancillaryLayer in the experiment 
      @note: valAttr, valUnits, isCategorical are used for mapping and may be 
                      different than attrValue, which is used for intersect 
                      calculations
                      lyrId=None, lyrUserId=None,
                createTime=None, modTime=None, metadataUrl=None
      """
      _AncillaryValue.__init__(self, matrixIndex, attrValue, weightedMean, 
                               largestClass, minPercent, modTime, 
                               ancUserId, ancId)
      Vector.__init__(self, name=name, title=title, bbox=bbox, 
                      dlocation=dlocation, metalocation=metalocation, 
                      startDate=startDate, endDate=endDate, mapunits=mapunits, 
                      resolution=resolution, epsgcode=epsgcode, 
                      ogrType=ogrType, ogrFormat=dataFormat, 
                      valAttribute=valAttribute, valUnits=valUnits, 
                      isCategorical=isCategorical, keywords=keywords, 
                      description=description, 
                      svcObjId=ancLyrId, lyrId=lyrId, lyrUserId=lyrUserId,
                      createTime=createTime, modTime=modTime, metadataUrl=metadataUrl,
                      serviceType=serviceType, moduleType=moduleType )
      ProcessObject.__init__(self, objId=ancLyrId, parentId=bucketId, 
               status=status, statusModTime=statusModTime)
# ...............................................
   @classmethod
   def initFromParts(cls, ancParam, vlayer, procObj):
      """
      @summary AncillaryVector constructor, inherits from Vector and _AncillaryValue
      @param ancParam: An _AncillaryValue object
      @param vlayer: A Vector object
      @param procObj: ProcessObject
      """
      anclyr = AncillaryVector(vlayer.name, title=vlayer.title, 
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
                 matrixIndex=ancParam.getMatrixIndex(), 
                 attrPresence=ancParam.attrPresence, minPresence=ancParam.minPresence, 
                 maxPresence=ancParam.maxPresence, 
                 percentPresence=ancParam.percentPresence, 
                 attrAbsence=ancParam.attrAbsence, 
                 minAbsence=ancParam.minAbsence, maxAbsence=ancParam.maxAbsence, 
                 percentAbsence=ancParam.percentAbsence,                
                 paUserId=ancParam.getParametersUserId(), 
                 paId=ancParam.getParametersId(), lyrId=vlayer.getLayerId(), 
                 lyrUserId=vlayer.getLayerUserId(), ancLyrId=procObj.objId,
                 bucketId=procObj.parentId, 
                 status=procObj.status, statusModTime=procObj.statusModTime,
                 createTime=vlayer.createTime, modTime=vlayer.modTime, 
                 metadataUrl=vlayer.metadataUrl)
      return anclyr

# .............................................................................
# Private methods
# .............................................................................
   def _setValueIndex(self):
      if (self._attrPresenceIdx is None and self._featureAttributes 
          and self.attrPresence):
         for idx, (colname, coltype) in self._featureAttributes.iteritems():
            if colname == self.attrPresence:
               self._attrPresenceIdx = idx

   def _getValueIndex(self):
      if self._attrPresenceIdx is None:
         self._setPresenceIndex()
      return self._attrPresenceIdx

# .............................................................................
# Public methods
# .............................................................................

# .............................................................................
# AncillaryRaster class (inherits from _AncillaryValue, Raster)
# .............................................................................
class AncillaryRaster(_AncillaryValue, Raster, ProcessObject):
   """
   Class to hold information about a AncillaryRaster dataset.  
   """
# .............................................................................
# Constructor
# .............................................................................   
   def __init__(self, name, title=None, bbox=None, startDate=None, 
                endDate=None, mapunits=None, resolution=None, 
                epsgcode=None, dlocation=None, metalocation=None, 
                valUnits=None, isCategorical=None, 
                gdalType=None, dataFormat=None, keywords=None, description=None,
                matrixIndex=-1, attrValue=None, weightedMean=False, 
                largestClass=False, minPercent=None, 
                ancUserId=None, ancId=None, lyrUserId=None, lyrId=None, ancLyrId=None,
                bucketId=None, status=None, statusModTime=None,
                createTime=None, modTime=None, metadataUrl=None):
      """
      @summary Vector constructor, inherits from _Layer
      @copydoc Vector::__init__
      @copydoc _AncillaryValue::__init__
      @param lyrUserId: Vector @userId 
      @param ancLyrId: Unique (join) ID for the ancillaryLayer in the experiment 
      """
      _AncillaryValue.__init__(self, matrixIndex, attrValue, weightedMean, 
                               largestClass, minPercent, ancUserId, ancId)
      Raster.__init__(self, name=name, title=title, bbox=bbox, 
               startDate=startDate, endDate=endDate, mapunits=mapunits, 
               resolution=resolution, epsgcode=epsgcode, dlocation=dlocation, 
               metalocation=metalocation, valUnits=valUnits, 
               isCategorical=isCategorical, gdalType=gdalType, 
               gdalFormat=dataFormat, keywords=keywords, description=description, 
               svcObjId=ancLyrId, lyrId=lyrId, lyrUserId=lyrUserId, 
               createTime=createTime, modTime=modTime, metadataUrl=metadataUrl,
               serviceType=LMServiceType.ANCILLARY_LAYERS, 
               moduleType=LMServiceModule.RAD)
      ProcessObject.__init__(self, objId=ancLyrId, parentId=bucketId, 
               status=status, statusModTime=statusModTime)
         
# ...............................................
   @classmethod
   def initFromParts(cls, ancParam, rlayer, procObj):
      """
      @param ancParam: _AncillaryValue object with parameters for this layer
      @param rlayer: Raster layer object
      @param procObj: ProcessObject
      """
      anclyr = AncillaryRaster(rlayer.name, title=rlayer.title, 
                 bbox=rlayer.bbox,  startDate=rlayer.startDate, 
                 endDate=rlayer.endDate, mapunits=rlayer.mapUnits, 
                 resolution=rlayer.resolution, epsgcode=rlayer.epsgcode, 
                 dlocation=rlayer.getDLocation(), 
                 metalocation=rlayer.getMetaLocation(), 
                 ogrType=rlayer.ogrType, dataFormat=rlayer.dataFormat,
                 featureAttributes=rlayer.getFeatureAttributes(), 
                 features=rlayer.getFeatures(), fidAttribute=rlayer.fidAttribute,
                 valAttribute=rlayer.getValAttribute(), valUnits=rlayer.valUnits, 
                 isCategorical=rlayer.isCategorical,
                 description=rlayer.description, keywords=rlayer.keywords, 
                 matrixIndex=ancParam.getMatrixIndex(), 
                 attrPresence=ancParam.attrPresence, minPresence=ancParam.minPresence, 
                 maxPresence=ancParam.maxPresence, 
                 percentPresence=ancParam.percentPresence, 
                 attrAbsence=ancParam.attrAbsence, 
                 minAbsence=ancParam.minAbsence, maxAbsence=ancParam.maxAbsence, 
                 percentAbsence=ancParam.percentAbsence,                
                 paUserId=ancParam.getParametersUserId(), 
                 paId=ancParam.getParametersId(), lyrId=rlayer.getLayerId(), 
                 lyrUserId=rlayer.getLayerUserId(), ancLyrId=procObj.objId,
                 bucketId=procObj.parentId, 
                 status=procObj.status, statusModTime=procObj.statusModTime,
                 createTime=rlayer.createTime, modTime=rlayer.modTime, 
                 metadataUrl=rlayer.metadataUrl)
      return anclyr
    
# .............................................................................
# Public methods
# .............................................................................
