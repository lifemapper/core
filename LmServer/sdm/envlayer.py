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
from LmCommon.common.lmconstants import DEFAULT_EPSG
from LmServer.common.localconstants import ARCHIVE_USER

from LmServer.base.layer import Raster, _LayerParameters
from LmServer.base.lmobj import LMError
from LmServer.base.serviceobject import ServiceObject
from LmServer.common.lmconstants import LMServiceType, LMServiceModule, LMFileType
# .........................................................................
class EnvironmentalType(_LayerParameters, ServiceObject):
# .............................................................................
   """
   """ 
# .............................................................................
   def __init__(self, envType, title, description, userId, keywords=None, modTime=None, 
                environmentalTypeId=None):
      """
      @summary Initialize the _PresenceAbsence class instance
      @param envType: Code for the environmentalLayerType to be used when  
                      matching layers for an SDM to be projected onto to the  
                      layers used when calculating the SDM. 
      @param title: Title of the layer type (short, human-readable)
      @param description: Description of the data this describes.
      @param userId: Id for the owner of this layer type
      @param modTime: Time stamp for creation or modification.
      @param environmentalTypeId: The environmentalTypeId for the database.  
      """
      # lyr.getParametersId() <-- lyr._layerTypeId 
      _LayerParameters.__init__(self, -1, modTime, userId, environmentalTypeId)
      ServiceObject.__init__(self, userId, environmentalTypeId, modTime, modTime, 
                             serviceType=LMServiceType.LAYERTYPES, 
                             moduleType=LMServiceModule.SDM)
      #  typeCode <-- layerType
      self.typeCode = envType
      self.typeTitle = title
      self.typeDescription = description
      self._setTypeKeywords(keywords)
      
# ...............................................
   def _getTypeKeywords(self):
      """
      @summary Get keywords associated with the EnvironmentalType
      @return set of keywords associated with the EnvironmentalType
      """
      return self._typeKeywords
         
   def _setTypeKeywords(self, keywordSequence):
      """
      @summary Set the keywords to be associated with the EnvironmentalType
      @param keywordSequence: sequence of keywords
      """
      if keywordSequence is not None:
         self._typeKeywords = set(keywordSequence)
      else:
         self._typeKeywords = set()
         
   def addTypeKeyword(self, keyword):
      """
      @summary Add a keyword to be associated with the EnvironmentalType
      @param keyword: single keyword to add
      """
      self._typeKeywords.add(keyword)
      
   typeKeywords = property(_getTypeKeywords, _setTypeKeywords)
      

# .........................................................................
class EnvironmentalLayer(EnvironmentalType, Raster):
   """       
   Class to hold a Raster object used for species distribution modeling.
   """
# .............................................................................
   def __init__(self, name, scencode=None, title=None, verify=None,
                minVal=None, maxVal=None, nodataVal=None, valUnits=None,
                isCategorical=False, bbox=None, dlocation=None, metalocation=None,
                gdalType=None, gdalFormat=None, author=None, 
                startDate=None, endDate=None, 
                mapunits=None, resolution=None, epsgcode=DEFAULT_EPSG,
                keywords=None, description=None, isDiscreteData=False,
                layerType=None, layerTypeId=None, layerTypeTitle=None, 
                layerTypeDescription=None, layerTypeModTime=None,
                userId=ARCHIVE_USER, layerId=None, 
                createTime=None, modTime=None, metadataUrl=None ):
      """
      @copydoc Raster::__init__()
      @param layerType: Code for the environmentalLayerType to be used when  
                      matching layers for an SDM to be projected onto to the  
                      layers used when calculating the SDM. 
      @param layerTypeTitle: Title of the layer type (short, human-readable)
      @param layerTypeDescription: Description of the data this describes.
      @param layerTypeUserId: Id for the owner of this layer type
      @param layerTypeModTime: Time stamp for EnvironmentalType creation/modification.
      @param layerTypeId: The environmentalTypeId for the database.  
      """
      if name is None:
         raise LMError(currargs='EnvironmentalLayer.name is required')
      EnvironmentalType.__init__(self, layerType, layerTypeTitle, 
                                 layerTypeDescription, userId, 
                                 keywords=keywords,
                                 modTime=layerTypeModTime, 
                                 environmentalTypeId=layerTypeId)
      self._mapPrefix = None
      # Raster metadataUrl and serviceType override those of EnvironmentalType 
      # if it is a full EnvironmentalLayer
      Raster.__init__(self, name=name, title=title, author=author, bbox=bbox, 
                      startDate=startDate, endDate=endDate, mapunits=mapunits, 
                      resolution=resolution, epsgcode=epsgcode, 
                      dlocation=dlocation, metalocation=metalocation,
                      minVal=minVal, maxVal=maxVal, nodataVal=nodataVal, 
                      valUnits=valUnits, isCategorical=isCategorical,
                      gdalType=gdalType, gdalFormat=gdalFormat, 
                      description=description,
                      isDiscreteData=isDiscreteData, svcObjId=layerId,
                      lyrId=layerId, lyrUserId=userId, verify=verify, 
                      createTime=createTime, modTime=modTime,
                      metadataUrl=metadataUrl, serviceType=LMServiceType.LAYERS, 
                      moduleType=LMServiceModule.SDM)
      self._scenCode = scencode
      self._setMapPrefix(scencode=scencode)

# ...............................................
   @classmethod
   def initFromParts(cls, raster, envType):
      envLyr = EnvironmentalLayer(raster.name, title=raster.title, 
                        minVal=raster.minVal, maxVal=raster.maxVal, 
                        nodataVal=raster.nodataVal, valUnits=raster.valUnits,
                        isCategorical=raster.isCategorical,
                        bbox=raster.bbox, dlocation=raster.getDLocation(), 
                        metalocation=raster.getMetaLocation(),
                        gdalType=raster.gdalType, gdalFormat=raster.dataFormat, 
                        startDate=raster.startDate, endDate=raster.endDate, 
                        mapunits=raster.mapUnits, resolution=raster.resolution, 
                        epsgcode=raster.epsgcode,  
                        description=raster.description, author=raster.author,
                        isDiscreteData=raster.getIsDiscreteData(),
                        layerType=envType.typeCode, 
                        layerTypeId=envType.getParametersId(), 
                        layerTypeTitle=envType.typeTitle, 
                        layerTypeDescription=envType.typeDescription, 
                        keywords=envType.typeKeywords,
                        layerTypeModTime=envType.parametersModTime,
                        userId=raster.getUserId(), layerId=raster.getId(),
                        createTime=raster.createTime, modTime=raster.modTime,
                        metadataUrl=raster.metadataUrl)
      return envLyr

# ...............................................
   def _getKeywords(self):
      """
      @summary Get keywords associated with the layer and EnvironmentalType
      @note: Overrides layer.keywords property
      """
      return self._keywords.union(self.typeKeywords)
         
   def _setKeywords(self, keywordSequence):
      """
      @summary Set the keywords to be associated with the EnvironmentalType
      @param keywordSequence: sequence of keywords
      """
      if keywordSequence is not None:
         self._keywords = set(keywordSequence)
      else:
         self._keywords = set()
         
   def addKeyword(self, keyword):
      """
      @summary Add a keyword to be associated with the EnvironmentalType
      @param keyword: single keyword to add
      """
      self._keywords.add(keyword)
      
   keywords = property(_getKeywords, _setKeywords)

# ...............................................
# other methods
# ...............................................
   def _createMapPrefix(self, scencode=None):
      """
      @summary: Construct the endpoint of a Lifemapper WMS URL for 
                this object.
      @note: Uses the metatadataUrl for this object, plus 'ogc' format, 
             map=<mapname>, and layers=<layername> key/value pairs.  
      @note: If the object has not yet been inserted into the database, a 
             placeholder is used until replacement after database insertion.
      """
      if scencode is not None:
         ftype = LMFileType.SCENARIO_MAP
      else:
         ftype = LMFileType.OTHER_MAP
      mapprefix = self._earlJr.constructMapPrefix(ftype=ftype, 
                     scenarioCode=self._scenCode, lyrname=self.name, 
                     usr=self._userId, epsg=self._epsg)      
      return mapprefix

# ...............................................
   @property
   def mapLayername(self):
      return self.name

# ...............................................
   @property
   def mapPrefix(self): 
      return self._mapPrefix
    
   def _setMapPrefix(self, mapprefix=None, scencode=None):
      if mapprefix is None:
         mapprefix = self._createMapPrefix(scencode=self._scenCode)
      self._mapPrefix = mapprefix
          
# ...............................................
   @property
   def scenCode(self): 
      return self._scenCode
    
   def setScenCode(self, scencode=None):
      self._scenCode = scencode

# ...............................................
   def createLocalMapFilename(self, scencode=None):
      """
      @summary: Find mapfile containing this layer.  
      """
      if scencode is not None:
         ftype = LMFileType.SCENARIO_MAP
      else:
         ftype = LMFileType.OTHER_MAP
      mapfname = self._earlJr.createFilename(ftype, scenarioCode=self._scenCode, 
                                             usr=self._userId, epsg=self._epsg)
      return mapfname
   
# ...............................................
   def setLocalMapFilename(self, mapfname=None):
      """
      @note: Overrides existing _mapFilename
      @summary: Find mapfile containing layers for this model's occurrenceSet.
      @param mapfname: Previously constructed mapfilename
      @param scencode: Scenario code for initial construction of mapfilename. 
      """
      if mapfname is None:
         mapfname = self.createLocalMapFilename(scencode=self._scenCode)
      self._mapFilename = mapfname

# ...............................................
   def setLayerParam(self, envType):
      """ 
      @param envType: an LmServer.sdm.EnvironmentalType object
      """
      # _LayerParameters
      self._matrixIndex = -1
      self.parametersModTime = envType.parametersModTime
      self._parametersId = envType.getParametersId()
      self._parametersUserId = envType.getParametersUserId()
      self.attrFilter = envType.attrFilter
      self.valueFilter = envType.valueFilter
      self.typeCode = envType.typeCode
      self.typeTitle = envType.typeTitle
      self.typeDescription = envType.typeDescription
