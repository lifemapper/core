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

from LmServer.common.lmconstants import LMServiceType, LMServiceModule, LMFileType
from LmServer.base.layer2 import Raster, _LayerParameters
from LmServer.base.lmobj import LMError
# .........................................................................
class EnvType(_LayerParameters):
# .............................................................................
   def __init__(self, envCode, userId, 
                gcmCode=None, altpredCode=None, dateCode=None, 
                metadata={}, modTime=None, envTypeId=None):
      """
      @summary Initialize the EnvType  class instance
      @copydoc LmServer.base.serviceobject2.ServiceObject::__init__()
      @copydoc LmServer.base.layer2._LayerParameters::__init__()
      @param envCode: Code for the environmental type (i.e. temp, elevation, bio7)
      @param gcmCode: Code for the Global Climate Model used to create these data
      @param altpredCode: Code for the alternate prediction (i.e. IPCC scenario 
             or Representative Concentration Pathways/RCPs) used to create 
             these data
      @param dateCode: Code for the time period for which these data are predicted.
      """
      # lyr.getParametersId() <-- lyr._layerTypeId 
      _LayerParameters.__init__(self, userId, paramId=envTypeId, 
                                metadata=metadata, modTime=modTime)
      self.envCode = envCode
      self.gcmCode = gcmCode
      self.altpredCode = altpredCode
      self.dateCode = dateCode

# .........................................................................
class EnvLayer(EnvType, Raster):
   """       
   Class to hold a Raster object used for species distribution modeling.
   """
# .............................................................................
   def __init__(self, name, userId, epsgcode, scencode=None, lyrId=None, 
                squid=None, verify=None, dlocation=None, 
                lyrMetadata={}, dataFormat=None, gdalType=None, 
                valUnits=None, valAttribute=None, 
                nodataVal=None, minVal=None, maxVal=None, 
                mapunits=None, resolution=None, 
                bbox=None, envLayerId=None, metadataUrl=None, 
                parentMetadataUrl=None, modTime=None,                
                # EnvType
                envCode=None, gcmCode=None, altpredCode=None, dateCode=None, 
                envMetadata={}, envModTime=None, envTypeId=None):
      """
      @copydoc LmServer.base.layer2.Raster::__init__()
      @copydoc LmServer.base.legion.EnvType::__init__()
      """
      if name is None:
         raise LMError(currargs='EnvLayer.name is required')
      EnvType.__init__(self, envCode, userId, 
                gcmCode=gcmCode, altpredCode=altpredCode, dateCode=dateCode, 
                metadata=envMetadata, modTime=envModTime, envTypeId=envTypeId)
      self._mapPrefix = None
      # Raster metadataUrl and serviceType override those of EnvType 
      # if it is a full EnvLayer
      Raster.__init__(self, name, userId, epsgcode, lyrId=lyrId, 
                squid=squid, verify=verify, dlocation=dlocation, 
                metadata=lyrMetadata, dataFormat=dataFormat, gdalType=gdalType, 
                valUnits=valUnits, nodataVal=nodataVal, minVal=minVal, maxVal=maxVal, 
                mapunits=mapunits, resolution=resolution, 
                bbox=bbox, svcObjId=envLayerId, 
                serviceType=LMServiceType.ENVIRONMENTAL_LAYERS, 
                moduleType=LMServiceModule.LM, 
                metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl, 
                modTime=modTime)
      self._scenCode = scencode
      self._setMapPrefix(scencode=scencode)

# ...............................................
   @classmethod
   def initFromParts(cls, raster, envType, envLayerId=None, scencode=None):
      envLyr = EnvLayer(raster.name, raster.getUserId(), raster.epsgcode, 
                  scencode=scencode, lyrId=raster.getId(), squid=raster.squid, 
                  verify=raster.verify, dlocation=raster.getDLocation(),
                  lyrMetadata=raster.lyrMetadata, dataFormat=raster.dataFormat, 
                  gdalType=raster.gdalType, valUnits=raster.valUnits, 
                  nodataVal=raster.nodataVal, minVal=raster.minVal, 
                  maxVal=raster.maxVal, mapunits=raster.mapUnits, 
                  resolution=raster.resolution, bbox=raster.bbox,
                  # Join table for EnvironmentalLayer ServiceObject unique id
                  envLayerId=envLayerId, 
                  metadataUrl=raster.metadataUrl, 
                  parentMetadataUrl=raster.parentMetadataUrl, 
                  modTime=raster.modTime,                
                  # EnvType
                  envCode=envType.envCode, gcmCode=envType.gcmCode, 
                  altpredCode=envType.altpredCode, dateCode=envType.dateCode, 
                  envMetadata=envType.paramMetadata, envModTime=envType.modTime, 
                  envTypeId=envType.getParametersId())
      return envLyr


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
      @param envType: an LmServer.legion.EnvironmentalType object
      """
      # _LayerParameters
      self.envCode=envType.envCode
      self.gcmCode=envType.gcmCode, 
      self.altpredCode=envType.altpredCode
      self.dateCode=envType.dateCode 
      self.loadParamMetadata(envType.paramMetadata)
      self.envModTime=envType.modTime
      self.setParametersId(envType.getParametersId())
      self.parametersModTime = envType.parametersModTime
