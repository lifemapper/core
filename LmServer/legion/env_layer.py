"""
@license: gpl2
@copyright: Copyright (C) 2019, University of Kansas Center for Research

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
from LmBackend.common.lmobj import LMError
from LmServer.base.layer import Raster, _LayerParameters
from LmServer.common.lmconstants import LMServiceType, LMFileType


# .........................................................................
class EnvType(_LayerParameters):

# .............................................................................
    def __init__(self, envCode, userId,
                     gcmCode=None, altpredCode=None, dateCode=None,
                     metadata={}, mod_time=None, envTypeId=None):
        """
        @summary Initialize the EnvType  class instance
        @copydoc LmServer.base.layer._LayerParameters::__init__()
        @param envCode: Code for the environmental type (i.e. temp, elevation, bio7)
        @param gcmCode: Code for the Global Climate Model used to create these data
        @param altpredCode: Code for the alternate prediction (i.e. IPCC scenario 
                 or Representative Concentration Pathways/RCPs) used to create 
                 these data
        @param dateCode: Code for the time period for which these data are predicted.
        """
        _LayerParameters.__init__(self, userId, param_id=envTypeId,
                                          metadata=metadata, mod_time=mod_time)
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
                     squid=None, ident=None, verify=None, dlocation=None,
                     lyrMetadata={}, dataFormat=None, gdalType=None,
                     valUnits=None, valAttribute=None,
                     nodataVal=None, minVal=None, maxVal=None,
                     mapunits=None, resolution=None,
                     bbox=None, envLayerId=None, metadataUrl=None,
                     parentMetadataUrl=None, mod_time=None,
                     # EnvType
                     envCode=None, gcmCode=None, altpredCode=None, dateCode=None,
                     envMetadata={}, envModTime=None, envTypeId=None):
        """
        @copydoc LmServer.base.layer.Raster::__init__()
        @copydoc LmServer.base.legion.EnvType::__init__()
        """
        if name is None:
            raise LMError('EnvLayer.name is required')
        EnvType.__init__(self, envCode, userId,
                     gcmCode=gcmCode, altpredCode=altpredCode, dateCode=dateCode,
                     metadata=envMetadata, mod_time=envModTime, envTypeId=envTypeId)
        self._map_prefix = None
        # Raster metadataUrl and serviceType override those of EnvType
        # if it is a full EnvLayer
        Raster.__init__(self, name, userId, epsgcode, lyrId=lyrId,
                     squid=squid, ident=ident, verify=verify, dlocation=dlocation,
                     metadata=lyrMetadata, dataFormat=dataFormat, gdalType=gdalType,
                     valUnits=valUnits, nodataVal=nodataVal, minVal=minVal, maxVal=maxVal,
                     mapunits=mapunits, resolution=resolution,
                     bbox=bbox, svcObjId=envLayerId,
                     serviceType=LMServiceType.ENVIRONMENTAL_LAYERS,
                     metadataUrl=metadataUrl, parentMetadataUrl=parentMetadataUrl,
                     mod_time=mod_time)
        self._scenCode = scencode
        self._set_map_prefix(scencode=scencode)

# ...............................................
    @classmethod
    def init_from_parts(cls, raster, envType, envLayerId=None, scencode=None):
        envLyr = EnvLayer(raster.name, raster.getUserId(), raster.epsgcode,
                        scencode=scencode, lyrId=raster.get_id(), squid=raster.squid,
                        ident=raster.ident, verify=raster.verify, dlocation=raster.get_dlocation(),
                        lyrMetadata=raster.lyrMetadata, dataFormat=raster.dataFormat,
                        gdalType=raster.gdalType, valUnits=raster.valUnits,
                        nodataVal=raster.nodataVal, minVal=raster.minVal,
                        maxVal=raster.maxVal, mapunits=raster.mapUnits,
                        resolution=raster.resolution, bbox=raster.bbox,
                        # Join table for EnvironmentalLayer ServiceObject unique id
                        envLayerId=envLayerId,
                        metadataUrl=raster.metadataUrl,
                        parentMetadataUrl=raster.parentMetadataUrl,
                        mod_time=raster.mod_time,
                        # EnvType
                        envCode=envType.envCode, gcmCode=envType.gcmCode,
                        altpredCode=envType.altpredCode, dateCode=envType.dateCode,
                        envMetadata=envType.paramMetadata, envModTime=envType.paramModTime,
                        envTypeId=envType.getParamId())
        return envLyr

# ...............................................
# other methods
# ...............................................
    def _create_map_prefix(self, scencode=None):
        """
        @summary: Construct the endpoint of a Lifemapper WMS URL for 
                     this object.
        @param scencode: override scenario associated with this layer
        @note: Uses the metatadataUrl for this object, plus 'ogc' format, 
                 map=<mapname>, and layers=<layername> key/value pairs.  
        @note: If the object has not yet been inserted into the database, a 
                 placeholder is used until replacement after database insertion.
        """
        if scencode is None:
            scencode = self._scenCode
        mapprefix = self._earl_jr.constructMapPrefixNew(ftype=LMFileType.SCENARIO_MAP,
                                                                      objCode=scencode,
                                                                      lyrname=self.name,
                                                                      usr=self._userId,
                                                                      epsg=self._epsg)
        return mapprefix

# ...............................................
    @property
    def map_layername(self):
        return self.name

# ...............................................
    @property
    def map_prefix(self):
        return self._map_prefix

    def _set_map_prefix(self, mapprefix=None, scencode=None):
        if mapprefix is None:
            mapprefix = self._create_map_prefix(scencode=self._scenCode)
        self._map_prefix = mapprefix

# ...............................................
    @property
    def scenCode(self):
        return self._scenCode

    def setScenCode(self, scencode=None):
        self._scenCode = scencode

# ...............................................
    def create_local_map_filename(self, scencode=None):
        """
        @summary: Find mapfile containing this layer.
        @param scencode: override scenario associated with this layer
        """
        if scencode is None:
            scencode = self._scenCode
        mapfname = self._earl_jr.create_filename(LMFileType.SCENARIO_MAP,
                                                            objCode=scencode,
                                                            usr=self._userId, epsg=self._epsg)
        return mapfname

# ...............................................
    def set_local_map_filename(self, mapfname=None, scencode=None):
        """
        @note: Overrides existing _map_filename
        @summary: Find mapfile containing layers for this model's occurrenceSet.
        @param mapfname: Previously constructed mapfilename
        @param scencode: override scenario associated with this layer
        """
        if scencode is None:
            scencode = self._scenCode
        if mapfname is None:
            mapfname = self.create_local_map_filename(scencode=scencode)
        self._map_filename = mapfname

# ...............................................
    def set_layer_param(self, envType):
        """ 
        @param envType: an LmServer.legion.EnvironmentalType object
        """
        # _LayerParameters
        self.envCode = envType.envCode
        self.gcmCode = envType.gcmCode,
        self.altpredCode = envType.altpredCode
        self.dateCode = envType.dateCode
        self.loadParamMetadata(envType.paramMetadata)
        self.envModTime = envType.mod_time
        self.setParamId(envType.getParamId())
        self.paramModTime = envType.paramModTime
