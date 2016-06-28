"""
@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
import mx.DateTime
import os 
   
from LmCommon.common.lmconstants import JobStatus, DEFAULT_EPSG, ProcessType

from LmServer.base.dbpgsql import DbPostgresql
from LmServer.base.layer import Raster, Vector
from LmServer.base.taxon import ScientificName
from LmServer.base.layerset import MapLayerSet                                  
from LmServer.base.lmobj import LMError
from LmServer.common.computeResource import LMComputeResource
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (ALGORITHM_DATA, LMServiceModule,
                  DEFAULT_PROJECTION_FORMAT, JobFamily, DB_STORE, ReferenceType)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import ARCHIVE_USER
from LmServer.common.notifyJob import NotifyJob
from LmServer.rad.shapegrid import ShapeGrid
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.envlayer import EnvironmentalType, EnvironmentalLayer
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.scenario import Scenario
from LmServer.sdm.sdmmodel import SDMModel
from LmServer.sdm.sdmprojection import SDMProjection

# .............................................................................
class Borg(DbPostgresql):
   """
   Class to control modifications to the MAL database.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger, dbHost, dbPort, dbUser, dbKey):
      """
      @summary Constructor for MAL class
      @param logger: LmLogger to use for MAL
      @param dbHost: hostname for database machine
      @param dbPort: port for database connection
      """
      DbPostgresql.__init__(self, logger, db=DB_STORE, user=dbUser, 
                            password=dbKey, host=dbHost, port=dbPort)
      earl = EarlJr()
      self._relativeArchivePath = earl.createArchiveDataPath()
      self._webservicePrefix = earl.createWebServicePrefix()()
            
# ...............................................
   def _getRelativePath(self, dlocation=None, url=None):
      relativePath = None
      if dlocation is not None:
         if dlocation.startswith(self._relativeArchivePath):
            relativePath = dlocation[len(self._relativeArchivePath):]
      elif url is not None:
         if url.startswith(self._webservicePrefix):
            relativePath = url[len(self._webservicePrefix)]
      return relativePath

# ...............................................
   def _createScenario(self, row, idxs):
      scen = None
      if row is not None:
         scen = Scenario(self._getColumnValue(row, idxs, ['scenariocode']), 
                         title=self._getColumnValue(row, idxs, ['title']), 
                         author=self._getColumnValue(row, idxs, ['author']), 
                         description=self._getColumnValue(row, idxs, ['description']),
                         metadataUrl=self._getColumnValue(row, idxs, ['metadataurl']), 
                         dlocation=self._getColumnValue(row, idxs, ['dlocation']),
                         startdt=self._getColumnValue(row, idxs, ['startdate']), 
                         enddt=self._getColumnValue(row, idxs, ['enddate']),
                         units=self._getColumnValue(row, idxs, ['units']), 
                         res=self._getColumnValue(row, idxs, ['resolution']), 
                         bbox=self._getColumnValue(row, idxs, ['bbox']), 
                         modTime=self._getColumnValue(row, idxs, ['modtime']),
                         epsgcode=self._getColumnValue(row, idxs, ['epsgcode']),
                         scenarioid=self._getColumnValue(row, idxs, ['scenarioid']))
         keystr = self._getColumnValue(row, idxs, ['keywords'])
         if keystr is not None:
            scen.keywords = keystr.split(',')
      return scen

# ...............................................
   def _createLayerType(self, row, idxs):
      """
      Create an _EnvironmentalType from a LayerType, lm_envlayer,
      lm_envlayerAndKeywords or lm_layerTypeAndKeywords record in the MAL
      """
      lyrType = None
      keywordLst = []
      if row is not None:
         keystr = self._getColumnValue(row, idxs, ['keywords'])
         if keystr is not None and len(keystr) > 0:
            keywordLst = keystr.split(',')
         code = self._getColumnValue(row, idxs, ['typecode', 'code'])
         title = self._getColumnValue(row, idxs, ['typetitle', 'title'])
         desc = self._getColumnValue(row, idxs, ['typedescription', 'description'])
         modtime = self._getColumnValue(row, idxs, ['typemodtime', 'modtime'])
         usr = self._getColumnValue(row, idxs, ['userid'])
         ltid = self._getColumnValue(row, idxs, ['layertypeid'])
                                                
         lyrType = EnvironmentalType(code, title, desc, usr,
                                     keywords=keywordLst,
                                     modTime=modtime, 
                                     environmentalTypeId=ltid)
      return lyrType
   
# ...............................................
   def _createLayer(self, row, idxs):
      """
      Create Raster or Vector layer from a Layer record in the MAL
      """
      lyr = None
      if row is not None:
         dbid = self._getColumnValue(row, idxs, 
                  ['projectionid', 'occurrencesetid', 'layerid'])
         usr = self._getColumnValue(row, idxs, ['lyruserid', 'userid'])
         verify = self._getColumnValue(row, idxs, ['verify'])
         squid = self._getColumnValue(row, idxs, ['squid'])
         name = self._getColumnValue(row, idxs, ['name'])
         title = self._getColumnValue(row, idxs, ['title'])
         author = self._getColumnValue(row, idxs, ['author'])
         desc = self._getColumnValue(row, idxs, ['description'])
         dlocation = self._getColumnValue(row, idxs, ['dlocation'])
         murl = self._getColumnValue(row, idxs, 
                  ['prjmetadataurl', 'occmetadataurl', 'metadataurl'])
         mlocation = self._getColumnValue(row, idxs, ['metalocation'])
         vtype = self._getColumnValue(row, idxs, ['ogrtype'])
         rtype = self._getColumnValue(row, idxs, ['gdaltype'])
         iscat = self._getColumnValue(row, idxs, ['iscategorical'])
         fformat = self._getColumnValue(row, idxs, ['dataformat'])
         epsg = self._getColumnValue(row, idxs, ['epsgcode'])
         munits = self._getColumnValue(row, idxs, ['mapunits'])
         res = self._getColumnValue(row, idxs, ['resolution'])
         sDate = self._getColumnValue(row, idxs, ['startdate'])
         eDate = self._getColumnValue(row, idxs, ['enddate'])
         dtmod = self._getColumnValue(row, idxs, 
                  ['prjstatusmodtime', 'occstatusmodtime', 'datelastmodified', 
                   'statusmodtime'])
         bbox = self._getColumnValue(row, idxs, ['bbox'])
         vattr = self._getColumnValue(row, idxs, ['valattribute'])
         nodata = self._getColumnValue(row, idxs, ['nodataval'])
         minval = self._getColumnValue(row, idxs, ['minval'])
         maxval = self._getColumnValue(row, idxs, ['maxval'])
         vunits = self._getColumnValue(row, idxs, ['valunits'])
                     
         if vtype is not None:
            lyr = Vector(name=name, title=title, bbox=bbox, startDate=sDate, 
                         verify=verify, squid=squid,
                         endDate=eDate, mapunits=munits, resolution=res, 
                         epsgcode=epsg, dlocation=dlocation, 
                         metalocation=mlocation, valAttribute=vattr, 
                         valUnits=vunits, isCategorical=iscat, 
                         ogrType=vtype, ogrFormat=fformat, 
                         author=author, description=desc, 
                         svcObjId=dbid, lyrId=dbid, lyrUserId=usr, 
                         modTime=dtmod, metadataUrl=murl) 
         elif rtype is not None:
            lyr = Raster(name=name, title=title, bbox=bbox, startDate=sDate, 
                         verify=verify, squid=squid,
                         endDate=eDate, mapunits=munits, resolution=res, 
                         epsgcode=epsg, dlocation=dlocation, 
                         metalocation=mlocation, minVal=minval, maxVal=maxval, 
                         nodataVal=nodata, valUnits=vunits, isCategorical=iscat,
                         gdalType=rtype, gdalFormat=fformat, author=author, 
                         description=desc, svcObjId=dbid, lyrId=dbid, lyrUserId=usr, 
                         modTime=dtmod, metadataUrl=murl)
      return lyr
   

# ...............................................
   def _createEnvironmentalLayer(self, row, idxs):
      """
      Create an EnvironmentalLayer from a lm_envlayer record in the MAL
      """
      envRst = None
      if row is not None:
         rst = self._createLayer(row, idxs)
         if rst is not None:
            etype = self._createLayerType(row, idxs)
            envRst = EnvironmentalLayer.initFromParts(rst, etype)
      return envRst


# ...............................................
   def _createShapeGrid(self, row, idxs):
      """
      @note: takes lm_shapegrid record
      """
      shg = None
      if row is not None:
         lyr = self._createLayer(row, idxs)
         shg = ShapeGrid.initFromParts(lyr, 
                        self._getColumnValue(row,idxs,['cellsides']), 
                        self._getColumnValue(row,idxs,['cellsize']), 
                        siteId='siteid', siteX='centerX', siteY='centerY', 
                        size=self._getColumnValue(row,idxs,['vsize']), 
                        shapegridId=self._getColumnValue(row,idxs,['shapegridid']))
      return shg
      
# ...............................................
   def _findOrInsertShapeGridParams(self, shpgrd, cutout):
      """
      @summary: Insert ShapeGrid parameters into the database
      @param shpgrd: The ShapeGrid to insert
      @postcondition: The database contains a new record with shpgrd 
                   attributes.  The shpgrd has _dbId, _dlocation, _parametersId 
                   and metadataUrl populated.
      @note: findShapeGrids should be executed first to ensure that the
             user and shapename combination are unique.
      @raise LMError: on failure to insert or update the database record. 
      """
      shpgrdId = self.executeInsertFunction('lm_insertShapeGrid',
                                             shpgrd.getLayerId(),
                                             shpgrd.cellsides,
                                             shpgrd.cellsize,
                                             shpgrd.size,
                                             shpgrd.siteId,
                                             shpgrd.siteX, shpgrd.siteY,
                                             shpgrd.status,
                                             shpgrd.statusModTime)
      shpgrd.setParametersId(shpgrdId)
      return shpgrd

# ...............................................
   def _findOrInsertBaseLayer(self, lyr):
      min = max = nodata = ltypeid = None
      if isinstance(lyr, EnvironmentalLayer):
         ltypeid = lyr.getParametersId()
      if isinstance(lyr, Raster):
         min = lyr.minVal
         max = lyr.maxVal
         nodata = lyr.nodataVal
      if lyr.epsgcode == DEFAULT_EPSG:
         wkt = lyr.getWkt()
      row, idxs = self.executeInsertFunction('lm_findOrInsertLayer', 
                                         lyr.verify,
                                         lyr.squid,
                                         lyr.getLayerUserId(),
                                         lyr.name,
                                         lyr.title,
                                         lyr.author,
                                         lyr.description,
                                         self._getRelativePath(
                                             dlocation=lyr.getDLocation()),
                                         self._getRelativePath(
                                             dlocation=lyr.getMetaLocation()),
                                         lyr.ogrType,
                                         lyr.gdalType,
                                         lyr.isCategorical,
                                         lyr.dataFormat,
                                         lyr.epsgcode,
                                         lyr.mapUnits,
                                         lyr.resolution,
                                         lyr.startDate,
                                         lyr.endDate,
                                         lyr.modTime,
                                         lyr.getCSVExtentString(), wkt,
                                         lyr.getValAttribute(),
                                         nodata, min, max,
                                         lyr.valUnits,
                                         ltypeid,
                                         self._getRelativePath(
                                             url=lyr.metadataUrl))
      updatedLyr = self._createLayer(row, idxs)
#       if lyrid != -1:
#          lyr.setLayerId(lyrid)
#          lyr.setId(lyrid)
#          lyr.resetMetadataUrl()
#          updatedLyr = lyr
#       else:
#          raise LMError(currargs='Error on adding Layer object (Command: %s)' % 
#                        str(self.lastCommands))
      return updatedLyr

# .............................................................................
# Public functions
# .............................................................................
# ...............................................
   def insertAlgorithm(self, alg):
      """
      @summary Inserts an Algorithm into the database
      @param alg: The algorithm to add
      @note: lm_insertAlgorithm(varchar, varchar, double) returns an int
      """
      alg.modTime = mx.DateTime.utc().mjd
      success = self.executeInsertFunction('lm_insertAlgorithm', alg.code, 
                                           alg.name, alg.modTime)
      return success

# ...............................................
   def findOrInsertTaxonSource(self, taxonSourceName, taxonSourceUrl):
      taxSource = self.executeInsertFunction('lm_findOrInsertTaxonSource', 
                                               taxonSourceName, taxonSourceUrl, 
                                               mx.DateTime.gmt().mjd)
      return taxSource

# ...............................................
   def findOrInsertScenario(self, scen):
      """
      @summary Inserts all scenario layers into the database
      @param scen: The scenario to insert
      """
      scen.modTime = mx.DateTime.utc().mjd
      wkt = None
      if scen.epsgcode == DEFAULT_EPSG:
         wkt = scen.getWkt()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertScenario', 
                           scen.name, scen.title, scen.author, scen.description,
                           self._getRelativePath(url=scen.metadataUrl),
                           scen.startDate, scen.endDate, scen.units, 
                           scen.resolution, scen.epsgcode, 
                           scen.getCSVExtentString(), wkt, 
                           scen.modTime, scen.getUserId())
      newOrExistingScen = self._createScenario(row, idxs)
      if not newOrExistingScen.keywords:
         newOrExistingScen.addKeywords(scen.keywords)
         for kw in newOrExistingScen.keywords:
            successCode = self.executeInsertFunction('lm_joinScenarioKeyword',
                                                newOrExistingScen.getId(), kw)
            if successCode != 0:
               self.log.error('Failed to insert keyword %s for scenario %d' % 
                              (kw, newOrExistingScen.getId()))
      return newOrExistingScen

# ...............................................
   def getEnvironmentalType(self, typeid, typecode, usrid):
      try:
         if typeid is not None:
            row, idxs = self.executeSelectOneFunction('lm_getLayerType', typeid)
         else:
            row, idxs = self.executeSelectOneFunction('lm_getLayerType', 
                                                      usrid, typecode)
      except:
         envType = None
      else:
         envType = self._createLayerType(row, idxs)
      return envType

# ...............................................
   def findOrInsertEnvironmentalType(self, envtype):
      """
      @summary: Insert or find _EnvironmentalType values. Return the record id.
      @param envtype: An EnvironmentalType or EnvironmentalLayer object
      """
      envtype.parametersModTime = mx.DateTime.utc().mjd
      newOrExistingEnvType = self.executeInsertAndSelectOneFunction('lm_findOrInsertLayerType',
                                                    envtype.getParametersId(),
                                                    envtype.getParametersUserId(),
                                                    envtype.typeCode,
                                                    envtype.typeTitle,
                                                    envtype.typeDescription,
                                                    envtype.parametersModTime)
      # Existing EnvType will return with keywords
      if not newOrExistingEnvType.keywords:
         newOrExistingEnvType.typeKeywords = envtype.typeKeywords
         for kw in newOrExistingEnvType.typeKeywords:
            success = self.executeInsertFunction('lm_joinLayerTypeKeyword', 
                                    newOrExistingEnvType.getParametersId(), kw)
            if not success:
               self.log.debug('Failed to insert keyword {} for layertype {}'
                              .format(kw, newOrExistingEnvType.getParametersId()))

      return newOrExistingEnvType
                             
# ...............................................
   def findOrInsertShapeGrid(self, shpgrd, cutout):
      """
      @summary: Find or insert a ShapeGrid into the database
      @param shpgrd: The ShapeGrid to insert
      @postcondition: The database contains a new or existing records for 
                   shapegrid and layer.  The shpgrd object has _dbId, _dlocation, 
                   and metadataUrl populated.
      @note: findShapeGrids should be executed first to ensure that the
             user and shapename combination are unique.
      @raise LMError: on failure to insert or update the database record. 
      """
      if shpgrd.epsgcode == DEFAULT_EPSG:
         wkt = shpgrd.getWkt()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertShapeGrid',
                           shpgrd.verify, shpgrd.getUserId(), shpgrd.name,
                           shpgrd.title, shpgrd.author, shpgrd.description, 
                           self._getRelativePath(dlocation=shpgrd.getDLocation()), 
                           self._getRelativePath(dlocation=shpgrd.getMetaLocation()), 
                           shpgrd.ogrType, shpgrd.dataFormat, shpgrd.epsgcode,
                           shpgrd.mapUnits, shpgrd.resolution, shpgrd.modTime, 
                           shpgrd.getCSVExtentString(), wkt, 
                           self._getRelativePath(url=shpgrd.metadataUrl),
                           shpgrd.cellsides, shpgrd.cellsize, shpgrd.size, 
                           shpgrd.siteId, shpgrd.siteX, shpgrd.siteY, 
                           shpgrd.status, shpgrd.statusModTime)
      updatedShpgrd = self._createShapeGrid(row, idxs)
      return updatedShpgrd

# ...............................................
   def findOrInsertEnvLayer(self, lyr, scenarioId=None):
      """
      @summary Insert or find a layer's metadata in the MAL. 
      @param envLayer: layer to update
      @return: the updated or found EnvironmentalLayer
      @note: layer title and layertype title are the same
      @note: Layer should already have name, filename, and url populated.
      @note: We are setting the layername to the layertype to ensure that they 
             will be unique within a scenario
      @note: lm_insertEnvLayer(...) returns int
      @note lm_insertLayerTypeKeyword(...) returns int
      """
      lyr.modTime = mx.DateTime.utc().mjd
      if lyr.epsgcode == DEFAULT_EPSG:
         wkt = lyr.getWkt()
      row, idxs = self.executeInsertAndSelectOneFunction(
                           'lm_findOrInsertEnvLayer', lyr.verify, lyr.squid,
                           lyr.getUserId(), lyr.name,
                           lyr.title, lyr.author, lyr.description, 
                           self._getRelativePath(dlocation=lyr.getDLocation()), 
                           self._getRelativePath(dlocation=lyr.getMetaLocation()), 
                           lyr.ogrType, lyr.gdalType, lyr.isCategorical, 
                           lyr.dataFormat, lyr.epsgcode,
                           lyr.mapUnits, lyr.resolution, lyr.startTime, 
                           lyr.endTime, lyr.modTime, 
                           lyr.getCSVExtentString(), wkt, 
                           lyr.getValAttribute, lyr.nodataVal, lyr.minVal, 
                           lyr.maxVal, lyr.valUnits, lyr.getParametersId(),
                           self._getRelativePath(url=lyr.metadataUrl),
                           lyr.typeCode, lyr.typeTitle, lyr.typeDescription)
      newOrExistingLyr = self._createEnvLayer(row, idxs)
      # if keywords are returned, layertype was existing
      if not newOrExistingLyr.typeKeywords:
         newOrExistingLyr.typeKeywords = lyr.typeKeywords
         for kw in newOrExistingLyr.typeKeywords:
            success = self.executeInsertFunction('lm_joinLayerTypeKeyword',
                              newOrExistingLyr.getParametersId(), kw)
            if not success:
               self.log.debug('Failed to insert keyword {} for layertype {}'
                              .format(kw, newOrExistingLyr.getParametersId()))
      if scenarioId is not None:
         success = self.executeInsertFunction('lm_joinScenarioLayer', scenarioId, 
                                              newOrExistingLyr.getId()) 
         if not success:
            raise LMError(currargs='Failed to join layer {} to scenario {}'
                           .format(newOrExistingLyr.getId(), scenarioId))
      return newOrExistingLyr

# ...............................................
   def findOrInsertUser(self, usr):
      """
      @summary: Insert a user of the Lifemapper system. 
      @param usr: LMUser object to insert
      @return: True on success, False on failure (i.e. userid is not unique)
      """
      usr.modTime = mx.DateTime.utc().mjd
      newOrExistingUsr = self.executeInsertFunction('lm_findOrInsertUser', 
                              usr.userid, usr.firstName, usr.lastName, 
                              usr.institution, usr.address1, usr.address2, 
                              usr.address3, usr.phone, usr.email, usr.modTime, 
                              usr.getPassword())
      return newOrExistingUsr

   # ...............................................
   def findUser(self, usrid, email):
      """
      @summary: find a user with either a matching userId or email address
      @param usrid: the database primary key of the LMUser in the MAL
      @param email: the email address of the LMUser in the MAL
      @return: a LMUser object
      """
      row, idxs = self.executeSelectOneFunction('lm_findUser', usrid, email)
      usr = self._createUser(row, idxs)
      return usr

