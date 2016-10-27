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
from LmServer.common.lmconstants import (ALGORITHM_DATA, ARCHIVE_PATH, 
                  LMServiceModule, DEFAULT_PROJECTION_FORMAT, JobFamily, 
                  DB_STORE, ReferenceType, LM_SCHEMA_BORG)
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
                            password=dbKey, host=dbHost, port=dbPort,
                            schema=LM_SCHEMA_BORG)
      earl = EarlJr()
      self._webservicePrefix = earl.createWebServicePrefix()
            
# ...............................................
   def _getRelativePath(self, dlocation=None, url=None):
      relativePath = None
      if dlocation is not None:
         if dlocation.startswith(ARCHIVE_PATH):
            relativePath = dlocation[len(ARCHIVE_PATH):]
      elif url is not None:
         if url.startswith(self._webservicePrefix):
            relativePath = url[len(self._webservicePrefix):]
      return relativePath

# ...............................................
   def _createUser(self, row, idxs):
      usr = None
      if row is not None:
         usr = LMUser(row[idxs['userid']], row[idxs['email']], 
                      row[idxs['password']], isEncrypted=True, 
                      firstName=row[idxs['firstname']], lastName=row[idxs['lastname']], 
                      institution=row[idxs['institution']], 
                      addr1=row[idxs['address1']], addr2=row[idxs['address2']], 
                      addr3=row[idxs['address3']], phone=row[idxs['phone']], 
                      modTime=row[idxs['modtime']])
      return usr
   
# ...............................................
   def _createComputeResource(self, row, idxs):
      cr = None 
      if row is not None:
         cr = LMComputeResource(self._getColumnValue(row, idxs, ['name']), 
                                self._getColumnValue(row, idxs, ['ipaddress']), 
                                self._getColumnValue(row, idxs, ['userid']), 
                                ipSignificantBits=self._getColumnValue(row, idxs, ['ipsigbits']), 
                                FQDN=self._getColumnValue(row, idxs, ['fqdn']), 
                                dbId=self._getColumnValue(row, idxs, ['computeresourceid']), 
                                modTime=self._getColumnValue(row, idxs, ['modtime']), 
                                hbTime=self._getColumnValue(row, idxs, ['lastheartbeat']))
      return cr

# ...............................................
   def _createAlgorithm(self, row, idxs):
      """
      Created only from a model, lm_fullModel, or lm_fullProjection 
      """
      code = self._getColumnValue(row, idxs, ['algorithmcode'])
      name = self._getColumnValue(row, idxs, ['name'])
      params = self._getColumnValue(row, idxs, ['algorithmparams'])
      try:
         alg = Algorithm(code, name=name, parameters=params)
      except:
         alg = None
      return alg
   
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
   def _createEnvType(self, row, idxs):
      """
      Create an _EnvironmentalType from a LayerType, lm_envlayer,
      gcmcode, altpredCode, datecode, metadata

      """
      lyrType = None
      if row is not None:
         envcode = self._getColumnValue(row, idxs, ['envcode'])
         gcmcode = self._getColumnValue(row, idxs, ['gcmcode'])
         altcode = self._getColumnValue(row, idxs, ['altpredcode'])
         dtcode = self._getColumnValue(row, idxs, ['datecode'])
         meta = self._getColumnValue(row, idxs, ['envmetadata, metadata'])
         modtime = self._getColumnValue(row, idxs, ['envmodtime', 'modtime'])
         usr = self._getColumnValue(row, idxs, ['envuserid', 'userid'])
         ltid = self._getColumnValue(row, idxs, ['environmentalTypeId'])
                                                
         lyrType = EnvironmentalType(envcode, None, None, usr,
                                     gcmCode=gcmcode, altpredCode=altcode, 
                                     dateCode=dtcode, metadata=meta, 
                                     modTime=modtime, environmentalTypeId=ltid)
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
         meta = self._getColumnValue(row, idxs, ['occmetadata', 'lyrmetadata'])
         dlocation = self._getColumnValue(row, idxs, ['prjdlocation', 'occdlocation', 'dlocation'])
         murl = self._getColumnValue(row, idxs, 
                  ['prjmetadataurl', 'occmetadataurl', 'metadataurl'])
         murl = self._getColumnValue(row, idxs, 
                  ['prjmetadata', 'occmetadata', 'metadata'])
         vtype = self._getColumnValue(row, idxs, ['ogrtype'])
         rtype = self._getColumnValue(row, idxs, ['gdaltype'])
         fformat = self._getColumnValue(row, idxs, ['dataformat'])
         epsg = self._getColumnValue(row, idxs, ['epsgcode'])
         munits = self._getColumnValue(row, idxs, ['mapunits'])
         res = self._getColumnValue(row, idxs, ['resolution'])
         dtmod = self._getColumnValue(row, idxs, 
                  ['prjstatusmodtime', 'occstatusmodtime', 'statusmodtime'])
         bbox = self._getColumnValue(row, idxs, ['prjbbox', 'occbbox', 'bbox'])
         nodata = self._getColumnValue(row, idxs, ['nodataval'])
         minval = self._getColumnValue(row, idxs, ['minval'])
         maxval = self._getColumnValue(row, idxs, ['maxval'])
         vunits = self._getColumnValue(row, idxs, ['valunits'])
                     
         if vtype is not None:
            lyr = Vector(name=name, metadata=meta, bbox=bbox, 
                         verify=verify, squid=squid,
                         mapunits=munits, resolution=res, 
                         epsgcode=epsg, dlocation=dlocation, 
                         valUnits=vunits, 
                         ogrType=vtype, ogrFormat=fformat, 
                         svcObjId=dbid, lyrId=dbid, lyrUserId=usr, 
                         modTime=dtmod, metadataUrl=murl) 
         elif rtype is not None:
            lyr = Raster(name=name, metadata=meta, bbox=bbox, 
                         verify=verify, squid=squid,
                         mapunits=munits, resolution=res, 
                         epsgcode=epsg, dlocation=dlocation, 
                         minVal=minval, maxVal=maxval, 
                         nodataVal=nodata, valUnits=vunits,
                         gdalType=rtype, gdalFormat=fformat,  
                         svcObjId=dbid, lyrId=dbid, lyrUserId=usr, 
                         modTime=dtmod, metadataUrl=murl)
      return lyr

# ...............................................
   def _createEnvLayer(self, row, idxs):
      """
      Create an EnvironmentalLayer from a lm_envlayer record in the MAL
      """
      envRst = None
      if row is not None:
         rst = self._createLayer(row, idxs)
         if rst is not None:
            etype = self._createEnvType(row, idxs)
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
                        status=self._getColumnValue(row,idxs,['prjstatus', 
                                 'occstatus', 'shpstatus', 'status']), 
                        statusModTime=self._getColumnValue(row,idxs,
                                 ['prjstatusmodtime', 'occstatusmodtime', 
                                  'shpstatusmodtime', 'statusmodtime']),
                        shapegridId=self._getColumnValue(row,idxs,['shapegridid']))
      return shg

# .............................................................................
# Public functions
# .............................................................................
# ...............................................
   def findOrInsertAlgorithm(self, alg):
      """
      @summary Inserts an Algorithm into the database
      @param alg: The algorithm to add
      @return: new or existing Algorithm
      """
      alg.modTime = mx.DateTime.utc().mjd
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertAlgorithm', 
                                               alg.code, alg.name, alg.modTime)
      algo = self._createAlgorithm(row, idxs)
      return algo

# ...............................................
   def findOrInsertTaxonSource(self, taxonSourceName, taxonSourceUrl):
      """
      @summary Finds or inserts a Taxonomy Source record into the database
      @param taxonSourceName: Name for Taxonomy Source
      @param taxonSourceUrl: URL for Taxonomy Source
      @return: record id for the new or existing Taxonomy Source 
      """
      taxSourceId = None
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertTaxonSource', 
                                               taxonSourceName, taxonSourceUrl, 
                                               mx.DateTime.gmt().mjd)
      if row is not None:
         taxSourceId = self._getColumnValue(row,idxs,['taxonomysourceid'])
      return taxSourceId
   
# ...............................................
   def findOrInsertBaseLayer(self, lyr):
      """
      @summary Finds or inserts a Layer record into the database
      @param lyr: Raster or Vector to insert
      @return: new or existing Raster or Vector object 
      """
      min = max = nodata = wkt = None
      if isinstance(lyr, Raster):
         min = lyr.minVal
         max = lyr.maxVal
         nodata = lyr.nodataVal
      meta = lyr.dumpLyrMetadata()
      if lyr.epsgcode == DEFAULT_EPSG:
         wkt = lyr.getWkt()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertLayer', 
                           lyr.getLayerUserId(),
                           lyr.squid,
                           lyr.verify,
                           lyr.name,
                           self._getRelativePath(dlocation=lyr.getDLocation()),
                           self._getRelativePath(url=lyr.metadataUrl), meta,                                       meta,
                           lyr.dataFormat,
                           lyr.gdalType,
                           lyr.ogrType,
                           lyr.valUnits, nodata, min, max,
                           lyr.epsgcode,
                           lyr.mapUnits,
                           lyr.resolution,
                           lyr.getCSVExtentString(), wkt,
                           lyr.modTime)
      updatedLyr = self._createLayer(row, idxs)
      return updatedLyr

# ...............................................
   def findOrInsertScenario(self, scen):
      """
      @summary Inserts all scenario layers into the database
      @param scen: The scenario to insert
      @return: new or existing Scenario
      """
      scen.modTime = mx.DateTime.utc().mjd
      wkt = None
      if scen.epsgcode == DEFAULT_EPSG:
         wkt = scen.getWkt()
      meta = scen.dumpMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertScenario', 
                           scen.getUserId(), scen.code, 
                           self._getRelativePath(url=scen.metadataUrl), meta, 
                           scen.gcmCode, scen.altpredCode, scen.dateCode, 
                           scen.units, scen.resolution, scen.epsgcode, 
                           scen.getCSVExtentString(), wkt, scen.modTime)
      newOrExistingScen = self._createScenario(row, idxs)
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
      @summary: Insert or find _EnvironmentalType values.
      @param envtype: An EnvironmentalType or EnvironmentalLayer object
      @return: new or existing EnvironmentalType
      """
      envtype.parametersModTime = mx.DateTime.utc().mjd
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertEnvironmentalType',
                                                    envtype.getParametersUserId(),
                                                    envtype.getParametersId(),
                                                    envtype.typeCode,
                                                    envtype.typeTitle,
                                                    envtype.typeDescription,
                                                    envtype.parametersModTime)
      newOrExistingEnvType = self._createLayerType(row, idxs)
      # Existing EnvType will return with keywords
      if not newOrExistingEnvType.typeKeywords:
         newOrExistingEnvType.typeKeywords = envtype.typeKeywords
         for kw in newOrExistingEnvType.typeKeywords:
            successCode = self.executeInsertFunction('lm_joinLayerTypeKeyword', 
                                    newOrExistingEnvType.getParametersId(), kw)
            if successCode != 0:
               self.log.debug('Failed to insert keyword {} for layertype {}'
                              .format(kw, newOrExistingEnvType.getParametersId()))

      return newOrExistingEnvType
                             
# ...............................................
   def findOrInsertShapeGrid(self, shpgrd, cutout):
      """
      @summary: Find or insert a ShapeGrid into the database
      @param shpgrd: ShapeGrid to insert
      @return: new or existing ShapeGrid.
      """
      wkt = None
      if shpgrd.epsgcode == DEFAULT_EPSG:
         wkt = shpgrd.getWkt()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertShapeGrid',
                           shpgrd.verify, shpgrd.getUserId(), shpgrd.name,
                           shpgrd.title, shpgrd.author, shpgrd.description, 
                           self._getRelativePath(dlocation=shpgrd.getDLocation()), 
                           self._getRelativePath(dlocation=shpgrd.getMetaLocation()), 
                           shpgrd.ogrType, shpgrd.isCategorical, 
                           shpgrd.dataFormat, shpgrd.epsgcode,
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
      @param lyr: layer to insert
      @return: new or existing EnvironmentalLayer
      """
      lyr.modTime = mx.DateTime.utc().mjd
      wkt = None
      if lyr.epsgcode == DEFAULT_EPSG:
         wkt = lyr.getWkt()
      envmeta = lyr.dumpParamMetadata()
      lyrmeta = lyr.dumpParamMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction(
                           'lm_findOrInsertEnvLayer', lyr.getId(), 
                           lyr.getUserId(), lyr.squid, lyr.verify, lyr.name,
                           self._getRelativePath(dlocation=lyr.getDLocation()), 
                           self._getRelativePath(url=lyr.metadataUrl),
                           lyrmeta, lyr.dataFormat,  lyr.gdalType, lyr.ogrType, 
                           lyr.valUnits, lyr.nodataVal, lyr.minVal, lyr.maxVal, 
                           lyr.epsgcode, lyr.mapUnits, lyr.resolution, 
                           lyr.getCSVExtentString(), wkt, lyr.modTime, 
                           lyr.getParametersId(), lyr.typeCode, lyr.gcmCode,
                           lyr.altpredCode, lyr.dateCode, envmeta, 
                           lyr.parametersModTime)
      newOrExistingLyr = self._createEnvLayer(row, idxs)
      if scenarioId is not None:
         successCode = self.executeInsertFunction('lm_joinScenarioLayer', scenarioId, 
                                              newOrExistingLyr.getId()) 
         if successCode != 0:
            raise LMError(currargs='Failed to join layer {} to scenario {}'
                           .format(newOrExistingLyr.getId(), scenarioId))
      return newOrExistingLyr

# ...............................................
   def findOrInsertComputeResource(self, compResource):
      """
      @summary: Insert a compute resource of this Lifemapper system.  
      @param usr: LMComputeResource object to insert
      @return: new or existing ComputeResource
      """
      compResource.modTime = mx.DateTime.utc().mjd
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertCompute', 
                                        compResource.name, 
                                        compResource.ipAddress, 
                                        compResource.ipSignificantBits, 
                                        compResource.FQDN, 
                                        compResource.getUserId(), 
                                        compResource.modTime)
      newOrExistingCR = self._createComputeResource(row, idxs)
      return newOrExistingCR

# ...............................................
   def findOrInsertUser(self, usr):
      """
      @summary: Insert a user of the Lifemapper system. 
      @param usr: LMUser object to insert
      @return: new or existing LMUser
      """
      usr.modTime = mx.DateTime.utc().mjd
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertUser', 
                              usr.userid, usr.firstName, usr.lastName, 
                              usr.institution, usr.address1, usr.address2, 
                              usr.address3, usr.phone, usr.email, usr.modTime, 
                              usr.getPassword())
      newOrExistingUsr = self._createUser(row, idxs)
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

# .............................................................................
   def countJobChains(self, status, userId=None):      
      """
      @summary: Return the number of jobchains fitting the given filter conditions
      @param status: include only jobs with this status
      @param userId: (optional) include only jobs with this userid
      @return: number of jobs fitting the given filter conditions
      """
      row, idxs = self.executeSelectOneFunction('lm_countJobChains', 
                                                userId, status)
      return self._getCount(row)

# ...............................................
   def findTaxonSource(self, taxonSourceName):
      txSourceId = url = createdate = moddate = None
      if taxonSourceName is not None:
         try:
            row, idxs = self.executeSelectOneFunction('lm_findTaxonSource', 
                                                      taxonSourceName)
         except Exception, e:
            if not isinstance(e, LMError):
               e = LMError(currargs=e.args, lineno=self.getLineno())
            raise e
         if row is not None:
            txSourceId = self._getColumnValue(row, idxs, ['taxonomysourceid'])
            url = self._getColumnValue(row, idxs, ['url'])
            moddate =  self._getColumnValue(row, idxs, ['modtime'])
      return txSourceId, url, moddate
   
# .............................................................................
   def getScenario(self, code=None, scenid=None):
      """
      @summary: Return a scenario by its db id or code, filling its layers.  
      @param code: Code for the scenario to be fetched.
      @param scenid: ScenarioId for the scenario to be fetched.
      """
      if code is not None:
         row, idxs = self.executeSelectOneFunction('lm_getScenarioByCode', code)
      elif scenid is not None:
         row, idxs = self.executeSelectOneFunction('lm_getScenarioById', scenid)
      else:
         raise LMError(currargs='Must provide scenario code or id')
      scen = self._createScenario(row, idxs)
      if scen is not None:
         rows, idxs = self.executeSelectManyFunction('lm_getLayersByScenarioId', 
                                                     scen.getId())
         for r in rows:
            lyr = self._createEnvironmentalLayer(r, idxs)
            scen.addLayer(lyr)
      return scen
                     
