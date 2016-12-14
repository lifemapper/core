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
import mx.DateTime
import os 
   
from LmCommon.common.lmconstants import JobStatus, ProcessType

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
from LmServer.common.localconstants import ARCHIVE_USER, DEFAULT_EPSG
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
   Class to control modifications to the Borg database.
   """
# .............................................................................
# Constructor
# .............................................................................
   def __init__(self, logger, dbHost, dbPort, dbUser, dbKey):
      """
      @summary Constructor for Borg class
      @param logger: LmLogger to use for Borg
      @param dbHost: hostname for database machine
      @param dbPort: port for database connection
      """
      DbPostgresql.__init__(self, logger, db=DB_STORE, user=dbUser, 
                            password=dbKey, host=dbHost, port=dbPort,
                            schema=LM_SCHEMA_BORG)
      earl = EarlJr()
      self._webservicePrefix = earl.createWebServicePrefix()

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
   def _createScientificName(self, row, idxs):
      """
      @summary Returns an ScientificName object from:
                - an ScientificName row
                - an lm_fullScientificName
      @param row: A row of ScientificName data
      @param idxs: Indexes for the row of data
      @return A ScientificName object generated from the information in the row
      """
      sciname = None
      if row is not None:
         scientificname = self._getColumnValue(row, idxs, ['sciname'])
         
         if scientificname is not None:
            taxonid = self._getColumnValue(row, idxs, ['taxonid'])
            taxonomySourceId = self._getColumnValue(row, idxs, ['taxonomysourceid']) 
            usr = self._getColumnValue(row, idxs, ['userid']) 
            srckey = self._getColumnValue(row, idxs, ['taxonomykey'])
            squid = self._getColumnValue(row, idxs, ['squid'])
            kingdom = self._getColumnValue(row, idxs, ['kingdom'])
            phylum = self._getColumnValue(row, idxs, ['phylum']) 
            txClass = self._getColumnValue(row, idxs, ['tx_class'])
            txOrder = self._getColumnValue(row, idxs, ['tx_order'])
            family = self._getColumnValue(row, idxs, ['family'])
            genus = self._getColumnValue(row, idxs, ['genus'])
            rank = self._getColumnValue(row, idxs, ['rank'])
            canonical = self._getColumnValue(row, idxs, ['canonical'])
            genkey = self._getColumnValue(row, idxs, ['genuskey'])
            spkey = self._getColumnValue(row, idxs, ['specieskey'])
            hier = self._getColumnValue(row, idxs, ['keyhierarchy'])
            lcnt = self._getColumnValue(row, idxs, ['lastcount'])
            modtime = self._getColumnValue(row, idxs, ['taxmodtime', 'modtime'])

            sciname = ScientificName(scientificname, 
                                     rank=rank, canonicalName=canonical, 
                                     userId=usr, squid=squid,
                                     kingdom=kingdom, phylum=phylum,  
                                     txClass=txClass, txOrder=txOrder, 
                                     family=family, genus=genus, 
                                     lastOccurrenceCount=lcnt, 
                                     modTime=modtime, 
                                     taxonomySourceId=taxonomySourceId, 
                                     taxonomySourceKey=srckey, 
                                     taxonomySourceGenusKey=genkey, 
                                     taxonomySourceSpeciesKey=spkey, 
                                     taxonomySourceKeyHierarchy=hier,
                                     scientificNameId=taxonid)
      return sciname
   
# ...............................................
   def _createAlgorithm(self, row, idxs):
      """
      Created only from a model, lm_fullModel, or lm_fullProjection 
      """
      code = self._getColumnValue(row, idxs, ['algorithmcode'])
      params = self._getColumnValue(row, idxs, ['algorithmparams'])
      try:
         alg = Algorithm(code, parameters=params)
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
         meta = self._getColumnValue(row, idxs, ['envmetadata', 'metadata'])
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
      Create Raster or Vector layer from a Layer record in the Borg
      """
      lyr = None
      if row is not None:
         dbid = self._getColumnValue(row, idxs, 
                  ['projectionid', 'occurrencesetid', 'layerid'])
         usr = self._getColumnValue(row, idxs, ['lyruserid', 'userid'])
         verify = self._getColumnValue(row, idxs, ['lyrverify', 'verify'])
         squid = self._getColumnValue(row, idxs, ['lyrsquid', 'squid'])
         name = self._getColumnValue(row, idxs, ['lyrname', 'name'])
         dlocation = self._getColumnValue(row, idxs, ['prjdlocation', 
                   'occdlocation', 'lyrdlocation', 'dlocation'])
         murl = self._getColumnValue(row, idxs, ['prjmetadataurl', 
                   'occmetadataurl', 'lyrmetadataurl', 'metadataurl'])
         meta = self._getColumnValue(row, idxs, 
                  ['prjmetadata', 'occmetadata', 'lyrmetadata', 'metadata'])
         vtype = self._getColumnValue(row, idxs, ['ogrtype'])
         rtype = self._getColumnValue(row, idxs, ['gdaltype'])
         vunits = self._getColumnValue(row, idxs, ['valunits'])
         nodata = self._getColumnValue(row, idxs, ['nodataval'])
         minval = self._getColumnValue(row, idxs, ['minval'])
         maxval = self._getColumnValue(row, idxs, ['maxval'])
         fformat = self._getColumnValue(row, idxs, ['dataformat'])
         epsg = self._getColumnValue(row, idxs, ['epsgcode'])
         munits = self._getColumnValue(row, idxs, ['mapunits'])
         res = self._getColumnValue(row, idxs, ['resolution'])
         # for non-joined layer tables OccurrenceSet and Projection 
         dtmod = self._getColumnValue(row, idxs, ['prjstatusmodtime', 
                   'occstatusmodtime', 'statusmodtime', 'lyrmodtime', 'modtime'])
         bbox = self._getColumnValue(row, idxs, ['prjbbox', 'occbbox', 'bbox'])
                     
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
      Create an EnvironmentalLayer from a lm_scenlayer record in the Borg
      """
      envRst = None
      if row is not None:
         scenid = self._getColumnValue(row,idxs,['scenarioid'])
         scencode = self._getColumnValue(row,idxs,['scenariocode'])
         rst = self._createLayer(row, idxs)
         if rst is not None:
            etype = self._createEnvType(row, idxs)
            envRst = EnvironmentalLayer.initFromParts(rst, etype, scencode=scencode)
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
                        status=self._getColumnValue(row,idxs,['status']), 
                        statusModTime=self._getColumnValue(row,idxs,['statusmodtime']),
                        shapegridId=self._getColumnValue(row,idxs,['shapegridid']))
      return shg

# ...............................................
   def _createOccurrenceLayer(self, row, idxs):
      """
      @note: takes lm_shapegrid record
      """
      occ = None
      if row is not None:
         occ = OccurrenceLayer(self._getColumnValue(row,idxs,['displayname']), 
               occId=self._getColumnValue(row,idxs,['occurrencesetid']),
               occMetadata=self._getColumnValue(row,idxs,['occmetadata','metadata']),
               squid=self._getColumnValue(row,idxs,['squid']),
               verify=self._getColumnValue(row,idxs,['occverify','verify']),
               userId=self._getColumnValue(row,idxs,['occuserid','userid']),
               metadataUrl=self._getColumnValue(row,idxs,['occmetadataurl','metadataurl']),
               dlocation=self._getColumnValue(row,idxs,['occdlocation','dlocation']),
               rawDLocation=self._getColumnValue(row,idxs,['rawdlocation']),
               queryCount=self._getColumnValue(row,idxs,['querycount']),
               bbox=self._getColumnValue(row,idxs,['occbbox','bbox']),
               epsgcode=self._getColumnValue(row,idxs,['epsgcode']),
               status=self._getColumnValue(row,idxs,['occstatus','status']),
               statusModTime=self._getColumnValue(row,idxs,['occstatusmodtime','statusmodtime']))
      return occ

# ...............................................
   def _createSDMModel(self, row, idxs):
      """
      @note: takes lm_shapegrid record
      """
      occ = None
      if row is not None:
         priority = None
         occ = self._createOccurrenceLayer(row, idxs)
         scen = Scenario(self._getColumnValue(row, idxs, ['mdlscenariocode', 'scenariocode']), 
                         scenarioid=self._getColumnValue(row, idxs, ['mdlscenarioid', 'scenarioid']))
         algorithm = self._createAlgorithm(row, idxs)
         occ = SDMModel(priority, occ, scen, algorithm, 
                maskId=self._getColumnValue(row, idxs, ['mdlmaskid', 'maskid']), 
                email=self._getColumnValue(row, idxs, ['mdlscenarioid', 'email']), 
                status=self._getColumnValue(row,idxs,['mdlstatus','status']),
                statusModTime=self._getColumnValue(row,idxs,['mdlstatusmodtime','statusmodtime']),
                ruleset=self._getColumnValue(row,idxs,['mdldlocation','dlocation']),
                userId=self._getColumnValue(row,idxs,['occuserid','userid']), 
                modelId=self._getColumnValue(row,idxs,['sdmmodelid']))
      return occ

# ...............................................
   def _createProjection(self, row, idxs):
      """
      @note: takes lm_shapegrid record
      """
      prj = None
      if row is not None:
         mdl = self._createSDMModel(row, idxs)
         scen = Scenario(self._getColumnValue(row, idxs, ['prjscenariocode', 'scenariocode']), 
                         scenarioid=self._getColumnValue(row, idxs, ['prjscenarioid', 'scenarioid']))
         prj = SDMProjection(mdl, scen, 
                  metadata = self._getColumnValue(row, idxs, ['prjmetadata', 'metadata']),
                  maskId=self._getColumnValue(row, idxs, ['prjmaskid', 'maskid']),
                  dlocation=self._getColumnValue(row,idxs,['prjdlocation','dlocation']), 
                  status=self._getColumnValue(row,idxs,['prjstatus','status']),
                  statusModTime=self._getColumnValue(row,idxs,['prjstatusmodtime','statusmodtime']),
                  bbox=self._getColumnValue(row,idxs,['prjbbox','bbox']),
                  epsgcode=self._getColumnValue(row,idxs,['epsgcode']),
                  metadataUrl=self._getColumnValue(row, idxs, ['prjmetadataurl', 'metadataurl']),
                  gdalType=self._getColumnValue(row, idxs, ['gdaltype']), 
                  gdalFormat= self._getColumnValue(row, idxs, ['dataformat']),
                  mapunits=self._getColumnValue(row, idxs, ['mapunits']), 
                  resolution=self._getColumnValue(row, idxs, ['resolution']), 
                  userId=self._getColumnValue(row,idxs,['userid']),
                  projectionId=self._getColumnValue(row,idxs,['projectionid']), 
                  verify=self._getColumnValue(row,idxs,['prjverify', 'verify']), 
                  squid=self._getColumnValue(row,idxs,['squid']))
      return prj

# .............................................................................
# Public functions
# .............................................................................
# ...............................................
   def findOrInsertAlgorithm(self, alg, modtime):
      """
      @summary Inserts an Algorithm into the database
      @param alg: The algorithm to add
      @return: new or existing Algorithm
      """
      if not modtime:
         modtime = mx.DateTime.utc().mjd
      meta = alg.dumpAlgMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertAlgorithm', 
                                               alg.code, meta, modtime)
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
                           lyr.getId(),
                           lyr.getLayerUserId(),
                           lyr.squid,
                           lyr.verify,
                           lyr.name,
                           lyr.getDLocation(),
                           lyr.metadataUrl, meta,                                       meta,
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
   def getBaseLayer(self, lyrid, lyrverify, lyruser, lyrname, epsgcode):
      row, idxs = self.executeSelectOneFunction('lm_getLayer', lyrid, lyrverify, 
                                                lyruser, lyrname, epsgcode)
      lyr = self._createLayer(row, idxs)
      return lyr

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
                           scen.metadataUrl, meta, 
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
      meta = shpgrd.dumpParamMetadata()
      gdaltype = valunits = nodata = min = max = None
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertShapeGrid',
                           shpgrd.getId(), shpgrd.getUserId(), 
                           shpgrd.squid, shpgrd.verify, shpgrd.name,
                           shpgrd.getDLocation(), shpgrd.metadataUrl, meta,
                           shpgrd.dataFormat, gdaltype, shpgrd.ogrType, 
                           valunits, nodata, min, max, 
                           shpgrd.epsgcode, shpgrd.mapUnits, shpgrd.resolution, 
                           shpgrd.getCSVExtentString(), wkt, shpgrd.modTime, 
                           shpgrd.cellsides, shpgrd.cellsize, shpgrd.size, 
                           shpgrd.siteId, shpgrd.siteX, shpgrd.siteY, 
                           shpgrd.status, shpgrd.statusModTime)
      updatedShpgrd = self._createShapeGrid(row, idxs)
      return updatedShpgrd

# ...............................................
   def getShapeGrid(self, shpgridId, lyrId, userId, lyrName, epsg):
      """
      @summary: Find or insert a ShapeGrid into the database
      @param shpgrd: ShapeGrid to insert
      @return: new or existing ShapeGrid.
      """
      row, idxs = self.executeInsertAndSelectOneFunction('lm_getShapeGrid',
                           shpgridId, lyrId, userId, lyrName, epsg)
      shpgrid = self._createShapeGrid(row, idxs)
      return shpgrid
   
# ...............................................
   def findOrInsertEnvLayer(self, lyr, scenarioId):
      """
      @summary Insert or find a layer's metadata in the Borg. 
      @param lyr: layer to insert
      @return: new or existing EnvironmentalLayer
      """
      lyr.modTime = mx.DateTime.utc().mjd
      wkt = None
      if lyr.epsgcode == DEFAULT_EPSG:
         wkt = lyr.getWkt()
      envmeta = lyr.dumpParamMetadata()
      lyrmeta = lyr.dumpLyrMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction(
                           'lm_findOrInsertEnvLayer', scenarioId, lyr.getId(), 
                           lyr.getUserId(), lyr.squid, lyr.verify, lyr.name,
                           lyr.getDLocation(), 
                           lyr.metadataUrl,
                           lyrmeta, lyr.dataFormat,  lyr.gdalType, lyr.ogrType, 
                           lyr.valUnits, lyr.nodataVal, lyr.minVal, lyr.maxVal, 
                           lyr.epsgcode, lyr.mapUnits, lyr.resolution, 
                           lyr.getCSVExtentString(), wkt, lyr.modTime, 
                           lyr.getParametersId(), lyr.typeCode, lyr.gcmCode,
                           lyr.altpredCode, lyr.dateCode, envmeta, 
                           lyr.parametersModTime)
      newOrExistingLyr = self._createEnvLayer(row, idxs)
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
      @param usrid: the database primary key of the LMUser in the Borg
      @param email: the email address of the LMUser in the Borg
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
   
# ...............................................
   def findTaxon(self, taxonSourceId, taxonkey):
      try:
         row, idxs = self.executeSelectOneFunction('lm_findOrInsertTaxon', 
                        taxonSourceId, taxonkey, None, None, None, None, None, 
                        None, None, None, None, None, None, None, None, None, 
                        None, None)
      except Exception, e:
         raise e
      sciname = self._createScientificName(row, idxs)
      return sciname
   
# ...............................................
   def findOrInsertTaxon(self, taxonSourceId, taxonKey, sciName):
      scientificname = None
      currtime = mx.DateTime.gmt().mjd
      usr = squid = kingdom = phylum = cls = ordr = family = genus = None
      rank = canname = sciname = genkey = spkey = keyhierarchy = lastcount = None
      try:
         taxonSourceId = sciName.taxonomySourceId
         taxonKey = sciName.sourceTaxonKey
         usr = sciName.userId
         squid = sciName.squid
         kingdom = sciName.kingdom
         phylum = sciName.phylum
         cls = sciName.txClass
         ordr = sciName.txOrder
         family = sciName.family
         genus = sciName.genus
         rank = sciName.rank
         canname = sciName.canonicalName
         sciname = sciName.scientificName
         genkey = sciName.sourceGenusKey
         spkey = sciName.sourceSpeciesKey
         keyhierarchy = sciName.sourceKeyHierarchy
         lastcount = sciName.lastOccurrenceCount
      except:
         pass
      try:
         row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertTaxon', 
                                                taxonSourceId, taxonKey,
                                                usr, squid, kingdom, phylum,
                                                cls, ordr, family, genus, rank,
                                                canname, sciname, genkey, spkey,
                                                keyhierarchy, lastcount, 
                                                currtime)
      except Exception, e:
         raise e
      else:
         scientificname = self._createScientificName(row, idxs)
      
      return scientificname


# .............................................................................
   def getScenario(self, scenid=None, code=None, usrid=None, fillLayers=False):
      """
      @summary: Return a scenario by its db id or code, filling its layers.  
      @param code: Code for the scenario to be fetched.
      @param scenid: ScenarioId for the scenario to be fetched.
      """
      row, idxs = self.executeSelectOneFunction('lm_getScenario', scenid, usrid, code)
      scen = self._createScenario(row, idxs)
      if scen is not None and fillLayers:
         lyrs = self.getScenarioLayers(scen.getId())
         scen.layers = lyrs
      return scen

# .............................................................................
   def getScenarioLayers(self, scenid):
      """
      @summary: Return a scenario by its db id or code, filling its layers.  
      @param code: Code for the scenario to be fetched.
      @param scenid: ScenarioId for the scenario to be fetched.
      """
      lyrs = []
      rows, idxs = self.executeSelectManyFunction('lm_getEnvLayersForScenario', scenid)
      for r in rows:
         lyr = self._createEnvironmentalLayer(r, idxs)
         lyrs.append(lyr)
      return lyrs
   
# .............................................................................
   def getOccurrenceSet(self, occid, squid, userId, epsg):
      row, idxs = self.executeSelectOneFunction('lm_getOccurrenceSet',
                                                  occid, squid, userId, epsg)
      occ = self._createOccurrenceLayer(row, idxs)
      return occ
   
# ...............................................
   def updateOccurrenceSet(self, occ, polyWkt, pointsWkt):
      """
      @summary Method to update an occurrenceSet object in the MAL database with 
               the verify hash, displayname, dlocation, queryCount, bbox, geom, 
               status/statusmodtime.
      @param occ the occurrences object to update
      @note: queryCount should be updated on the object before calling this;
             geometries should be calculated and sent separately. 
      """
      metadata = occ.dumpLyrMetadata()
      try:
         success = self.executeModifyFunction('lm_updateOccurrenceSet', 
                                              occ.getId(), 
                                              occ.verify,
                                              occ.displayName,
                                              occ.getDLocation(), 
                                              occ.getRawDLocation(), 
                                              occ.queryCount, 
                                              occ.getCSVExtentString(), 
                                              occ.epsgcode, 
                                              metadata,
                                              occ.status, 
                                              occ.statusModTime, 
                                              polyWkt, 
                                              pointsWkt)
      except Exception, e:
         raise e
      return success


# .............................................................................
   def insertMatrixColumn(self, palyr, bktid):
      """
      @summary: Insert a MatrixColumn with optional intersect params and Layer.
                Return the updated (or found) record.
      @return: Method returns a new, updated object.
      """
      updatedlyr = None
      currtime=mx.DateTime.gmt().mjd
      palyr.createTime = currtime
      palyr.modTime = currtime
      rtype, vtype = self._getSpatialLayerType(palyr)
      if rtype is None:
         if vtype is not None:
            try:
               prsOk = palyr.verifyField(palyr.getDLocation(), palyr.dataFormat, 
                                          palyr.attrPresence)
            except LMError:
               raise 
            except Exception, e:
               raise LMError(currargs=e.args)
            
            if not prsOk:
               raise LMError('Field %s of Layer %s is not present or the wrong type' 
                             % (palyr.attrPresence, palyr.name))
            if palyr.attrAbsence is not None:
               try:
                  absOk = palyr.verifyField(palyr.getDLocation(), palyr.dataFormat, 
                                  palyr.attrAbsence)
               except LMError:
                  raise 
               except Exception, e:
                  raise LMError(currargs=e.args)

               if not absOk:
                  raise LMError('Field %s of Layer %s is not present or the wrong type' 
                             % (palyr.attrAbsence, palyr.name))
         else:
            raise LMError('GDAL or OGR data type must be provided')

# ...............................................
   def findOrInsertOccurrenceSet(self, occ):
      """
      @summary: Find existing (from occsetid OR usr/squid/epsg) 
                OR save a new OccurrenceLayer  
      @param occ: New OccurrenceSet to save 
      @return new or existing OccurrenceLayer 
      """
      polywkt = pointswkt = None
      pointtotal = occ.queryCount
      if occ.getFeatures():
         pointtotal = occ.featureCount
         polywkt = occ.getConvexHullWkt()
         pointswkt = occ.getWkt()
         
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertOccurrenceSet', 
                              occ.getId(), occ.getUserId(), occ.squid, 
                              occ.verify, occ.displayName,
                              occ.constructMetadataUrl(),
                              occ.getDLocation(), occ.getRawDLocation(),
                              pointtotal, occ.getCSVExtentString(), occ.epsgcode,
                              occ.dumpLyrMetadata(),
                              occ.status, occ.statusModTime, polywkt, pointswkt)
      newOrExistingOcc = self._createOccurrenceLayer(row, idxs)
      return newOrExistingOcc
