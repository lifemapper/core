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
   
from LmCommon.common.lmconstants import ProcessType
from LmServer.base.dbpgsql import DbPostgresql
from LmServer.base.layer2 import Raster, Vector
from LmServer.base.taxon import ScientificName
from LmServer.base.lmobj import LMError
from LmServer.common.computeResource import LMComputeResource
from LmServer.common.datalocator import EarlJr
from LmServer.common.lmconstants import (GDALFormatCodes, OGRFormatCodes, 
                                         DB_STORE, LM_SCHEMA_BORG)
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import SCENARIO_PACKAGE_EPSG
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.envlayer import EnvType, EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.sdmproj import SDMProjection

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
      params = self._getColumnValue(row, idxs, ['algparams'])
      try:
         alg = Algorithm(code, parameters=params)
      except:
         alg = None
      return alg
   
# ...............................................
   def _createMFChain(self, row, idxs):
      mfchain = None
      if row is not None:
         mfchain = MFChain(self._getColumnValue(row, idxs, ['userid']), 
                     dlocation=self._getColumnValue(row, idxs, ['dlocation']), 
                     priority=self._getColumnValue(row, idxs, ['priority']), 
                     metadata=self._getColumnValue(row, idxs, ['metadata']),  
                     status=self._getColumnValue(row, idxs, ['status']), 
                     statusModTime=self._getColumnValue(row, idxs, 
                                                        ['statusmodtime']), 
                     mfChainId=self._getColumnValue(row, idxs, ['mfprocessid']))
      return mfchain
   
# ...............................................
   def _createScenario(self, row, idxs, isForModel=True):
      """
      @note: created only from Scenario table or lm_sdmproject view
      """
      scen = None
      if isForModel:
         scenid = self._getColumnValue(row, idxs, ['mdlscenarioid', 'scenarioid'])
         scencode = self._getColumnValue(row, idxs, ['mdlscenariocode', 'scenariocode'])
         meta = self._getColumnValue(row, idxs, ['mdlscenmetadata', 'metadata'])
         gcmcode = self._getColumnValue(row, idxs, ['mdlscengcmcode', 'gcmcode'])
         altpredcode = self._getColumnValue(row, idxs, ['mdlscenaltpredcode', 'altpredcode'])
         datecode = self._getColumnValue(row, idxs, ['mdlscendatecode', 'datecode'])
      else:
         scenid = self._getColumnValue(row, idxs, ['prjscenarioid', 'scenarioid'])
         scencode = self._getColumnValue(row, idxs, ['prjscenariocode', 'scenariocode'])
         meta = self._getColumnValue(row, idxs, ['prjscenmetadata', 'metadata'])
         gcmcode = self._getColumnValue(row, idxs, ['prjscengcmcode', 'gcmcode'])
         altpredcode = self._getColumnValue(row, idxs, ['prjscenaltpredcode', 'altpredcode'])
         datecode = self._getColumnValue(row, idxs, ['prjscendatecode', 'datecode'])
         
      usr = self._getColumnValue(row, idxs, ['userid'])
      metaurl = self._getColumnValue(row, idxs, ['metadataurl'])
      meta = self._getColumnValue(row, idxs, ['metadata'])
      units = self._getColumnValue(row, idxs, ['units'])
      res = self._getColumnValue(row, idxs, ['resolution'])
      epsg = self._getColumnValue(row, idxs, ['epsgcode'])
      bbox = self._getColumnValue(row, idxs, ['bbox'])
      modtime = self._getColumnValue(row, idxs, ['modtime'])
    
      if row is not None:
         scen = Scenario(scencode, metadata=meta, metadataUrl=metaurl, 
                     units=units, res=res, 
                     gcmCode=gcmcode, altpredCode=altpredcode, dateCode=datecode,
                     bbox=bbox, modTime=modtime, epsgcode=epsg,
                     layers=None, userId=usr, scenarioid=scenid)
      return scen

# ...............................................
   def _createEnvType(self, row, idxs):
      """
      @summary: Create an _EnvironmentalType from a database EnvType record, or 
                lm_envlayer, lm_scenlayer view
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
         ltid = self._getColumnValue(row, idxs, ['envtypeid'])
         lyrType = EnvType(envcode, usr, gcmCode=gcmcode, altpredCode=altcode, 
                           dateCode=dtcode, metadata=meta, modTime=modtime, 
                           envTypeId=ltid)
      return lyrType
   
# ...............................................
   def _createGridset(self, row, idxs):
      """
      @summary: Create a Gridset from a database Gridset record or lm_gridset view
      """
      grdset = None
      if row is not None:
         shp = self._createShapeGrid(row, idxs)
         shpId = self._getColumnValue(row, idxs, ['layerid'])
         grdid = self._getColumnValue(row, idxs, ['gridsetid'])
         usr = self._getColumnValue(row, idxs, ['userid'])
         name = self._getColumnValue(row, idxs, ['grdname', 'name'])
         murl = self._getColumnValue(row, idxs, ['grdmetadataurl', 'metadataurl'])
         dloc = self._getColumnValue(row, idxs, ['grddlocation', 'dlocation'])
         epsg = self._getColumnValue(row, idxs, ['grdepsgcode', 'epsgcode'])
         meta = self._getColumnValue(row, idxs, ['grdmetadata', 'metadata'])
         mtime = self._getColumnValue(row, idxs, ['grdmodtime', 'modtime'])
         grdset = Gridset(name=name, metadata=meta, shapeGrid=shp, 
                          shapeGridId=shpId, configFilename=dloc, epsgcode=epsg, userId=usr, 
                          gridsetId=grdid, metadataUrl=murl, modTime=mtime)
      return grdset
   
# ...............................................
   def _createLMMatrix(self, row, idxs):
      """
      @summary: Create an LMMatrix from a database Matrix record, or lm_matrix,
                lm_fullMatrix or lm_gridset view
      """
      mtx = None
      if row is not None:
         grdset = self._createGridset(row, idxs)
         mtxid = self._getColumnValue(row, idxs, ['matrixid'])
         mtype = self._getColumnValue(row, idxs, ['matrixtype'])
         gcm = self._getColumnValue(row, idxs, ['gcmcode']) 
         rcp  = self._getColumnValue(row, idxs, ['altpredcode'])
         dt =  self._getColumnValue(row, idxs, ['datecode'])
         dloc = self._getColumnValue(row, idxs, ['matrixiddlocation'])
         meta = self._getColumnValue(row, idxs, ['mtxmetadata', 'metadata'])
         usr = self._getColumnValue(row, idxs, ['userid'])
         murl = self._getColumnValue(row, idxs, ['mtxmetadataurl', 'metadataurl'])
         stat = self._getColumnValue(row, idxs, ['mtxstatus', 'status'])
         stattime = self._getColumnValue(row, idxs, ['mtxstatusmodtime', 'statusmodtime'])
         mtx = LMMatrix(None, matrixType=mtype, 
                        gcmCode=gcm, altpredCode=rcp, dateCode=dt,
                        metadata=meta, dlocation=dloc, 
                        metadataUrl=murl, userId=usr, gridset=grdset, 
                        matrixId=mtxid, status=stat, statusModTime=stattime)
      return mtx
   
   # ...............................................
   def _createMatrixColumn(self, row, idxs):
      """
      @summary: Create an MatrixColumn from a lm_matrixcolumn view
      """
      mtxcol = None
      if row is not None:
         # Ids of joined inputs, not used yet
         lyrid = self._getColumnValue(row,idxs,['layerid']) 
         shpgrdid = self._getColumnValue(row,idxs,['shplayerid']) 
         if lyrid:
            inputLayer = self.getBaseLayer(lyrid, None, None, None, None)
         if shpgrdid:
            shpgrid = self.getShapeGrid(shpgrdid, None, None, None)
         mtxcolid = self._getColumnValue(row,idxs,['matrixcolumnid']) 
         mtxid = self._getColumnValue(row,idxs,['matrixid']) 
         mtxIndex = self._getColumnValue(row,idxs,['matrixindex']) 
         squid = self._getColumnValue(row,idxs,['mtxcolsquid'])
         ident = self._getColumnValue(row,idxs,['mtxcolident'])
         mtxcolmeta = self._getColumnValue(row,idxs,['mtxcolmetatadata'])
         intparams = self._getColumnValue(row,idxs,['intersectparams'])
         mtxcolstat = self._getColumnValue(row,idxs,['mtxcolstatus']) 
         mtxcolstattime = self._getColumnValue(row,idxs,['mtxcolstatusmodtime']) 
         usr = self._getColumnValue(row,idxs,['userid'])

         mtxcol = MatrixColumn(mtxIndex, mtxid, usr, 
                        layer=inputLayer, shapegrid=shpgrid, 
                        intersectParams=intparams,
                        squid=squid, ident=ident,
                        processType=None, metadata=mtxcolmeta, 
                        matrixColumnId=mtxcolid, status=mtxcolstat, 
                        statusModTime=mtxcolstattime)
      return mtxcol

# ...............................................
   def _getLayerInputs(self, row, idxs):
      """
      @summary: Create Raster or Vector layer from a Layer or view in the Borg. 
      @note: OccurrenceSet and SDMProject objects do not use this function
      @note: used with Layer, lm_envlayer, lm_scenlayer, lm_sdmproject, lm_shapegrid
      """
      dbid = self._getColumnValue(row, idxs, ['layerid'])
      usr = self._getColumnValue(row, idxs, ['lyruserid', 'userid'])
      verify = self._getColumnValue(row, idxs, ['lyrverify', 'verify'])
      squid = self._getColumnValue(row, idxs, ['lyrsquid', 'squid'])
      name = self._getColumnValue(row, idxs, ['lyrname', 'name'])
      dloc = self._getColumnValue(row, idxs, ['lyrdlocation', 'dlocation'])
      murl = self._getColumnValue(row, idxs, ['lyrmetadataurl', 'metadataurl'])
      meta = self._getColumnValue(row, idxs, ['lyrmetadata', 'metadata'])
      vtype = self._getColumnValue(row, idxs, ['ogrtype'])
      rtype = self._getColumnValue(row, idxs, ['gdaltype'])
      vunits = self._getColumnValue(row, idxs, ['valunits'])
      vattr = self._getColumnValue(row, idxs, ['valattribute'])
      nodata = self._getColumnValue(row, idxs, ['nodataval'])
      minval = self._getColumnValue(row, idxs, ['minval'])
      maxval = self._getColumnValue(row, idxs, ['maxval'])
      fformat = self._getColumnValue(row, idxs, ['dataformat'])
      epsg = self._getColumnValue(row, idxs, ['epsgcode'])
      munits = self._getColumnValue(row, idxs, ['mapunits'])
      res = self._getColumnValue(row, idxs, ['resolution'])
      dtmod = self._getColumnValue(row, idxs, ['lyrmodtime', 'modtime'])
      bbox = self._getColumnValue(row, idxs, ['bbox'])
      return (dbid, usr, verify, squid, name, dloc, murl, meta, vtype, rtype, 
              vunits, vattr, nodata, minval, maxval, fformat, epsg, munits, res, 
              dtmod, bbox)

# ...............................................
   def _createLayer(self, row, idxs):
      """
      @summary: Create Raster or Vector layer from a Layer or view in the Borg. 
      @note: OccurrenceSet and SDMProject objects do not use this function
      @note: used with Layer, lm_envlayer, lm_scenlayer, lm_sdmproject, lm_shapegrid
      """
      lyr = None
      if row is not None:
         dbid = self._getColumnValue(row, idxs, ['layerid'])
         name = self._getColumnValue(row, idxs, ['lyrname', 'name'])
         usr = self._getColumnValue(row, idxs, ['lyruserid', 'userid'])
         epsg = self._getColumnValue(row, idxs, ['epsgcode'])
         # Layer may be optional
         if (dbid is not None and name is not None and usr is not None and epsg is not None):
            verify = self._getColumnValue(row, idxs, ['lyrverify', 'verify'])
            squid = self._getColumnValue(row, idxs, ['lyrsquid', 'squid'])
            dloc = self._getColumnValue(row, idxs, ['lyrdlocation', 'dlocation'])
            murl = self._getColumnValue(row, idxs, ['lyrmetadataurl', 'metadataurl'])
            meta = self._getColumnValue(row, idxs, ['lyrmetadata', 'metadata'])
            vtype = self._getColumnValue(row, idxs, ['ogrtype'])
            rtype = self._getColumnValue(row, idxs, ['gdaltype'])
            vunits = self._getColumnValue(row, idxs, ['valunits'])
            vattr = self._getColumnValue(row, idxs, ['valattribute'])
            nodata = self._getColumnValue(row, idxs, ['nodataval'])
            minval = self._getColumnValue(row, idxs, ['minval'])
            maxval = self._getColumnValue(row, idxs, ['maxval'])
            fformat = self._getColumnValue(row, idxs, ['dataformat'])
            munits = self._getColumnValue(row, idxs, ['mapunits'])
            res = self._getColumnValue(row, idxs, ['resolution'])
            dtmod = self._getColumnValue(row, idxs, ['lyrmodtime', 'modtime'])
            bbox = self._getColumnValue(row, idxs, ['lyrbbox', 'bbox'])
                  
            if fformat in OGRFormatCodes.keys():
               lyr = Vector(name, usr, epsg, lyrId=dbid, squid=squid, verify=verify, 
                            dlocation=dloc, metadata=meta, dataFormat=fformat, 
                            ogrType=vtype, valUnits=vunits, valAttribute=vattr,
                            nodataVal=nodata, minVal=minval, maxVal=maxval, 
                            mapunits=munits, resolution=res, bbox=bbox, 
                            metadataUrl=murl, modTime=dtmod)
            elif fformat in GDALFormatCodes.keys():
               lyr = Raster(name, usr, epsg, lyrId=dbid, squid=squid, verify=verify, 
                            dlocation=dloc, metadata=meta, dataFormat=fformat, 
                            gdalType=rtype, valUnits=vunits, nodataVal=nodata, 
                            minVal=minval, maxVal=maxval, mapunits=munits, 
                            resolution=res, bbox=bbox, metadataUrl=murl, 
                            modTime=dtmod)
      return lyr

# ...............................................
   def _createEnvLayer(self, row, idxs):
      """
      Create an EnvLayer from a lm_envlayer or lm_scenlayer record in the Borg
      """
      envRst = None
      envLayerId = self._getColumnValue(row,idxs,['envlayerid'])
      if row is not None:
         scenid = self._getColumnValue(row,idxs,['scenarioid'])
         scencode = self._getColumnValue(row,idxs,['scenariocode'])
         rst = self._createLayer(row, idxs)
         if rst is not None:
            etype = self._createEnvType(row, idxs)
            envRst = EnvLayer.initFromParts(rst, etype, envLayerId=envLayerId, 
                                            scencode=scencode)
      return envRst

# ...............................................
   def _createShapeGrid(self, row, idxs):
      """
      @note: takes lm_shapegrid record
      """
      shg = None
      if row is not None:
         lyr = self._createLayer(row, idxs)
         # Shapegrid may be optional
         if lyr is not None:
            shg = ShapeGrid.initFromParts(lyr, 
                     self._getColumnValue(row,idxs,['cellsides']), 
                     self._getColumnValue(row,idxs,['cellsize']),
                     siteId = self._getColumnValue(row,idxs,['idattribute']), 
                     siteX = self._getColumnValue(row,idxs,['xattribute']), 
                     siteY = self._getColumnValue(row,idxs,['yattribute']), 
                     size = self._getColumnValue(row,idxs,['vsize']),
                     # todo: will these ever be accessed without 'shpgrd' prefix?
                     status = self._getColumnValue(row,idxs,['shpgrdstatus', 'status']), 
                     statusModTime = self._getColumnValue(row,idxs,
                                       ['shpgrdstatusmodtime', 'statusmodtime']))
      return shg

# ...............................................
   def _createOccurrenceLayer(self, row, idxs):
      """
      @note: takes OccurrenceSet or lm_sdmproject record
      """
      occ = None
      if row is not None:
         name = self._getColumnValue(row,idxs,['displayname'])
         usr = self._getColumnValue(row,idxs,['occuserid','userid'])
         epsg = self._getColumnValue(row,idxs,['epsgcode'])
         qcount = self._getColumnValue(row,idxs,['querycount'])
         occ = OccurrenceLayer(name, usr, epsg, qcount,
               squid=self._getColumnValue(row,idxs,['squid']), 
               verify=self._getColumnValue(row,idxs,['occverify','verify']), 
               dlocation=self._getColumnValue(row,idxs,['occdlocation','dlocation']), 
               rawDLocation=self._getColumnValue(row,idxs,['rawdlocation']),
               bbox=self._getColumnValue(row,idxs,['occbbox','bbox']), 
               occurrenceSetId=self._getColumnValue(row,idxs,['occurrencesetid']), 
               metadataUrl=self._getColumnValue(row,idxs,['occmetadataurl',
                                                          'metadataurl']), 
               occMetadata=self._getColumnValue(row,idxs,['occmetadata','metadata']), 
               status=self._getColumnValue(row,idxs,['occstatus','status']), 
               statusModTime=self._getColumnValue(row,idxs,['occstatusmodtime',
                                                            'statusmodtime']))
      return occ

# ...............................................
   def _createSDMProjection(self, row, idxs):
      """
      @note: takes lm_sdmproject record
      """
      prj = None
      if row is not None:
         occ = self._createOccurrenceLayer(row, idxs)
         alg = self._createAlgorithm(row, idxs)
         mdlscen = self._createScenario(row, idxs, isForModel=True)
         prjscen = self._createScenario(row, idxs, isForModel=False)
         layer = self._createLayer(row, idxs)
         prj = SDMProjection.initFromParts(occ, alg, mdlscen, prjscen, layer,
#                   modelMaskId=self._getColumnValue(row, idxs, ['mdlmaskid']), 
#                   projMaskId=self._getColumnValue(row, idxs, ['prjmaskid']),
                  projMetadata=self._getColumnValue(row, idxs, ['prjmetadata']), 
                  status=self._getColumnValue(row,idxs,['prjstatus']), 
                  statusModTime=self._getColumnValue(row,idxs,['prjstatusmodtime']), 
                  sdmProjectionId=self._getColumnValue(row,idxs,['sdmprojectid']))                  
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
      try:
         min = lyr.minVal
         max = lyr.maxVal
         nodata = lyr.nodataVal
      except:
         min = max = nodata = wkt = None
      meta = lyr.dumpLyrMetadata()
      if lyr.epsgcode == SCENARIO_PACKAGE_EPSG:
         wkt = lyr.getWkt()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertLayer', 
                           lyr.getId(),
                           lyr.getLayerUserId(),
                           lyr.squid,
                           lyr.verify,
                           lyr.name,
                           lyr.getDLocation(),
                           lyr.metadataUrl, meta,
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
      """
      @summary: Get and fill a Layer from its layer id, SHASUM hash or 
                user/name/epsgcode.  
      @param lyrid: Layer database id
      @param lyrverify: SHASUM hash of layer data
      @param lyruser: Layer user id
      @param lyrname: Layer name
      @param lyrid: Layer EPSG code
      @return: LmServer.base.layer._Layer object
      """
      row, idxs = self.executeSelectOneFunction('lm_getLayer', lyrid, lyrverify, 
                                                lyruser, lyrname, epsgcode)
      lyr = self._createLayer(row, idxs)
      return lyr

# .............................................................................
   def countLayers(self, userId, squid, afterTime, beforeTime, epsg):
      """
      @summary: Count all Layers matching the filter conditions 
      @param userId: User (owner) for which to return occurrencesets.  
      @param squid: a species identifier, tied to a ScientificName
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @return: a list of OccurrenceSet atoms or full objects
      """
      row, idxs = self.executeSelectOneFunction('lm_countLayers', 
                                    userId, squid, afterTime, beforeTime, epsg)
      return self._getCount(row)

# .............................................................................
   def listLayers(self, firstRecNum, maxNum, userId, squid, 
                  afterTime, beforeTime, epsg, atom):
      """
      @summary: Return Layer Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return occurrencesets.  
      @param squid: a species identifier, tied to a ScientificName
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of Layer atoms or full objects
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listLayerAtoms', 
                              firstRecNum, maxNum, userId, squid, 
                              afterTime, beforeTime, epsg)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listLayerObjects', 
                              firstRecNum, maxNum, userId, squid, 
                              afterTime, beforeTime, epsg)
         for r in rows:
            objs.append(self._createLayer(r, idxs))
      return objs


# ...............................................
   def findOrInsertScenario(self, scen):
      """
      @summary Inserts all scenario layers into the database
      @param scen: The scenario to insert
      @return: new or existing Scenario
      """
      scen.modTime = mx.DateTime.utc().mjd
      wkt = None
      if scen.epsgcode == SCENARIO_PACKAGE_EPSG:
         wkt = scen.getWkt()
      meta = scen.dumpScenMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertScenario', 
                           scen.getUserId(), scen.code, 
                           scen.metadataUrl, meta, 
                           scen.gcmCode, scen.altpredCode, scen.dateCode, 
                           scen.units, scen.resolution, scen.epsgcode, 
                           scen.getCSVExtentString(), wkt, scen.modTime)
      newOrExistingScen = self._createScenario(row, idxs)
      return newOrExistingScen
   
# .............................................................................
   def countScenarios(self, userId, afterTime, beforeTime, epsg, 
                      gcmCode, altpredCode, dateCode):
      """
      @summary: Return the number of scenarios fitting the given filter conditions
      @param userId: filter by LMUser 
      @param beforeTime: filter by modified at or before this time
      @param afterTime: filter by modified at or after this time
      @param epsg: filter by the EPSG spatial reference system code 
      @param gcmCode: filter by the Global Climate Model code
      @param altpredCode: filter by the alternate predictor code (i.e. IPCC RCP)
      @param dateCode: filter by the date code
      @return: number of scenarios fitting the given filter conditions
      """
      row, idxs = self.executeSelectOneFunction('lm_countScenarios', userId, 
                                                afterTime, beforeTime, epsg,
                                                gcmCode, altpredCode, dateCode)
      return self._getCount(row)

# .............................................................................
   def listScenarios(self, firstRecNum, maxNum, userId, afterTime, beforeTime, 
                     epsg, gcmCode, altpredCode, dateCode, atom):
      """
      @summary: Return scenario Objects or Atoms fitting the given filters 
      @param firstRecNum: start at this record
      @param maxNum: maximum number of records to return
      @param userId: filter by LMUser 
      @param beforeTime: filter by modified at or before this time
      @param afterTime: filter by modified at or after this time
      @param epsg: filter by the EPSG spatial reference system code 
      @param gcmCode: filter by the Global Climate Model code
      @param altpredCode: filter by the alternate predictor code (i.e. IPCC RCP)
      @param dateCode: filter by the date code
      @param atom: True if return objects will be Atoms, False if full objects
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listScenarioAtoms', 
                                                     firstRecNum, maxNum, userId, 
                                                     afterTime, beforeTime, epsg,
                                                     gcmCode, altpredCode, dateCode)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listScenarioObjects', 
                                                     firstRecNum, maxNum, userId, 
                                                     afterTime, beforeTime, epsg,
                                                     gcmCode, altpredCode, dateCode)
         for r in rows:
            objs.append(self._createScenario(r, idxs))
      return objs

# ...............................................
   def getEnvironmentalType(self, typeId, typecode, usrid):
      try:
         if typeId is not None:
            row, idxs = self.executeSelectOneFunction('lm_getLayerType', typeId)
         else:
            row, idxs = self.executeSelectOneFunction('lm_getLayerType', 
                                                      usrid, typecode)
      except:
         envType = None
      else:
         envType = self._createLayerType(row, idxs)
      return envType

# ...............................................
   def findOrInsertEnvType(self, envtype):
      """
      @summary: Insert or find EnvType values.
      @param envtype: An EnvType or EnvLayer object
      @return: new or existing EnvironmentalType
      """
      currtime = mx.DateTime.utc().mjd
      meta = envtype.dumpParamMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertEnvType',
                                                    envtype.getParamId(),
                                                    envtype.getParamUserId(),
                                                    envtype.typeCode,
                                                    envtype.gcmCode,
                                                    envtype.altpredCode,
                                                    envtype.dateCode,
                                                    meta,
                                                    currtime)
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
      if shpgrd.epsgcode == SCENARIO_PACKAGE_EPSG:
         wkt = shpgrd.getWkt()
      meta = shpgrd.dumpParamMetadata()
      gdaltype = valunits = nodataval = minval = maxval = None
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertShapeGrid',
                           shpgrd.getId(), shpgrd.getUserId(), 
                           shpgrd.squid, shpgrd.verify, shpgrd.name,
                           shpgrd.getDLocation(), shpgrd.metadataUrl, meta,
                           shpgrd.dataFormat, gdaltype, shpgrd.ogrType, 
                           valunits, nodataval, minval, maxval, 
                           shpgrd.epsgcode, shpgrd.mapUnits, shpgrd.resolution, 
                           shpgrd.getCSVExtentString(), wkt, shpgrd.modTime, 
                           shpgrd.cellsides, shpgrd.cellsize, shpgrd.size, 
                           shpgrd.siteId, shpgrd.siteX, shpgrd.siteY, 
                           shpgrd.status, shpgrd.statusModTime)
      updatedShpgrd = self._createShapeGrid(row, idxs)
      return updatedShpgrd
   
# ...............................................
   def findOrInsertGridset(self, grdset):
      """
      @summary: Find or insert a Gridset into the database
      @param grdset: Gridset to insert
      @return: Updated new or existing Gridset.
      """
      meta = grdset.dumpGrdMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertGridset',
                                                         grdset.getId(),
                                                         grdset.getUserId(),
                                                         grdset.name,
                                                         grdset.metadataUrl,
                                                         grdset.shapeGridId,
                                                         grdset.configFilename,
                                                         grdset.epsgcode,
                                                         meta,
                                                         grdset.modTime)
      updatedGrdset = self._createGridset(row, idxs)
      return updatedGrdset
      
# ...............................................
   def getGridset(self, gridset, fillMatrices):
      """
      @summary: Retrieve a Gridset from the database
      @param gridset: Gridset to retrieve
      @param fillMatrices: True/False indicating whether to find and attach any 
             matrices associated with this Gridset
      @return: Existing LmServer.legion.gridset.Gridset
      @note: ProcessType is not held in DB record and must be set after
             retrieval
      """
      row, idxs = self.executeSelectOneFunction('lm_getGridset', 
                                                gridset.getId(),
                                                gridset.getUserId(),
                                                gridset.name)
      fullGset = self._createGridset(row, idxs)
      if fullGset is not None and fillMatrices:
         rows, idxs = self.executeSelectManyFunction('lm_getMatricesForGridset',
                                                     fullGset.getId())
         for r in rows:
            mtx = self._createLMMatrix(r, idxs)
            # addMatrix sets userid
            fullGset.addMatrix(mtx)
      return fullGset

# ...............................................
   def getMatrix(self, mtx, mtxId):
      """
      @summary: Retrieve a Matrix with its gridset from the database
      @param mtx: Matrix to retrieve
      @return: Existing Matrix
      """
      row = None
      if mtx is not None:
         row, idxs = self.executeSelectOneFunction('lm_getMatrix', mtx.getId(),
                                                   mtx.matrixType,
                                                   mtx.parentId, 
                                                   mtx.gridsetName,
                                                   mtx.getUserId())
      elif mtxId is not None:
         row, idxs = self.executeSelectOneFunction('lm_getMatrix', mtxId,
                                                   None, None, None, None)
      fullMtx = self._createLMMatrix(row, idxs)
      return fullMtx
      
# ...............................................
   def updateShapeGrid(self, shpgrd):
      """
      @summary: Update Shapegrid attributes: 
         verify, dlocation, metadata, modtime, size, status, statusModTime
      @param shpgrd: ShapeGrid to be updated.  
      @return: Updated record for successful update.
      """
      meta = shpgrd.dumpLyrMetadata()
      success = self.executeModifyFunction('lm_updateShapeGrid',
                        shpgrd.getId(), shpgrd.verify, shpgrd.getDLocation(),
                        meta, shpgrd.modTime, shpgrd.size, 
                        shpgrd.status, shpgrd.statusModTime)
      return success

# ...............................................
   def getShapeGrid(self, lyrId, userId, lyrName, epsg):
      """
      @summary: Find or insert a ShapeGrid into the database
      @param shpgrdId: ShapeGrid database id
      @return: new or existing ShapeGrid.
      """
      row, idxs = self.executeInsertAndSelectOneFunction('lm_getShapeGrid',
                                                   lyrId, userId, lyrName, epsg)
      shpgrid = self._createShapeGrid(row, idxs)
      return shpgrid
   
# # ...............................................
#    def findOrInsertLayer(self, layer):
#       """
#       @summary Insert or find a layer's metadata in the Borg. 
#       @param lyr: layer to insert
#       @return: new or existing EnvironmentalLayer
#       """
#       layer.modTime = mx.DateTime.utc().mjd
#       wkt = None
#       if layer.epsgcode == SCENARIO_PACKAGE_EPSG:
#          wkt = layer.getWkt()
#       lyrmeta = layer.dumpLyrMetadata()
#       row, idxs = self.executeInsertAndSelectOneFunction(
#                            'lm_findOrInsertLayer', layer.getId(), 
#                            layer.getUserId(), layer.squid, layer.verify, layer.name,
#                            layer.getDLocation(), layer.metadataUrl,
#                            lyrmeta, layer.dataFormat,  layer.gdalType, layer.ogrType, 
#                            layer.valUnits, layer.nodataVal, layer.minVal, layer.maxVal, 
#                            layer.epsgcode, layer.mapUnits, layer.resolution, 
#                            layer.getCSVExtentString(), wkt, layer.modTime)
#       newOrExistingLyr = self._createLayer(row, idxs)
#       return newOrExistingLyr

# ...............................................
   def findOrInsertEnvLayer(self, lyr, scenarioId):
      """
      @summary Insert or find a layer's metadata in the Borg. 
      @param lyr: layer to insert
      @return: new or existing EnvironmentalLayer
      """
      lyr.modTime = mx.DateTime.utc().mjd
      wkt = None
      if lyr.epsgcode == SCENARIO_PACKAGE_EPSG:
         wkt = lyr.getWkt()
      envmeta = lyr.dumpParamMetadata()
      lyrmeta = lyr.dumpLyrMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction(
                           'lm_findOrInsertEnvLayer', lyr.getId(), 
                           lyr.getUserId(), lyr.squid, lyr.verify, lyr.name,
                           lyr.getDLocation(), 
                           lyr.metadataUrl,
                           lyrmeta, lyr.dataFormat,  lyr.gdalType, lyr.ogrType, 
                           lyr.valUnits, lyr.nodataVal, lyr.minVal, lyr.maxVal, 
                           lyr.epsgcode, lyr.mapUnits, lyr.resolution, 
                           lyr.getCSVExtentString(), wkt, lyr.modTime, 
                           lyr.getParamId(), lyr.envCode, lyr.gcmCode,
                           lyr.altpredCode, lyr.dateCode, envmeta, 
                           lyr.paramModTime)
      if scenarioId is not None:
         etid = self._getColumnValue(row, idxs, ['envtypeid'])
         lyrid = self._getColumnValue(row, idxs, ['layerid'])
         row, idxs = self.executeInsertAndSelectOneFunction(
                     'lm_v3.lm_joinScenarioLayer', scenarioId, lyrid, etid)
      # Use row from first or second query      
      newOrExistingLyr = self._createEnvLayer(row, idxs)
      return newOrExistingLyr

# ...............................................
   def getEnvLayer(self, envlyrId, lyrid, lyrverify, lyruser, lyrname, epsgcode):
      """
      @summary: Get and fill a Layer from its layer id, SHASUM hash or 
                user/name/epsgcode.  
      @param envlyrId: EnvLayer join id
      @param lyrid: Layer database id
      @param lyrverify: SHASUM hash of layer data
      @param lyruser: Layer user id
      @param lyrname: Layer name
      @param lyrid: Layer EPSG code
      @return: LmServer.base.layer._Layer object
      """
      row, idxs = self.executeSelectOneFunction('lm_getLayer', lyrid, lyrverify, 
                                                lyruser, lyrname, epsgcode)
      lyr = self._createLayer(row, idxs)
      return lyr


# .............................................................................
   def countEnvLayers(self, userId, envCode, gcmcode, altpredCode, dateCode, 
                      afterTime, beforeTime, epsg, envTypeId):
      """
      @summary: Count all EnvLayers matching the filter conditions 
      @param userId: User (owner) for which to return occurrencesets.  
      @param envCode: filter by the environmental code (i.e. bio13)
      @param gcmCode: filter by the Global Climate Model code
      @param altpredCode: filter by the alternate predictor code (i.e. IPCC RCP)
      @param dateCode: filter by the date code
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param envTypeId: filter by the DB id of EnvironmentalType
      @return: a count of EnvLayers
      """
      row, idxs = self.executeSelectOneFunction('lm_countEnvLayers', 
                           userId, envCode, gcmcode, altpredCode, dateCode, 
                           afterTime, beforeTime, epsg, envTypeId)
      return self._getCount(row)

# .............................................................................
   def listEnvLayers(self, firstRecNum, maxNum, userId, envCode, gcmcode, 
                     altpredCode, dateCode, afterTime, beforeTime, epsg, 
                     envTypeId, atom):
      """
      @summary: List all EnvLayer objects or atoms matching the filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return occurrencesets.  
      @param envCode: filter by the environmental code (i.e. bio13)
      @param gcmCode: filter by the Global Climate Model code
      @param altpredCode: filter by the alternate predictor code (i.e. IPCC RCP)
      @param dateCode: filter by the date code
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param envTypeId: filter by the DB id of EnvironmentalType
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of EnvLayer objects or atoms
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listSDMProjectAtoms', 
                           firstRecNum, maxNum, userId, envCode, gcmcode, 
                           altpredCode, dateCode, afterTime, beforeTime, epsg, 
                           envTypeId)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listSDMProjectObjects', 
                           firstRecNum, maxNum, userId, envCode, gcmcode, 
                           altpredCode, dateCode, afterTime, beforeTime, epsg, 
                           envTypeId)
         for r in rows:
            objs.append(self._createEnvLayer(r, idxs))
      return objs


# .............................................................................
   def listSDMProjects(self, firstRecNum, maxNum, userId, displayName, 
                       afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                       occsetId, algCode, mdlscenCode, prjscenCode, atom):
      """
      @summary: Return SDMProjects Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return occurrencesets.  
      @param minOccurrenceCount: filter by minimum number of points in set.
      @param displayName: filter by display name
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @param occsetId: filter by occurrenceSet identifier
      @param algCode: filter by algorithm code
      @param mdlscenCode: filter by model scenario code
      @param prjscenCode: filter by projection scenario code
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of SDMProjects atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listSDMProjectAtoms', 
                           firstRecNum, maxNum, userId, displayName, afterTime, 
                           beforeTime, epsg, afterStatus, beforeStatus, occsetId, 
                           algCode, mdlscenCode, prjscenCode)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listSDMProjectObjects', 
                           firstRecNum, maxNum, userId, displayName, afterTime, 
                           beforeTime, epsg, afterStatus, beforeStatus, occsetId, 
                           algCode, mdlscenCode, prjscenCode)
         for r in rows:
            objs.append(self._createOccurrenceSet(r, idxs))
      return objs


# ...............................................
   def deleteScenarioLayer(self, envlyr, scenarioId):
      """
      @summary: Un-joins EnvLayer from scenario (if not None)
      @param envlyr: EnvLayer to remove from Scenario
      @param scenarioId: Id for scenario from which to remove EnvLayer 
      @return: True/False for success of operation
      """
      success = self.executeModifyFunction('lm_deleteScenarioLayer', 
                                           envlyr.getId(), scenarioId)         
      return success

# ...............................................
   def deleteEnvLayer(self, envlyr):
      """
      @summary: Un-joins EnvLayer from scenario (if not None) and deletes Layer 
                if it is not in any Scenarios or MatrixColumns
      @param envlyr: EnvLayer to delete (if orphaned)
      @return: True/False for success of operation
      @note: The layer will not be removed if it is used in any scenarios
      @note: If the EnvType is orphaned, it will also be removed
      """
      success = self.executeModifyFunction('lm_deleteEnvLayer', 
                                           envlyr.getId())         
      return success

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

# ...............................................
   def getTaxon(self, taxonSourceId, taxonKey, userId, taxonName):
      row, idxs = self.executeSelectOneFunction('lm_getTaxon', 
                                    taxonSourceId, taxonKey, userId, taxonName)
      scientificname = self._createScientificName(row, idxs)
      
      return scientificname

# .............................................................................
   def getScenario(self, scenid=None, code=None, usrid=None, fillLayers=False):
      """
      @summary: Get and fill a scenario from its user and code or database id.   
                If  fillLayers is true, populate the layers in the objecgt.
      @param scenid: ScenarioId for the scenario to be fetched.
      @param code: Code for the scenario to be fetched.
      @param usrid: User id for the scenario to be fetched.
      @param fillLayers: Boolean indicating whether to retrieve and populate 
             layers from to be fetched.
      @return: a LmServer.legion.scenario.Scenario object
      """
      row, idxs = self.executeSelectOneFunction('lm_getScenario', scenid, usrid, code)
      scen = self._createScenario(row, idxs)
      if scen is not None and fillLayers:
         lyrs = self.getScenarioLayers(scen.getId())
         scen.setLayers(lyrs)
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
         lyr = self._createEnvLayer(r, idxs)
         lyrs.append(lyr)
      return lyrs
   
# .............................................................................
   def getOccurrenceSet(self, occId, squid, userId, epsg):
      """
      @summary: get an occurrenceset for the given id or squid and User
      @param occId: the database primary key of the Occurrence record
      @param squid: a species identifier, tied to a ScientificName
      @param userId: the database primary key of the LMUser
      @param epsg: Spatial reference system code from EPSG
      """
      row, idxs = self.executeSelectOneFunction('lm_getOccurrenceSet',
                                                  occId, userId, squid, epsg)
      occ = self._createOccurrenceLayer(row, idxs)
      return occ
   
# ...............................................
   def updateOccurrenceSet(self, occ, polyWkt, pointsWkt):
      """
      @summary: Update OccurrenceLayer attributes: 
                verify, displayName, dlocation, rawDlocation, queryCount, 
                bbox, metadata, status, statusModTime, geometries if valid
      @note: Does not update the userid, squid, and epsgcode (unique constraint) 
      @param occ: OccurrenceLayer to be updated.  
      @param polyWkt: geometry for the minimum polygon around these points
      @param pointsWkt: multipoint geometry for these points
      @return: True/False for successful update.
      """
      success = False
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

# ...............................................
   def getSDMProject(self, layerid):
      """
      @summary: get a projection for the given id
      @param layerid: Database id for the SDMProject layer record
      """
      modelMask = projMask = None
      row, idxs = self.executeSelectOneFunction('lm_getSDMProjectLayer', layerid)
      proj = self._createSDMProjection(row, idxs)
      modelMaskId=self._getColumnValue(row, idxs, ['mdlmaskid'])
      projMaskId=self._getColumnValue(row, idxs, ['prjmaskid'])
      if modelMaskId is not None:
         modelMask = self.getBaseLayer(modelMaskId, None, None, None, None)
         proj.setModelMask(modelMask)
      if projMaskId is not None:
         projMask = self.getBaseLayer(projMaskId, None, None, None, None)
         proj.setProjMask(projMask)
      return proj

# ...............................................
   def updateSDMProject(self, proj):
      """
      @summary Method to update an SDMProjection object in the database with 
               the verify hash, metadata, data extent and values, status/statusmodtime.
      @param proj the SDMProjection object to update
      """
      success = False
      lyrmeta = proj.dumpLyrMetadata()
      prjmeta = proj.dumpParamMetadata()
      try:
         success = self.executeModifyFunction('lm_updateSDMProjectLayer', 
                                              proj.getParamId(), 
                                              proj.getId(), 
                                              proj.verify,
                                              proj.getDLocation(), 
                                              lyrmeta,
                                              proj.valUnits,
                                              proj.nodataVal,
                                              proj.minVal,
                                              proj.maxVal,
                                              proj.epsgcode,
                                              proj.getCSVExtentString(),
                                              proj.getWkt(),
                                              proj.modTime,
                                              prjmeta,
                                              proj.status, 
                                              proj.statusModTime)
      except Exception, e:
         raise e
      return success

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

# .............................................................................
   def countOccurrenceSets(self, userId, minOccurrenceCount, displayName, 
                        afterTime, beforeTime, epsg, afterStatus, beforeStatus):
      """
      @summary: Count all OccurrenceSets matching the filter conditions 
      @param userId: User (owner) for which to return occurrencesets.  
      @param minOccurrenceCount: filter by minimum number of points in set.
      @param displayName: filter by display name *starting with* this string
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @return: a list of OccurrenceSet atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      row, idxs = self.executeSelectOneFunction('lm_countOccurrenceSets', 
                                                minOccurrenceCount,
                                                userId, displayName,
                                                afterTime, beforeTime, epsg,
                                                afterStatus, beforeStatus)
      return self._getCount(row)

# .............................................................................
   def listOccurrenceSets(self, firstRecNum, maxNum, userId, 
                          minOccurrenceCount, displayName, afterTime, beforeTime, 
                          epsg, afterStatus, beforeStatus, atom):
      """
      @summary: Return OccurrenceSet Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return occurrencesets.  
      @param minOccurrenceCount: filter by minimum number of points in set.
      @param displayName: filter by display name
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of OccurrenceSet atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listOccSetAtoms', 
                              firstRecNum, maxNum, userId, minOccurrenceCount,
                              displayName, afterTime, beforeTime, epsg, 
                              afterStatus, beforeStatus)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listOccSetObjects', 
                              firstRecNum, maxNum, userId, minOccurrenceCount,
                              displayName, afterTime, beforeTime, epsg, 
                              afterStatus, beforeStatus)
         for r in rows:
            objs.append(self._createOccurrenceSet(r, idxs))
      return objs

# ...............................................
   def findOrInsertSDMProject(self, proj):
      """
      @summary: Find existing (from projectID, layerid, OR usr/layername/epsg) 
                OR save a new SDMProjection
      @param proj: the SDMProjection object to update
      @return new or existing SDMProjection 
      @note: assumes that modelMask and projMask have already been inserted
      """
      lyrmeta = proj.dumpLyrMetadata()
      prjmeta = proj.dumpParamMetadata()
      algparams = proj.dumpAlgorithmParametersAsString()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertSDMProjectLayer', 
                     proj.getParamId(), proj.getId(), proj.getUserId(), 
                     proj.squid, proj.verify, proj.name, proj.getDLocation(), 
                     proj.metadataUrl, lyrmeta, proj.dataFormat, proj.gdalType,
                     proj.ogrType, proj.valUnits, proj.nodataVal, proj.minVal,
                     proj.maxVal, proj.epsgcode, proj.mapUnits, proj.resolution,
                     proj.getCSVExtentString(), proj.getWkt(), proj.modTime,
                     proj.getOccurrenceSetId(), proj.algorithmCode, algparams,
                     proj.getModelScenarioId(), proj.getModelMaskId(),
                     proj.getProjScenarioId(), proj.getProjMaskId(), prjmeta,
                     proj.processType, proj.status, proj.statusModTime)
      newOrExistingProj = self._createSDMProjection(row, idxs)
      modelMaskId=self._getColumnValue(row, idxs, ['mdlmaskid'])
      projMaskId=self._getColumnValue(row, idxs, ['prjmaskid'])
      if modelMaskId is not None:
         modelMask = self.getBaseLayer(modelMaskId, None, None, None, None)
         newOrExistingProj.setModelMask(modelMask)
      if projMaskId is not None:
         projMask = self.getBaseLayer(projMaskId, None, None, None, None)
         newOrExistingProj.setProjMask(projMask)
      return newOrExistingProj

# .............................................................................
   def countSDMProjects(self, userId, displayName, 
                        afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                        occsetId, algCode, mdlscenCode, prjscenCode):
      """
      @summary: Count all SDMProjects matching the filter conditions 
      @param userId: User (owner) for which to return occurrencesets.  
      @param displayName: filter by display name *starting with* this string
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @param occsetId: filter by occurrenceSet identifier
      @param algCode: filter by algorithm code
      @param mdlscenCode: filter by model scenario code
      @param prjscenCode: filter by projection scenario code
      @return: a count of SDMProjects 
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      row, idxs = self.executeSelectOneFunction('lm_countSDMProjects', 
                           userId, displayName, afterTime, beforeTime, epsg,
                           afterStatus, beforeStatus, occsetId, algCode, 
                           mdlscenCode, prjscenCode)
      return self._getCount(row)

# .............................................................................
   def listSDMProjects(self, firstRecNum, maxNum, userId, displayName, 
                       afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                       occsetId, algCode, mdlscenCode, prjscenCode, atom):
      """
      @summary: Return SDMProjects Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return occurrencesets.  
      @param minOccurrenceCount: filter by minimum number of points in set.
      @param displayName: filter by display name
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @param occsetId: filter by occurrenceSet identifier
      @param algCode: filter by algorithm code
      @param mdlscenCode: filter by model scenario code
      @param prjscenCode: filter by projection scenario code
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of SDMProjects atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listSDMProjectAtoms', 
                           firstRecNum, maxNum, userId, displayName, afterTime, 
                           beforeTime, epsg, afterStatus, beforeStatus, occsetId, 
                           algCode, mdlscenCode, prjscenCode)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listSDMProjectObjects', 
                           firstRecNum, maxNum, userId, displayName, afterTime, 
                           beforeTime, epsg, afterStatus, beforeStatus, occsetId, 
                           algCode, mdlscenCode, prjscenCode)
         for r in rows:
            objs.append(self._createOccurrenceSet(r, idxs))
      return objs

# ...............................................
   def findOrInsertMatrixColumn(self, mtxcol):
      """
      @summary: Find existing OR save a new MatrixColumn
      @param mtxcol: the LmServer.legion.MatrixColumn object to get or insert
      @return new or existing MatrixColumn object
      """
      lyrid = None
      if mtxcol.layer is not None:
         # Check for existing id before pulling from db
         lyrid = mtxcol.layer.getId()
         if lyrid is None:
            newOrExistingLyr = self.findOrInsertBaseLayer(mtxcol.layer)
            lyrid = newOrExistingLyr.getId()

            # Shapegrid is already in db
            shpid = mtxcol.shapegrid.getId()

      mcmeta = mtxcol.dumpParamMetadata()
      intparams = mtxcol.dumpIntersectParams()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertMatrixColumn', 
                     mtxcol.getParamUserId(), mtxcol.getParamId(), mtxcol.parentId, 
                     mtxcol.getMatrixIndex(), lyrid, mtxcol.squid, mtxcol.ident, 
                     mcmeta, intparams, 
                     mtxcol.status, mtxcol.statusModTime)
      newOrExistingMtxCol = self._createMatrixColumn(row, idxs)
      return newOrExistingMtxCol

# ...............................................
   def updateMatrixColumn(self, mtxcol):
      """
      @summary: Update a MatrixColumn
      @param mtxcol: the LmServer.legion.MatrixColumn object to update
      @return: Boolean success/failure
      """
      meta = mtxcol.dumpParamMetadata()
      intparams = mtxcol.dumpIntersectParams()
      success = self.executeModifyFunction('lm_updateMatrixColumn', 
                                           mtxcol.getId(), 
                                           mtxcol.getMatrixIndex(),
                                           meta, intparams,
                                           mtxcol.status, mtxcol.statusModTime)
      return success

# ...............................................
   def updateMatrix(self, mtx):
      """
      @summary: Update a LMMatrix
      @param mtxcol: the LmServer.legion.LMMatrix object to update
      @return: Boolean success/failure
      @TODO: allow update of MatrixType, gcmCode, altpredCode, dateCode?
      """
      meta = mtx.dumpMtxMetadata()
      success = self.executeModifyFunction('lm_updateMatrix', 
                                           mtx.getId(), meta, 
                                           mtx.status, mtx.statusModTime)
      return success

# ...............................................
   def findOrInsertMatrix(self, mtx):
      """
      @summary: Find existing OR save a new Matrix
      @param mtx: the Matrix object to insert
      @return new or existing Matrix
      """
      meta = mtx.dumpMtxMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertMatrix', 
                     mtx.getId(), mtx.matrixType, mtx.parentId, 
                     mtx.gcmCode, mtx.altpredCode, mtx.dateCode,
                     mtx.getDLocation(), mtx.metadataUrl, meta, mtx.status, 
                     mtx.statusModTime)
      newOrExistingMtx = self._createLMMatrix(row, idxs)
      return newOrExistingMtx

# ...............................................
   def insertMFChain(self, mfchain):
      """
      @summary: Inserts a MFChain into database
      @return: updated MFChain object
      """
      meta = mfchain.dumpMfMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_insertMFChain', 
                                             mfchain.getUserId(), 
                                             mfchain.getDLocation(), 
                                             mfchain.priority, 
                                             meta, mfchain.status, 
                                             mfchain.statusModTime)
      mfchain = self._createMFChain(row, idxs)
      return mfchain

# ...............................................
   def findMFChains(self, count, userId, oldStatus, newStatus):
      """
      @summary: Retrieves MFChains from database, optionally filtered by status 
                and/or user, updates their status
      @param count: Number of MFChains to pull
      @param userId: If not None, filter by this user 
      @param oldStatus: Pull only MFChains at this status
      @param newStatus: Update MFChains to this status
      @return: list of MFChains
      """
      mfchainList = []
      modtime = mx.DateTime.utc().mjd
      rows, idxs = self.executeSelectManyFunction('lm_findMFChains', count, 
                                                  userId, oldStatus, newStatus,
                                                  modtime)
      for r in rows:
         mfchain = self._createMFChain(r, idxs)
         mfchainList.append(mfchain)
      return mfchainList
      
# ...............................................
   def updateMFChain(self, mfchain):
      """
      @summary: Updates MFChain status and statusModTime in the database
      @return: True/False for success of operation
      """
      success = self.executeModifyFunction('lm_updateMFChain', mfchain.objId,
                                           mfchain.getDLocation(), 
                                           mfchain.status, mfchain.statusModTime)
      return success

# ...............................................
   def updateObject(self, obj):
      """
      @summary: Updates object in database
      @return: True/False for success of operation
      """
      if isinstance(obj, OccurrenceLayer):
         polyWkt = pointsWkt = None
         success = self.updateOccurrenceSet(obj, None, None)
      elif isinstance(obj, SDMProjection):
         success = self.updateSDMProject(obj)
      elif isinstance(obj, ShapeGrid):
         success = self.updateShapeGrid(obj)
      elif isinstance(obj, MFChain):
         success = self.executeModifyFunction('lm_updateMFChain', obj.objId,
                                              obj.getDLocation(), 
                                              obj.status, obj.statusModTime)
      else:
         raise LMError('Unsupported update for object {}'.format(type(obj)))
      return success

# ...............................................
   def deleteObject(self, obj):
      """
      @summary: Deletes object from database
      @return: True/False for success of operation
      """
      try:
         objid = obj.getId()
      except:
         try:
            obj = obj.objId
         except:
            raise LMError('Failed getting ID for {} object'.format(type(obj)))
      if isinstance(obj, MFChain):
         success = self.executeModifyFunction('lm_deleteMFChain', objid)
      elif isinstance(obj, OccurrenceLayer):
         success = self.executeModifyFunction('lm_deleteOccurrenceSet', objid)
      elif isinstance(obj, SDMProjection):
         success = self.executeModifyFunction('lm_deleteSDMProjectLayer', objid)
      elif isinstance(obj, ShapeGrid):
         success = self.executeModifyFunction('lm_deleteShapeGrid', objid)
      elif isinstance(obj, Scenario):
         # Deletes ScenarioLayer join; only deletes layers if they are orphaned
         for lyr in obj.layers:
            self.deleteEnvLayer(lyr, objid)
         success = self.executeModifyFunction('lm_deleteScenario', objid)
      else:
         raise LMError('Unsupported delete for object {}'.format(type(obj)))
      return success
      
      