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
   
from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import MatrixType, LMFormat
from LmServer.base.dbpgsql import DbPostgresql
from LmServer.base.layer2 import Raster, Vector
from LmServer.base.taxon import ScientificName
from LmServer.common.computeResource import LMComputeResource
from LmServer.common.lmconstants import DB_STORE, LM_SCHEMA_BORG, LMServiceType
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import DEFAULT_EPSG
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.envlayer import EnvType, EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.mtxcolumn import MatrixColumn
from LmServer.legion.occlayer import OccurrenceLayer
from LmServer.legion.processchain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.sdmproj import SDMProjection
from LmServer.legion.tree import Tree

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
         scencode = self._getColumnValue(row, idxs, ['mdlscenariocode', 
                                                     'scenariocode'])
         meta = self._getColumnValue(row, idxs, ['mdlscenmetadata', 
                                                 'scenmetadata', 'metadata'])
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
      meta = self._getColumnValue(row, idxs, ['metadata'])
      units = self._getColumnValue(row, idxs, ['units'])
      res = self._getColumnValue(row, idxs, ['resolution'])
      epsg = self._getColumnValue(row, idxs, ['epsgcode'])
      bbox = self._getColumnValue(row, idxs, ['bbox'])
      modtime = self._getColumnValue(row, idxs, ['scenmodtime', 'modtime'])
    
      if row is not None:
         scen = Scenario(scencode, usr, epsg, metadata=meta, units=units, res=res, 
                     gcmCode=gcmcode, altpredCode=altpredcode, dateCode=datecode,
                     bbox=bbox, modTime=modtime, layers=None, scenarioid=scenid)
      return scen

# ...............................................
   def _createScenPackage(self, row, idxs):
      """
      @note: created only from Scenario table or lm_sdmproject view
      """
      scen = None
      pkgid = self._getColumnValue(row, idxs, ['scenpackageid'])
      usr = self._getColumnValue(row, idxs, ['userid'])
      name = self._getColumnValue(row, idxs, ['pkgname', 'name'])
      meta = self._getColumnValue(row, idxs, ['pkgmetadata', 'metadata'])
      epsg = self._getColumnValue(row, idxs, ['pkgepsgcode', 'epsgcode'])
      bbox = self._getColumnValue(row, idxs, ['pkgbbox', 'bbox'])
      units = self._getColumnValue(row, idxs, ['pkgunits', 'units'])
      modtime = self._getColumnValue(row, idxs, ['pkgmodtime', 'modtime'])
    
      if row is not None:
         scen = ScenPackage(name, usr, metadata=meta, epsgcode=epsg, bbox=bbox, 
                            mapunits=units, modTime=modtime,
                            scenPackageId=pkgid)
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
         tree = self._createTree(row, idxs)
         shpId = self._getColumnValue(row, idxs, ['layerid'])
         grdid = self._getColumnValue(row, idxs, ['gridsetid'])
         treeid = self._getColumnValue(row, idxs, ['treeid'])
         usr = self._getColumnValue(row, idxs, ['userid'])
         name = self._getColumnValue(row, idxs, ['grdname', 'name'])
         dloc = self._getColumnValue(row, idxs, ['grddlocation', 'dlocation'])
         epsg = self._getColumnValue(row, idxs, ['grdepsgcode', 'epsgcode'])
         meta = self._getColumnValue(row, idxs, ['grdmetadata', 'metadata'])
         mtime = self._getColumnValue(row, idxs, ['grdmodtime', 'modtime'])
         grdset = Gridset(name=name, metadata=meta, shapeGrid=shp, 
                          shapeGridId=shpId, tree=tree, treeId=treeid, 
                          dlocation=dloc, epsgcode=epsg, userId=usr, 
                          gridsetId=grdid, modTime=mtime)
      return grdset
   
# ...............................................
   def _createTree(self, row, idxs):
      """
      @summary: Create a Tree from a database Tree record
      @todo: Do we want to use binary attributes without reading data?
      """
      tree = None
      if row is not None:
         treeid = self._getColumnValue(row, idxs, ['treeid'])
         usr = self._getColumnValue(row, idxs, ['treeuserid', 'userid'])
         name = self._getColumnValue(row, idxs, ['treename', 'name'])
         dloc = self._getColumnValue(row, idxs, ['treedlocation', 'dlocation'])
         isbin = self._getColumnValue(row, idxs, ['isbinary'])
         isultra = self._getColumnValue(row, idxs, ['isultrametric'])
         haslen = self._getColumnValue(row, idxs, ['hasbranchlengths'])
         meta = self._getColumnValue(row, idxs, ['treemetadata', 'metadata'])
         modtime = self._getColumnValue(row, idxs, ['treemodtime', 'modtime'])
         tree = Tree(name, metadata=meta, dlocation=dloc, userId=usr, 
                     treeId=treeid, modTime=modtime)
      return tree
   
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
         stat = self._getColumnValue(row, idxs, ['mtxstatus', 'status'])
         stattime = self._getColumnValue(row, idxs, ['mtxstatusmodtime', 'statusmodtime'])
         mtx = LMMatrix(None, matrixType=mtype, 
                        gcmCode=gcm, altpredCode=rcp, dateCode=dt,
                        metadata=meta, dlocation=dloc, userId=usr, gridset=grdset, 
                        matrixId=mtxid, status=stat, statusModTime=stattime)
      return mtx
   
   # ...............................................
   def _createMatrixColumn(self, row, idxs):
      """
      @summary: Create an MatrixColumn from a lm_matrixcolumn view
      """
      mtxcol = None
      if row is not None:
         # Returned by only some functions
         inputLyr = self._createLayer(row, idxs)
         # Ids of joined input layers
         lyrid = self._getColumnValue(row,idxs,['layerid']) 
         shpgrdid = self._getColumnValue(row,idxs,['shplayerid']) 
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
                        layer=inputLyr, layerId=lyrid, shapeGridId=shpgrdid, 
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
      return (dbid, usr, verify, squid, name, dloc, meta, vtype, rtype, 
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
                  
            if fformat in LMFormat.OGRDrivers():
               lyr = Vector(name, usr, epsg, lyrId=dbid, squid=squid, verify=verify, 
                            dlocation=dloc, metadata=meta, dataFormat=fformat, 
                            ogrType=vtype, valUnits=vunits, valAttribute=vattr,
                            nodataVal=nodata, minVal=minval, maxVal=maxval, 
                            mapunits=munits, resolution=res, bbox=bbox, 
                            modTime=dtmod)
            elif fformat in LMFormat.GDALDrivers():
               lyr = Raster(name, usr, epsg, lyrId=dbid, squid=squid, verify=verify, 
                            dlocation=dloc, metadata=meta, dataFormat=fformat, 
                            gdalType=rtype, valUnits=vunits, nodataVal=nodata, 
                            minVal=minval, maxVal=maxval, mapunits=munits, 
                            resolution=res, bbox=bbox, modTime=dtmod)
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
      @note: takes OccurrenceSet, lm_occMatrixcolumn, or lm_sdmproject record
      """
      occ = None
      if row is not None:
         name = self._getColumnValue(row,idxs,['displayname'])
         usr = self._getColumnValue(row,idxs,['occuserid','userid'])
         epsg = self._getColumnValue(row,idxs,['occepsgcode', 'epsgcode'])
         qcount = self._getColumnValue(row,idxs,['querycount'])
         occ = OccurrenceLayer(name, usr, epsg, qcount,
               squid=self._getColumnValue(row,idxs,['occsquid', 'squid']), 
               verify=self._getColumnValue(row,idxs,['occverify','verify']), 
               dlocation=self._getColumnValue(row,idxs,['occdlocation','dlocation']), 
               rawDLocation=self._getColumnValue(row,idxs,['rawdlocation']),
               bbox=self._getColumnValue(row,idxs,['occbbox','bbox']), 
               occurrenceSetId=self._getColumnValue(row,idxs,['occurrencesetid']), 
               occMetadata=self._getColumnValue(row,idxs,['occmetadata','metadata']), 
               status=self._getColumnValue(row,idxs,['occstatus','status']), 
               statusModTime=self._getColumnValue(row,idxs,['occstatusmodtime',
                                                            'statusmodtime']))
      return occ

# ...............................................
   def _createSDMProjection(self, row, idxs, layer=None):
      """
      @note: takes lm_sdmproject or lm_sdmMatrixcolumn record
      @note: allows previously constructed layer object to avoid reconstructing
      """
      prj = None
      if row is not None:
         occ = self._createOccurrenceLayer(row, idxs)
         alg = self._createAlgorithm(row, idxs)
         mdlscen = self._createScenario(row, idxs, isForModel=True)
         prjscen = self._createScenario(row, idxs, isForModel=False)
         if layer is None:
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
      if lyr.epsgcode == DEFAULT_EPSG:
         wkt = lyr.getWkt()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertLayer', 
                           lyr.getId(),
                           lyr.getLayerUserId(),
                           lyr.squid,
                           lyr.verify,
                           lyr.name,
                           lyr.getDLocation(),
                           meta,
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
      @return: LmServer.base.layer2._Layer object
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
      @return: a count of OccurrenceSets
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
         objs = self._getAtoms(rows, idxs, LMServiceType.LAYERS)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listLayerObjects', 
                              firstRecNum, maxNum, userId, squid, 
                              afterTime, beforeTime, epsg)
         for r in rows:
            objs.append(self._createLayer(r, idxs))
      return objs

# ...............................................
   def findOrInsertScenPackage(self, scenPkg):
      """
      @summary Inserts a ScenPackage into the database
      @param scenPkg: The LmServer.legion.scenario.ScenPackage to insert
      @return: new or existing ScenPackage
      @note: This returns the updated ScenPackage 
      @note:  This Borg function inserts only the ScenPackage; 
              the calling Scribe method also adds and joins Scenarios present 
      """
      wkt = None
      if scenPkg.epsgcode == DEFAULT_EPSG:
         wkt = scenPkg.getWkt()
      meta = scenPkg.dumpScenpkgMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertScenPackage', 
                           scenPkg.getUserId(), scenPkg.name, meta, 
                           scenPkg.mapUnits, scenPkg.epsgcode, 
                           scenPkg.getCSVExtentString(), wkt,
                           scenPkg.modTime)
      newOrExistingScenPkg = self._createScenPackage(row, idxs)
      return newOrExistingScenPkg
   
# .............................................................................
   def countScenPackages(self, userId, afterTime, beforeTime, epsg, scenId):
      """
      @summary: Return the number of ScenarioPackages fitting the given filter 
               conditions
      @param userId: filter by LMUser 
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by the EPSG spatial reference system code 
      @param scenId: filter by a Scenario 
      @return: number of ScenarioPackages fitting the given filter conditions
      """
      row, idxs = self.executeSelectOneFunction('lm_countScenPackages', userId, 
                                                afterTime, beforeTime, epsg, 
                                                scenId)
      return self._getCount(row)

# .............................................................................
   def listScenPackages(self, firstRecNum, maxNum, userId, afterTime, beforeTime, 
                        epsg, scenId, atom):
      """
      @summary: Return ScenPackage Objects or Atoms fitting the given filters 
      @param firstRecNum: start at this record
      @param maxNum: maximum number of records to return
      @param userId: filter by LMUser 
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by the EPSG spatial reference system code 
      @param scenId: filter by a Scenario 
      @param atom: True if return objects will be Atoms, False if full objects
      @note: returned ScenPackage Objects contain Scenario objects, not filled
             with layers.
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listScenPackageAtoms', 
                                                     firstRecNum, maxNum, userId, 
                                                     afterTime, beforeTime,  
                                                     epsg, scenId)
         objs = self._getAtoms(rows, idxs, LMServiceType.SCEN_PACKAGES)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listScenPackageObjects', 
                                                     firstRecNum, maxNum, userId, 
                                                     afterTime, beforeTime, 
                                                     epsg, scenId)
         for r in rows:
            objs.append(self._createScenario(r, idxs))
      return objs


# ...............................................
   def getScenPackage(self, scenPkg, scenPkgId, userId, scenPkgName, 
                      fillLayers):
      """
      @summary Find all ScenPackages that contain the given Scenario
      @param scenPkg: The The LmServer.legion.scenario.ScenPackage to find 
      @param scenPkgId: The database Id for the ScenPackage
      @param userId: The userId for the ScenPackages
      @param scenPkgName: The name for the ScenPackage
      @return: LmServer.legion.scenario.ScenPackage object, filled
               with Scenarios
      """
      if scenPkg:
         scenPkgId = scenPkg.getId()
         userId = scenPkg.getUserId()
         scenPkgName = scenPkg.name
      row, idxs = self.executeSelectOneFunction('lm_getScenPackage',
                                                  scenPkgId, userId, scenPkgName)
      foundScenPkg = self._createScenPackage(row, idxs)
      if foundScenPkg:
         scens = self.getScenariosForScenPackage(foundScenPkg, None, None, None, 
                                                 fillLayers)
         foundScenPkg.setScenarios(scens)
      return foundScenPkg
   

# ...............................................
   def getScenPackagesForScenario(self, scen, scenId, userId, scenCode, fillLayers):
      """
      @summary Find all ScenPackages that contain the given Scenario
      @param scen: The The LmServer.legion.scenario.Scenario to find 
                     scenarios for
      @param scenId: The database Id for the Scenario to find ScenPackages
      @param userId: The userId for the Scenario to find ScenPackages
      @param scenPkgName: The name for the Scenario to find ScenPackages
      @return: list of LmServer.legion.scenario.ScenPackage objects, filled
               with Scenarios
      """
      scenPkgs = []
      if scen:
         scenId = scen.getId()
         userId = scen.getUserId()
         scenCode = scen.code
      rows, idxs = self.executeSelectManyFunction('lm_getScenPackagesForScenario',
                                                  scenId, userId, scenCode)
      for r in rows:
         epkg = self._createScenPackage(r, idxs)
         scens = self.getScenariosForScenPackage(epkg, None, None, None, fillLayers)
         epkg.setScenarios(scens)
         scenPkgs.append(epkg)
      return scenPkgs
   
# ...............................................
   def getScenariosForScenPackage(self, scenPkg, scenPkgId, userId, scenPkgName,
                                  fillLayers):
      """
      @summary Find all scenarios that are part of the given ScenPackage
      @param scenPkg: The LmServer.legion.scenario.ScenPackage to find 
                     scenarios for
      @param scenPkgId: The database Id for the ScenPackage to find scenarios
      @param userId: The userId for the ScenPackage to find scenarios
      @param scenPkgName: The name for the ScenPackage to find scenarios
      @return: list of LmServer.legion.scenario.Scenario objects
      """
      scens = []
      if scenPkg:
         scenPkgId = scenPkg.getId()
         userId = scenPkg.getUserId()
         scenPkgName = scenPkg.name
      rows, idxs = self.executeSelectManyFunction('lm_getScenariosForScenPackage',
                                                  scenPkgId, userId, scenPkgName)
      for r in rows:
         scen = self._createScenario(r, idxs, isForModel=False)
         if scen is not None and fillLayers:
            lyrs = self.getScenarioLayers(scen.getId())
            scen.setLayers(lyrs)
         scens.append(scen)

      return scens
   
# ...............................................
   def findOrInsertScenario(self, scen, scenPkgId):
      """
      @summary Inserts a scenario and any layers present into the database
      @param scen: The scenario to insert
      @return: new or existing Scenario
      """
      scen.modTime = mx.DateTime.utc().mjd
      wkt = None
      if scen.epsgcode == DEFAULT_EPSG:
         wkt = scen.getWkt()
      meta = scen.dumpScenMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertScenario', 
                           scen.getUserId(), scen.code, meta, 
                           scen.gcmCode, scen.altpredCode, scen.dateCode, 
                           scen.mapUnits, scen.resolution, scen.epsgcode, 
                           scen.getCSVExtentString(), wkt, scen.modTime)
      newOrExistingScen = self._createScenario(row, idxs)
      if scenPkgId is not None:
         scenarioId = self._getColumnValue(row, idxs, ['scenarioid'])
         joinId = self.executeModifyReturnValue(
                     'lm_joinScenPackageScenario', scenPkgId, scenarioId)
         if joinId < 0:
            raise LMError('Failed to join ScenPackage {} to Scenario {}'
                          .format(scenPkgId, scenarioId))
      return newOrExistingScen
   
# .............................................................................
   def countScenarios(self, userId, afterTime, beforeTime, epsg, 
                      gcmCode, altpredCode, dateCode, scenPackageId):
      """
      @summary: Return the number of scenarios fitting the given filter conditions
      @param userId: filter by LMUser 
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by the EPSG spatial reference system code 
      @param gcmCode: filter by the Global Climate Model code
      @param altpredCode: filter by the alternate predictor code (i.e. IPCC RCP)
      @param dateCode: filter by the date code
      @param scenPackageId: filter by a ScenPackage 
      @return: number of scenarios fitting the given filter conditions
      """
      row, idxs = self.executeSelectOneFunction('lm_countScenarios', userId, 
                                                afterTime, beforeTime, epsg,
                                                gcmCode, altpredCode, dateCode,
                                                scenPackageId)
      return self._getCount(row)

# .............................................................................
   def listScenarios(self, firstRecNum, maxNum, userId, afterTime, beforeTime, 
                     epsg, gcmCode, altpredCode, dateCode, scenPackageId, atom):
      """
      @summary: Return scenario Objects or Atoms fitting the given filters 
      @param firstRecNum: start at this record
      @param maxNum: maximum number of records to return
      @param userId: filter by LMUser 
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by the EPSG spatial reference system code 
      @param gcmCode: filter by the Global Climate Model code
      @param altpredCode: filter by the alternate predictor code (i.e. IPCC RCP)
      @param dateCode: filter by the date code
      @param scenPackageId: filter by a ScenPackage 
      @param atom: True if return objects will be Atoms, False if full objects
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listScenarioAtoms', 
                                                     firstRecNum, maxNum, userId, 
                                                     afterTime, beforeTime, epsg,
                                                     gcmCode, altpredCode, 
                                                     dateCode, scenPackageId)
         objs = self._getAtoms(rows, idxs, LMServiceType.SCENARIOS)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listScenarioObjects', 
                                                     firstRecNum, maxNum, userId, 
                                                     afterTime, beforeTime, epsg,
                                                     gcmCode, altpredCode, 
                                                     dateCode, scenPackageId)
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
      if shpgrd.epsgcode == DEFAULT_EPSG:
         wkt = shpgrd.getWkt()
      meta = shpgrd.dumpParamMetadata()
      gdaltype = valunits = nodataval = minval = maxval = None
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertShapeGrid',
                           shpgrd.getId(), shpgrd.getUserId(), 
                           shpgrd.squid, shpgrd.verify, shpgrd.name,
                           shpgrd.getDLocation(), meta,
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
                                                         grdset.shapeGridId,
                                                         grdset.getDLocation(),
                                                         grdset.epsgcode,
                                                         meta,
                                                         grdset.modTime)
      updatedGrdset = self._createGridset(row, idxs)
      return updatedGrdset
      
# ...............................................
   def getGridset(self, gridsetId, userId, name, fillMatrices):
      """
      @summary: Retrieve a Gridset from the database
      @param gridsetId: Database id of the Gridset to retrieve
      @param userId: UserId of the Gridset to retrieve
      @param name: Name of the Gridset to retrieve
      @param fillMatrices: True/False indicating whether to find and attach any 
             matrices associated with this Gridset
      @return: Existing LmServer.legion.gridset.Gridset
      """
      row, idxs = self.executeSelectOneFunction('lm_getGridset', 
                                                gridsetId,
                                                userId,
                                                name)
      fullGset = self._createGridset(row, idxs)
      if fullGset is not None and fillMatrices:
         mtxs = self.getMatricesForGridset(fullGset.getId(), None)
         for m in mtxs:
            # addMatrix sets userid
            fullGset.addMatrix(m)
      return fullGset

# .............................................................................
   def countGridsets(self, userId, shpgrdLyrid, metastring, afterTime, beforeTime, epsg):
      """
      @summary: Count Matrices matching filter conditions 
      @param userId: User (owner) for which to return MatrixColumns.  
      @param shpgrdLyrid: filter by ShapeGrid with Layer database ID 
      @param metastring: find gridsets containing this word in the metadata
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @return: a count of Matrices
      """
      metamatch = None
      if metastring is not None:
         metamatch = '%{}%'.format(metastring)
      row, idxs = self.executeSelectOneFunction('lm_countGridsets', userId, 
                           shpgrdLyrid, metamatch, afterTime, beforeTime, epsg)
      return self._getCount(row)

# .............................................................................
   def listGridsets(self, firstRecNum, maxNum, userId, shpgrdLyrid, metastring, 
                    afterTime, beforeTime, epsg, atom):
      """
      @summary: Return Matrix Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return MatrixColumns.  
      @param shpgrdLyrid: filter by ShapeGrid with Layer database ID 
      @param metastring: find matrices containing this word in the metadata
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of Matrix atoms or full objects
      """
      metamatch = None
      if metastring is not None:
         metamatch = '%{}%'.format(metastring)
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listGridsetAtoms', 
                                 firstRecNum, maxNum, userId, shpgrdLyrid, 
                                 metamatch, afterTime, beforeTime, epsg)
         objs = self._getAtoms(rows, idxs, LMServiceType.GRIDSETS)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listGridsetObjects', 
                                 firstRecNum, maxNum, userId, shpgrdLyrid, 
                                 metamatch, afterTime, beforeTime, epsg)
         for r in rows:
            objs.append(self._createGridset(r, idxs))
      return objs

# ...............................................
   def updateGridset(self, grdset):
      """
      @summary: Update a LmServer.legion.Gridset
      @param grdset: the LmServer.legion.Gridset object to update
      @return: Boolean success/failure
      """
      meta = grdset.dumpGrdMetadata()
      success = self.executeModifyFunction('lm_updateGridset', 
                                           grdset.getId(), 
                                           grdset.treeId, 
                                           grdset.getDLocation(),
                                           meta, grdset.modTime)
      return success
   
# ...............................................
   def getMatrix(self, mtxId, gridsetId, gridsetName, userId, mtxType, 
                 gcmCode, altpredCode, dateCode):
      """
      @summary: Retrieve an LmServer.legion.LMMatrix object with its gridset 
                from the database
      @param mtxId: database ID for the LMMatrix
      @param gridsetId: database ID for the Gridset containing the LMMatrix
      @param gridsetName: name of the Gridset containing the LMMatrix
      @param userId: userID for the Gridset containing the LMMatrix
      @param mtxType: LmCommon.common.lmconstants.MatrixType of the LMMatrix
      @param gcmCode: Global Climate Model Code of the LMMatrix
      @param altpredCode: alternate prediction code of the LMMatrix
      @param dateCode: date code of the LMMatrix
      @return: Existing LmServer.legion.lmmatrix.LMMatrix
      """
      row, idxs = self.executeSelectOneFunction('lm_getMatrix', 
                                                mtxId, mtxType, gridsetId, 
                                                gcmCode, altpredCode, dateCode, 
                                                gridsetName, userId)
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
   
# .............................................................................
   def countShapeGrids(self, userId, cellsides, cellsize, afterTime, beforeTime, epsg):
      """
      @summary: Count all Layers matching the filter conditions 
      @param userId: User (owner) for which to return occurrencesets.  
      @param cellsides: number of sides of each cell, 4=square, 6=hexagon
      @param cellsize: size of one side of cell in mapUnits
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @return: a count of OccurrenceSets
      """
      row, idxs = self.executeSelectOneFunction('lm_countShapegrids', userId, 
                              cellsides, cellsize, afterTime, beforeTime, epsg)
      return self._getCount(row)

# .............................................................................
   def listShapeGrids(self, firstRecNum, maxNum, userId, cellsides, cellsize, 
                      afterTime, beforeTime, epsg, atom):
      """
      @summary: Return Layer Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return shapegrids.  
      @param cellsides: number of sides of each cell, 4=square, 6=hexagon
      @param cellsize: size of one side of cell in mapUnits
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of Layer atoms or full objects
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listShapegridAtoms', 
                              firstRecNum, maxNum, userId, cellsides, cellsize, 
                              afterTime, beforeTime, epsg)
         objs = self._getAtoms(rows, idxs, LMServiceType.SHAPEGRIDS)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listShapegridObjects', 
                              firstRecNum, maxNum, userId, cellsides, cellsize, 
                              afterTime, beforeTime, epsg)
         for r in rows:
            objs.append(self._createShapeGrid(r, idxs))
      return objs

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
                           'lm_findOrInsertEnvLayer', lyr.getId(), 
                           lyr.getUserId(), lyr.squid, lyr.verify, lyr.name,
                           lyr.getDLocation(), 
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
                     'lm_joinScenarioLayer', scenarioId, lyrid, etid)
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
      @return: LmServer.base.layer2._Layer object
      """
      row, idxs = self.executeSelectOneFunction('lm_getEnvLayer', envlyrId, 
                                 lyrid, lyrverify, lyruser, lyrname, epsgcode)
      lyr = self._createEnvLayer(row, idxs)
      return lyr


# .............................................................................
   def countEnvLayers(self, userId, envCode, gcmcode, altpredCode, dateCode, 
                      afterTime, beforeTime, epsg, envTypeId, scenarioCode):
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
                           afterTime, beforeTime, epsg, envTypeId, scenarioCode)
      return self._getCount(row)

# .............................................................................
   def listEnvLayers(self, firstRecNum, maxNum, userId, envCode, gcmcode, 
                     altpredCode, dateCode, afterTime, beforeTime, epsg, 
                     envTypeId, scenCode, atom):
      """
      @todo: Add scenarioId!!
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
      select * from lm_v3.lm_listEnvLayerAtoms(0,10,'kubi',NULL,NULL,NULL,NULL,
                                                NULL,NULL,NULL,NULL);
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listEnvLayerAtoms', 
                           firstRecNum, maxNum, userId, envCode, gcmcode, 
                           altpredCode, dateCode, afterTime, beforeTime, epsg, 
                           envTypeId, scenCode)
         objs = self._getAtoms(rows, idxs, LMServiceType.ENVIRONMENTAL_LAYERS)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listEnvLayerObjects', 
                           firstRecNum, maxNum, userId, envCode, gcmcode, 
                           altpredCode, dateCode, afterTime, beforeTime, epsg, 
                           envTypeId, scenCode)
         for r in rows:
            objs.append(self._createEnvLayer(r, idxs))
      return objs

# # ...............................................
#    def deleteScenarioLayer(self, envlyr, scenarioId):
#       """
#       @summary: Un-joins EnvLayer from scenario (if not None)
#       @param envlyr: EnvLayer to remove from Scenario
#       @param scenarioId: Id for scenario from which to remove EnvLayer 
#       @return: True/False for success of operation
#       """
#       success = self.executeModifyFunction('lm_deleteScenarioLayer', 
#                                            envlyr.getId(), scenarioId)         
#       return success

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
      if usr.userid != newOrExistingUsr.userid:
         self.log.info('Failed to add new user {}; matching email for user {}'
                       .format(usr.userid, newOrExistingUsr.userid))
      return newOrExistingUsr

# ...............................................
   def findUserForObject(self, layerId, scenCode, occId, matrixId, gridsetId, 
                         mfprocessId):
      """
      @summary: find a userId for an LM Object identifier in the database
      @param layerId: the database primary key for a Layer
      @param scenCode: the code for a Scenario
      @param occId: the database primary key for a Layer in the database
      @param matrixId: the database primary key for a Matrix
      @param gridsetId: the database primary key for a Gridset
      @param mfprocessId: the database primary key for a MFProcess
      @return: a userId string
      """
      row, idxs = self.executeSelectOneFunction('lm_findUserForObject',
                     layerId, scenCode, occId, matrixId, gridsetId, mfprocessId)
      userId = row[0]
      return userId

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

# ...............................................
   def deleteComputedUserData(self, userId):
      """
      @summary: Deletes User OccurrenceSet and any dependent SDMProjects 
               (with Layer), dependent MatrixColumns, and all Makeflows
      @param userId: User for whom to delete SDM records, MatrixColumns, Makeflows
      @return: True/False for success of operation
      @note: All makeflows will be deleted, regardless
      """
      success = False
      occDelCount = self.executeModifyFunction('lm_clearComputedUserData', userId)
      self.log.info('Deleted {} OccurrenceLayers, all dependent objects, and all Makeflows for user {}'
                    .format(occDelCount, userId))

      if occDelCount > 0:
         success = True
      return success 

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
      """
      @summary: Return the taxonomy source info given the name
      @param taxonSourceName: unique name of this taxonomy source
      @return: database id, url, and modification time of this source
      """
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
      """
      @summary: Insert a taxon associated with a TaxonomySource into the 
                database.  
      @param taxonSourceId: Lifemapper database ID of the TaxonomySource
      @param taxonKey: unique identifier of the taxon in the (external) 
             TaxonomySource 
      @param sciName: ScientificName object with taxonomy information for this taxon
      @return: new or existing ScientificName
      """
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
   def getTaxon(self, squid, taxonSourceId, taxonKey, userId, taxonName):
      """
      @summary: Find a taxon associated with a TaxonomySource from database.
      @param squid: Hash value of either taxonSourceId+taxonKey 
                    or userId+taxonName
      @param taxonSourceId: Lifemapper database ID of the TaxonomySource
      @param taxonKey: unique identifier of the taxon in the (external) 
             TaxonomySource 
      @param userId: User id for the scenario to be fetched.
      @param taxonName: name string for this taxon
      @return: existing ScientificName
      """
      row, idxs = self.executeSelectOneFunction('lm_getTaxon', squid,
                                    taxonSourceId, taxonKey, userId, taxonName)
      scientificname = self._createScientificName(row, idxs)
      
      return scientificname

# .............................................................................
   def getScenario(self, scenid=None, userId=None, code=None, fillLayers=False):
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
      row, idxs = self.executeSelectOneFunction('lm_getScenario', scenid, 
                                                userId, code)
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
   def updateOccurrenceSet(self, occ):
      """
      @summary: Update OccurrenceLayer attributes: 
                verify, displayName, dlocation, rawDlocation, queryCount, 
                bbox, metadata, status, statusModTime, geometries if valid
      @note: Does not update the userid, squid, and epsgcode (unique constraint) 
      @param occ: OccurrenceLayer to be updated.  
      @return: True/False for successful update.
      """
      success = False
      polyWkt = pointsWkt = None
      metadata = occ.dumpLyrMetadata()
      try:
         polyWkt = occ.getConvexHullWkt()
      except:
         pass
      try:
         pointsWkt = occ.getWkt()
      except:
         pass
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
                              occ.getDLocation(), occ.getRawDLocation(),
                              pointtotal, occ.getCSVExtentString(), occ.epsgcode,
                              occ.dumpLyrMetadata(),
                              occ.status, occ.statusModTime, polywkt, pointswkt)
      newOrExistingOcc = self._createOccurrenceLayer(row, idxs)
      return newOrExistingOcc

# .............................................................................
   def countOccurrenceSets(self, userId, squid, minOccurrenceCount, displayName, 
                        afterTime, beforeTime, epsg, afterStatus, beforeStatus,
                        gridsetId):
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
      @param gridsetId: filter by occurrenceset used by this gridset
      @return: a list of OccurrenceSet atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      row, idxs = self.executeSelectOneFunction('lm_countOccSets', userId, squid,
                                                minOccurrenceCount, displayName,
                                                afterTime, beforeTime, epsg,
                                                afterStatus, beforeStatus,
                                                gridsetId)
      return self._getCount(row)

# .............................................................................
   def listOccurrenceSets(self, firstRecNum, maxNum, userId, squid, 
                          minOccurrenceCount, displayName, afterTime, beforeTime, 
                          epsg, afterStatus, beforeStatus, gridsetId, atom):
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
      @param gridsetId: filter by occurrenceset used by this gridset
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of OccurrenceSet atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listOccSetAtoms', 
                              firstRecNum, maxNum, userId, squid, minOccurrenceCount,
                              displayName, afterTime, beforeTime, epsg, 
                              afterStatus, beforeStatus, gridsetId)
         objs = self._getAtoms(rows, idxs, LMServiceType.OCCURRENCES)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listOccSetObjects', 
                              firstRecNum, maxNum, userId, squid, minOccurrenceCount,
                              displayName, afterTime, beforeTime, epsg, 
                              afterStatus, beforeStatus, gridsetId)
         for r in rows:
            objs.append(self._createOccurrenceLayer(r, idxs))
      return objs

# ...............................................
   def deleteOccurrenceSet(self, occ):
      """
      @summary: Deletes OccurrenceSet and any dependent SDMProjects (with Layer).  
      @param occ: OccurrenceSet to delete
      @return: True/False for success of operation
      @note: If dependent SDMProject is input to a MatrixColumn of a 
             Rolling (Global) PAM, the MatrixColumn will also be deleted.
      """
      pavDelcount = self._deleteOccsetDependentMatrixCols(occ.getId(), occ.getUserId())
      success = self.executeModifyFunction('lm_deleteOccurrenceSet', occ.getId())
      return success

# ...............................................
   def _deleteOccsetDependentMatrixCols(self, occId, usr):
      """
      @summary: Deletes dependent MatrixColumns IFF they belong to a ROLLING_PAM  
                for the OccurrenceSet specified by occId
      @param occId: OccurrenceSet for which to delete dependent MatrixCols
      @param usr: User (owner) of the OccurrenceSet for which to delete MatrixCols
      @return: Count of MatrixCols for success of operation
      """
      delcount = 0
      gpamMtxAtoms = self.listMatrices(0, 500, usr, MatrixType.ROLLING_PAM, None, 
                                       None, None, None, None, None, None, None, 
                                       None, None, True)
      self.log.info('{} ROLLING PAMs for User {}'.format(len(gpamMtxAtoms), usr))
      if len(gpamMtxAtoms) > 0:
         gpamIds = [gpam.getId() for gpam in gpamMtxAtoms]
         # Database will trigger delete of dependent projections on Occset delete
         _, pavs = self._findOccsetDependents(occId, usr, returnProjs=False, 
                                              returnMtxCols=True)
         for pav in pavs:
            if pav.parentId in gpamIds:
               success = self.executeModifyFunction('lm_deleteMatrixColumn', 
                                                    pav.getId())
               if success:
                  delcount += 1
         self.log.info('Deleted {} PAVs from {} ROLLING PAMs'
                       .format(len(gpamIds), delcount))
      return delcount

# ...............................................
   def _findOccsetDependents(self, occId, usr, returnProjs=True, returnMtxCols=True):
      """
      @summary: Finds any dependent SDMProjects and MatrixColumns for the 
                OccurrenceSet specified by occId
      @param occId: OccurrenceSet for which to find dependents
      @param usr: User (owner) of the OccurrenceSet for which to find dependents.
      @param returnProjs: flag indicating whether to return projection objects
             (True) or empty list (False)
      @param returnMtxCols: flag indicating whether to return MatrixColumn 
             objects (True) or empty list (False)
      @return: list of projection atoms/objects, list of MatrixColumns
      """
      pavs = []
      prjs = self.listSDMProjects(0, 500, usr, None, None, None, None, None, 
                                  None, None, occId, None, None, None, not(returnProjs))
      if returnMtxCols:
         for prj in prjs:
            layerid = prj.getId() 
            pavs = self.listMatrixColumns(0, 500, usr, None, None, None, None, 
                                          None, None, None, None, layerid, False)
      if not returnProjs:
         prjs = []
      return prjs, pavs
            
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
                     lyrmeta, proj.dataFormat, proj.gdalType,
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
   def countSDMProjects(self, userId, squid, displayName, 
                        afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                        occsetId, algCode, mdlscenCode, prjscenCode, gridsetId):
      """
      @summary: Count all SDMProjects matching the filter conditions 
      @param userId: User (owner) for which to return occurrencesets.  
      @param squid: a species identifier, tied to a ScientificName
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
      @param gridsetId: filter by projection included in this gridset
      @return: a count of SDMProjects 
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      row, idxs = self.executeSelectOneFunction('lm_countSDMProjects', 
                           userId, squid, displayName, afterTime, beforeTime, epsg,
                           afterStatus, beforeStatus, occsetId, algCode, 
                           mdlscenCode, prjscenCode, gridsetId)
      return self._getCount(row)

# .............................................................................
   def listSDMProjects(self, firstRecNum, maxNum, userId, squid, displayName, 
                       afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                       occsetId, algCode, mdlscenCode, prjscenCode, gridsetId, 
                       atom):
      """
      @summary: Return SDMProjects Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return occurrencesets.  
      @param squid: a species identifier, tied to a ScientificName
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
      @param gridsetId: filter by projection included in this gridset
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of SDMProjects atoms or full objects
      """
      if displayName is not None:
         displayName = displayName.strip() + '%'
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listSDMProjectAtoms', 
                           firstRecNum, maxNum, userId, squid, displayName, afterTime, 
                           beforeTime, epsg, afterStatus, beforeStatus, occsetId, 
                           algCode, mdlscenCode, prjscenCode, gridsetId)
         objs = self._getAtoms(rows, idxs, LMServiceType.PROJECTIONS)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listSDMProjectObjects', 
                           firstRecNum, maxNum, userId, squid, displayName, afterTime, 
                           beforeTime, epsg, afterStatus, beforeStatus, occsetId, 
                           algCode, mdlscenCode, prjscenCode, gridsetId)
         for r in rows:
            objs.append(self._createSDMProjection(r, idxs))
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
         lyrid = mtxcol.layer.getLayerId()
         if lyrid is None:
            newOrExistingLyr = self.findOrInsertBaseLayer(mtxcol.layer)
            lyrid = newOrExistingLyr.getLayerId()

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
   def getMatrixColumn(self, mtxcol, mtxcolId):
      """
      @summary: Get an existing MatrixColumn
      @param mtxcol: a MatrixColumn object with unique parameters matching the 
                     existing MatrixColumn to return 
      @param mtxcolId: a database ID for the LmServer.legion.MatrixColumn 
                     object to return 
      @return: a LmServer.legion.MatrixColumn object
      """
      row = None
      if mtxcol is not None:
         intparams = mtxcol.dumpIntersectParams()
         row, idxs = self.executeSelectOneFunction('lm_getMatrixColumn', 
                                                   mtxcol.getId(),
                                                   mtxcol.parentId,
                                                   mtxcol.getMatrixIndex(),
                                                   mtxcol.getLayerId(),
                                                   intparams)
      elif mtxcolId is not None:
         row, idxs = self.executeSelectOneFunction('lm_getMatrixColumn', 
                                             mtxcolId, None, None, None, None)
      mtxColumn = self._createMatrixColumn(row, idxs)
      return mtxColumn

# ...............................................
   def getColumnsForMatrix(self, mtxId):
      """
      @summary: Get all existing MatrixColumns for a Matrix
      @param mtxId: a database ID for the LmServer.legion.LMMatrix 
                     object to return columns for
      @return: a list of LmServer.legion.MatrixColumn objects
      """
      mtxColumns = []
      if mtxId is not None:
         rows, idxs = self.executeSelectManyFunction('lm_getColumnsForMatrix', 
                                                     mtxId)
         for r in rows:
            mtxcol = self._createMatrixColumn(r, idxs)
            mtxColumns.append(mtxcol)
      return mtxColumns

# ...............................................
   def getSDMColumnsForMatrix(self, mtxId, returnColumns, returnProjections):
      """
      @summary: Get all existing MatrixColumns and SDMProjections that have  
                SDMProjections as input layers for a Matrix
      @param mtxId: a database ID for the LmServer.legion.LMMatrix 
                     object to return columns for
      @param returnColumns: option to return MatrixColumn objects
      @param returnProjections: option to return SDMProjection objects
      @return: a list of tuples containing LmServer.legion.MatrixColumn
               and LmServer.legion.SDMProjection objects.  Either may be None
               if the option is False  
      """
      colPrjPairs = []
      if mtxId is not None:
         rows, idxs = self.executeSelectManyFunction('lm_getSDMColumnsForMatrix', 
                                                     mtxId)
         for r in rows:
            mtxcol = sdmprj = layer = None
            if returnColumns:
               mtxcol = self._createMatrixColumn(r, idxs)
               layer = mtxcol.layer
            if returnProjections:
               sdmprj = self._createSDMProjection(r, idxs, layer=layer)
            colPrjPairs.append((mtxcol, sdmprj))
      return colPrjPairs

# ...............................................
   def getOccLayersForMatrix(self, mtxId):
      """
      @summary: Get all existing OccurrenceLayer objects that are inputs to  
                SDMProjections used as input layers for a Matrix
      @param mtxId: a database ID for the LmServer.legion.LMMatrix 
                     object to return columns for
      @return: a list of LmServer.legion.OccurrenceLayer objects
      """
      occsets = []
      if mtxId is not None:
         rows, idxs = self.executeSelectManyFunction('lm_getOccLayersForMatrix', 
                                                     mtxId)
         for r in rows:
            occsets.append(self._createOccurrenceLayer(r, idxs))
      return occsets

# .............................................................................
   def countMatrixColumns(self, userId, squid, ident, afterTime, beforeTime, 
                          epsg, afterStatus, beforeStatus, matrixId, layerId):
      """
      @summary: Return count of MatrixColumns matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return MatrixColumns.  
      @param squid: a species identifier, tied to a ScientificName
      @param ident: a layer identifier for non-species data
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @param matrixId: filter by Matrix identifier
      @param layerId: filter by Layer input identifier
      @return: a count of MatrixColumns
      """
      row, idxs = self.executeSelectOneFunction('lm_countMtxCols', userId, 
                              squid, ident, afterTime, beforeTime, epsg, 
                              afterStatus, beforeStatus, matrixId, layerId)
      return self._getCount(row)

# .............................................................................
   def listMatrixColumns(self, firstRecNum, maxNum, userId, squid, ident, 
                         afterTime, beforeTime, epsg, afterStatus, beforeStatus, 
                         matrixId, layerId, atom):
      """
      @summary: Return MatrixColumn Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return MatrixColumns.  
      @param squid: a species identifier, tied to a ScientificName
      @param ident: a layer identifier for non-species data
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @param matrixId: filter by Matrix identifier
      @param layerId: filter by Layer input identifier
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of MatrixColumn atoms or full objects
      """
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listMtxColAtoms', 
                                 firstRecNum, maxNum, userId, squid, ident, 
                                 afterTime, beforeTime, epsg, afterStatus, 
                                 beforeStatus, matrixId, layerId)
         objs = self._getAtoms(rows, idxs)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listMtxColObjects', 
                                 firstRecNum, maxNum, userId, squid, ident, 
                                 afterTime, beforeTime, epsg, afterStatus, 
                                 beforeStatus, matrixId, layerId)
         for r in rows:
            objs.append(self._createMatrixColumn(r, idxs))
      return objs

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
                                           mtx.getId(), mtx.getDLocation(),
                                           meta, mtx.status, mtx.statusModTime)
      return success
   
# .............................................................................
   def countMatrices(self, userId, matrixType, gcmCode, altpredCode, dateCode, 
                     metastring, gridsetId, afterTime, beforeTime, epsg, 
                     afterStatus, beforeStatus):
      """
      @summary: Count Matrices matching filter conditions 
      @param userId: User (owner) for which to return MatrixColumns.  
      @param matrixType: filter by LmCommon.common.lmconstants.MatrixType
      @param gcmCode: filter by the Global Climate Model code
      @param altpredCode: filter by the alternate predictor code (i.e. IPCC RCP)
      @param dateCode: filter by the date code
      @param metastring: find matrices containing this word in the metadata
      @param gridsetId: find matrices in the Gridset with this identifier
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @return: a count of Matrices
      """
      metamatch = None
      if metastring is not None:
         metamatch = '%{}%'.format(metastring)
      row, idxs = self.executeSelectOneFunction('lm_countMatrices', userId, 
                                 matrixType, gcmCode, altpredCode, dateCode, 
                                 metamatch, gridsetId, afterTime, beforeTime, 
                                 epsg, afterStatus, beforeStatus)
      return self._getCount(row)

# .............................................................................
   def listMatrices(self, firstRecNum, maxNum, userId, matrixType, gcmCode, 
                    altpredCode, dateCode, metastring, gridsetId, afterTime, 
                    beforeTime, epsg, afterStatus, beforeStatus, atom):
      """
      @summary: Return Matrix Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return MatrixColumns.  
      @param matrixType: filter by LmCommon.common.lmconstants.MatrixType
      @param gcmCode: filter by the Global Climate Model code
      @param altpredCode: filter by the alternate predictor code (i.e. IPCC RCP)
      @param dateCode: filter by the date code
      @param metastring: find matrices containing this word in the metadata
      @param gridsetId: find matrices in the Gridset with this identifier
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param epsg: filter by this EPSG code
      @param afterStatus: filter by status >= value
      @param beforeStatus: filter by status <= value
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of Matrix atoms or full objects
      """
      metamatch = None
      if metastring is not None:
         metamatch = '%{}%'.format(metastring)
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listMatrixAtoms', 
                                 firstRecNum, maxNum, userId, matrixType, 
                                 gcmCode, altpredCode, dateCode, metamatch, 
                                 gridsetId, afterTime, beforeTime, epsg, 
                                 afterStatus, beforeStatus)
         objs = self._getAtoms(rows, idxs, LMServiceType.MATRICES)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listMatrixObjects', 
                                 firstRecNum, maxNum, userId, matrixType, 
                                 gcmCode, altpredCode, dateCode, metamatch, 
                                 gridsetId, afterTime, beforeTime, epsg, 
                                 afterStatus, beforeStatus)
         for r in rows:
            objs.append(self._createLMMatrix(r, idxs))
      return objs

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
                     mtx.getDLocation(), meta, mtx.status, 
                     mtx.statusModTime)
      newOrExistingMtx = self._createLMMatrix(row, idxs)
      return newOrExistingMtx

# .............................................................................
   def countTrees(self, userId, name, isBinary, isUltrametric, hasBranchLengths,
                  metastring, afterTime, beforeTime):
      """
      @summary: Count Trees matching filter conditions 
      @param userId: User (owner) for which to return Trees.  
      @param name: filter by name
      @param isBinary: filter by boolean binary attribute
      @param isUltrametric: filter by boolean ultrametric attribute
      @param hasBranchLengths: filter by boolean hasBranchLengths attribute
      @param metastring: find trees containing this word in the metadata
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @return: a count of Tree
      """
      metamatch = None
      if metastring is not None:
         metamatch = '%{}%'.format(metastring)
      row, idxs = self.executeSelectOneFunction('lm_countTrees', userId, 
                                 afterTime, beforeTime, name, metamatch,  
                                 isBinary, isUltrametric, hasBranchLengths)
      return self._getCount(row)

# .............................................................................
   def listTrees(self, firstRecNum, maxNum, userId, afterTime, beforeTime, 
                 name, metastring,  isBinary, isUltrametric, hasBranchLengths, 
                 atom):
      """
      @summary: Return Tree Objects or Atoms matching filter conditions 
      @param firstRecNum: The first record to return, 0 is the first record
      @param maxNum: Maximum number of records to return
      @param userId: User (owner) for which to return Trees.  
      @param afterTime: filter by modified at or after this time
      @param beforeTime: filter by modified at or before this time
      @param isBinary: filter by boolean binary attribute
      @param isUltrametric: filter by boolean ultrametric attribute
      @param hasBranchLengths: filter by boolean hasBranchLengths attribute
      @param name: filter by name
      @param metastring: find trees containing this word in the metadata
      @param atom: True if return objects will be Atoms, False if full objects
      @return: a list of Tree atoms or full objects
      """
      metamatch = None
      if metastring is not None:
         metamatch = '%{}%'.format(metastring)
      if atom:
         rows, idxs = self.executeSelectManyFunction('lm_listTreeAtoms', 
                     firstRecNum, maxNum, userId, afterTime, beforeTime, 
                     name, metamatch, isBinary, isUltrametric, hasBranchLengths)
         objs = self._getAtoms(rows, idxs, LMServiceType.TREES)
      else:
         objs = []
         rows, idxs = self.executeSelectManyFunction('lm_listTreeObjects', 
                     firstRecNum, maxNum, userId, afterTime, beforeTime, 
                     name, metamatch, isBinary, isUltrametric, hasBranchLengths)
         for r in rows:
            objs.append(self._createTree(r, idxs))
      return objs

# ...............................................
   def findOrInsertTree(self, tree):
      """
      @summary: Find existing OR save a new Tree
      @param tree: the Tree object to insert
      @return new or existing Tree
      """
      meta = tree.dumpTreeMetadata()
      row, idxs = self.executeInsertAndSelectOneFunction('lm_findOrInsertTree', 
                     tree.getId(), tree.getUserId(), tree.name, 
                     tree.getDLocation(), tree.isBinary(), tree.isUltrametric(),
                     tree.hasBranchLengths(), meta, tree.modTime)
      newOrExistingTree = self._createTree(row, idxs)
      return newOrExistingTree

# ...............................................
   def getTree(self, tree, treeId):
      """
      @summary: Retrieve a Tree from the database
      @param tree: Tree to retrieve
      @param treeId: Database ID of Tree to retrieve
      @return: Existing Tree
      """
      row = None
      if tree is not None:
         row, idxs = self.executeSelectOneFunction('lm_getTree', tree.getId(),
                                                   tree.getUserId(),
                                                   tree.name)
      else:
         row, idxs = self.executeSelectOneFunction('lm_getTree', treeId,
                                                   None, None)
      existingTree = self._createTree(row, idxs)
      return existingTree

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
   def updateObject(self, obj):
      """
      @summary: Updates object in database
      @return: True/False for success of operation
      """
      if isinstance(obj, OccurrenceLayer):
         success = self.updateOccurrenceSet(obj)
      elif isinstance(obj, SDMProjection):
         success = self.updateSDMProject(obj)
      elif isinstance(obj, ShapeGrid):
         success = self.updateShapeGrid(obj)
      elif isinstance(obj, LMMatrix):
         success = self.updateMatrix(obj)
      elif isinstance(obj, Tree):
         meta = obj.dumpTreeMetadata()
         success = self.executeModifyFunction('lm_updateTree', obj.getId(),
                                              obj.getDLocation(), 
                                              obj.isBinary(), 
                                              obj.isUltrametric(), 
                                              obj.hasBranchLengths(),
                                              meta, obj.modTime)
      elif isinstance(obj, MFChain):
         success = self.executeModifyFunction('lm_updateMFChain', obj.objId,
                                              obj.getDLocation(), 
                                              obj.status, obj.statusModTime)
      elif isinstance(obj, Gridset):
         success = self.updateGridset(obj)
      else:
         raise LMError('Unsupported update for object {}'.format(type(obj)))
      return success

# ...............................................
   def deleteObject(self, obj):
      """
      @summary: Deletes object from database
      @return: True/False for success of operation
      @note: OccurrenceSet delete cascades to SDMProject but not MatrixColumn
      @note: MatrixColumns for Global PAM should be deleted or reset on 
             OccurrenceSet delete or recalc 
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
         success = self.deleteOccurrenceSet(obj)
      elif isinstance(obj, SDMProjection):
         success = self.executeModifyFunction('lm_deleteSDMProjectLayer', objid)
      elif isinstance(obj, ShapeGrid):
         success = self.executeModifyFunction('lm_deleteShapeGrid', objid)
      elif isinstance(obj, Scenario):
         # Deletes ScenarioLayer join; only deletes layers if they are orphaned
         for lyr in obj.layers:
            success = self.executeModifyFunction('lm_deleteScenarioLayer', 
                                                 lyr.getId(), objid)         
         success = self.executeModifyFunction('lm_deleteScenario', objid)
      elif isinstance(obj, MatrixColumn):
         success = self.executeModifyFunction('lm_deleteMatrixColumn', objid)
      elif isinstance(obj, Tree):
         success = self.executeModifyFunction('lm_deleteTree', objid)
      else:
         raise LMError('Unsupported delete for object {}'.format(type(obj)))
      return success

# ...............................................
   def getMatricesForGridset(self, gridsetid, mtxType):
      """
      @summary Return all LmServer.legion.LMMatrix objects that are part of a 
               gridset
      @param gridsetid: Id of the gridset organizing these data matrices
      @param mtxType: optional filter for one type of LMMatrix
      """
      mtxs = []
      rows, idxs = self.executeSelectManyFunction('lm_getMatricesForGridset', 
                                                  gridsetid, mtxType)
      for r in rows:
         mtxs.append(self._createLMMatrix(r, idxs))
      return mtxs

