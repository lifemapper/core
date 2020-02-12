"""Module containing lower level functions for accessing database
"""
from collections import namedtuple

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import MatrixType, LMFormat, JobStatus
from LmCommon.common.time import gmt, LmTime
from LmServer.base.dbpgsql import DbPostgresql
from LmServer.base.layer2 import Raster, Vector
from LmServer.base.taxon import ScientificName
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
from LmServer.legion.sdmproj import SDMProjection
from LmServer.legion.shapegrid import ShapeGrid
from LmServer.legion.tree import Tree


# .............................................................................
class Borg(DbPostgresql):
    """Class to control modifications to the Borg database.
    """
    # ................................
    def __init__(self, logger, db_host, db_port, db_user, db_key):
        """Constructor for Borg class

        Args:
            logger (LmLogger): Logger to use for Borg
            db_host (str): hostname for database machine
            db_port (int): Port number for database connection
            db_user (str): Database user name for the connection
            db_key (str): Password for database user
        """
        DbPostgresql.__init__(
            self, logger, db=DB_STORE, user=db_user, password=db_key,
            host=db_host, port=db_port, schema=LM_SCHEMA_BORG)

    # ................................
    @staticmethod
    def _create_user(row, idxs):
        """Create an LMUser object from a database row
        """
        usr = None
        if row is not None:
            usr = LMUser(
                row[idxs['userid']], row[idxs['email']], row[idxs['password']],
                is_encrypted=True, first_name=row[idxs['firstname']],
                last_name=row[idxs['lastname']],
                institution=row[idxs['institution']],
                addr_1=row[idxs['address1']], addr_2=row[idxs['address2']],
                addr_3=row[idxs['address3']], phone=row[idxs['phone']],
                mod_time=row[idxs['modtime']])
        return usr

    # ................................
    def _create_scientific_name(self, row, idxs):
        """Create a ScientificName object from a database row.

        Returns:
            ScientificName
        """
        sci_name = None
        if row is not None:
            scientific_name = self._get_column_value(row, idxs, ['sciname'])

            if scientific_name is not None:
                taxon_id = self._get_column_value(row, idxs, ['taxonid'])
                taxonomy_source_id = self._get_column_value(
                    row, idxs, ['taxonomysourceid'])
                user = self._get_column_value(row, idxs, ['userid'])
                src_key = self._get_column_value(row, idxs, ['taxonomykey'])
                squid = self._get_column_value(row, idxs, ['squid'])
                kingdom = self._get_column_value(row, idxs, ['kingdom'])
                phylum = self._get_column_value(row, idxs, ['phylum'])
                class_ = self._get_column_value(row, idxs, ['tx_class'])
                order_ = self._get_column_value(row, idxs, ['tx_order'])
                family = self._get_column_value(row, idxs, ['family'])
                genus = self._get_column_value(row, idxs, ['genus'])
                rank = self._get_column_value(row, idxs, ['rank'])
                canonical = self._get_column_value(row, idxs, ['canonical'])
                gen_key = self._get_column_value(row, idxs, ['genuskey'])
                sp_key = self._get_column_value(row, idxs, ['specieskey'])
                hier = self._get_column_value(row, idxs, ['keyhierarchy'])
                last_count = self._get_column_value(row, idxs, ['lastcount'])
                mod_time = self._get_column_value(
                    row, idxs, ['taxmodtime', 'modtime'])

                sci_name = ScientificName(
                    scientific_name, rank=rank, canonical_name=canonical,
                    user_id=user, squid=squid, kingdom=kingdom, phylum=phylum,
                    class_=class_, order_=order_, family=family, genus=genus,
                    last_occurrence_count=last_count, mod_time=mod_time,
                    taxonomy_source_id=taxonomy_source_id,
                    taxonomy_source_key=src_key,
                    taxonomy_source_genus_key=gen_key,
                    taxonomy_source_species_key=sp_key,
                    taxonomy_source_key_hierarchy=hier,
                    scientific_name_id=taxon_id)
        return sci_name

    # ................................
    def _create_algorithm(self, row, idxs):
        """Created only from a model, lm_fullModel, or lm_fullProjection
        """
        code = self._get_column_value(row, idxs, ['algorithmcode'])
        params = self._get_column_value(row, idxs, ['algparams'])
        try:
            alg = Algorithm(code, parameters=params)
        except Exception:
            alg = None

        return alg

    # ................................
    def _create_mf_chain(self, row, idxs):
        mf_chain = None
        if row is not None:
            mf_chain = MFChain(
                self._get_column_value(row, idxs, ['userid']),
                dlocation=self._get_column_value(
                    row, idxs, ['mfpdlocation', 'dlocation']),
                priority=self._get_column_value(row, idxs, ['priority']),
                metadata=self._get_column_value(
                    row, idxs, ['mfpmetadata', 'metadata']),
                status=self._get_column_value(
                    row, idxs, ['mfpstatus', 'status']),
                status_mod_time=self._get_column_value(
                    row, idxs, ['mfpstatusmodtime', 'statusmodtime']),
                mf_chain_id=self._get_column_value(row, idxs, ['mfprocessid']))
        return mf_chain

    # ................................
    def _create_scenario(self, row, idxs, is_for_model=True):
        """Create a scenario object

        Note:
            Created only from Scenario table or lm_sdmproject view
        """
        scen = None
        if is_for_model:
            scen_id = self._get_column_value(
                row, idxs, ['mdlscenarioid', 'scenarioid'])
            scen_code = self._get_column_value(
                row, idxs, ['mdlscenariocode', 'scenariocode'])
            meta = self._get_column_value(
                row, idxs, ['mdlscenmetadata', 'scenmetadata', 'metadata'])
            gcm_code = self._get_column_value(
                row, idxs, ['mdlscengcmcode', 'gcmcode'])
            alt_pred_code = self._get_column_value(
                row, idxs, ['mdlscenaltpredcode', 'altpredcode'])
            date_code = self._get_column_value(
                row, idxs, ['mdlscendatecode', 'datecode'])
        else:
            scen_id = self._get_column_value(
                row, idxs, ['prjscenarioid', 'scenarioid'])
            scen_code = self._get_column_value(
                row, idxs, ['prjscenariocode', 'scenariocode'])
            meta = self._get_column_value(
                row, idxs, ['prjscenmetadata', 'metadata'])
            gcm_code = self._get_column_value(
                row, idxs, ['prjscengcmcode', 'gcmcode'])
            alt_pred_code = self._get_column_value(
                row, idxs, ['prjscenaltpredcode', 'altpredcode'])
            date_code = self._get_column_value(
                row, idxs, ['prjscendatecode', 'datecode'])

        usr = self._get_column_value(row, idxs, ['userid'])
        units = self._get_column_value(row, idxs, ['units'])
        res = self._get_column_value(row, idxs, ['resolution'])
        epsg = self._get_column_value(row, idxs, ['epsgcode'])
        bbox = self._get_column_value(row, idxs, ['bbox'])
        mod_time = self._get_column_value(
            row, idxs, ['scenmodtime', 'modtime'])

        if row is not None:
            scen = Scenario(
                scen_code, usr, epsg, metadata=meta, units=units, res=res,
                gcm_code=gcm_code, alt_pred_code=alt_pred_code,
                date_code=date_code, bbox=bbox, mod_time=mod_time, layers=None,
                scenario_id=scen_id)
        return scen

    # ................................
    def _createScenPackage(self, row, idxs):
        """
        @note: created only from Scenario table or lm_sdmproject view
        """
        scen = None
        pkgid = self._get_column_value(row, idxs, ['scenpackageid'])
        usr = self._get_column_value(row, idxs, ['userid'])
        name = self._get_column_value(row, idxs, ['pkgname', 'name'])
        meta = self._get_column_value(row, idxs, ['pkgmetadata', 'metadata'])
        epsg = self._get_column_value(row, idxs, ['pkgepsgcode', 'epsgcode'])
        bbox = self._get_column_value(row, idxs, ['pkgbbox', 'bbox'])
        units = self._get_column_value(row, idxs, ['pkgunits', 'units'])
        mod_time = self._get_column_value(row, idxs, ['pkgmodtime', 'modtime'])

        if row is not None:
            scen = ScenPackage(name, usr, metadata=meta, epsgcode=epsg, bbox=bbox,
                                     mapunits=units, mod_time=mod_time,
                                     scenPackageId=pkgid)
        return scen

    # ................................
    def _createEnvType(self, row, idxs):
        """
        @summary: Create an _EnvironmentalType from a database EnvType record, or 
                     lm_envlayer, lm_scenlayer view
        """
        lyrType = None
        if row is not None:
            envcode = self._get_column_value(row, idxs, ['envcode'])
            gcmcode = self._get_column_value(row, idxs, ['gcmcode'])
            altcode = self._get_column_value(row, idxs, ['altpredcode'])
            dtcode = self._get_column_value(row, idxs, ['datecode'])
            meta = self._get_column_value(row, idxs, ['envmetadata', 'metadata'])
            mod_time = self._get_column_value(row, idxs, ['envmodtime', 'modtime'])
            usr = self._get_column_value(row, idxs, ['envuserid', 'userid'])
            ltid = self._get_column_value(row, idxs, ['envtypeid'])
            lyrType = EnvType(envcode, usr, gcmCode=gcmcode, altpredCode=altcode,
                                    dateCode=dtcode, metadata=meta, mod_time=mod_time,
                                    envTypeId=ltid)
        return lyrType

    # ................................
    def _createGridset(self, row, idxs):
        """
        @summary: Create a Gridset from a database Gridset record or lm_gridset view
        @note: This does not return tree object data, only treeId
        """
        grdset = None
        if row is not None:
            shp = self._createShapeGrid(row, idxs)
            # TODO: return lm_tree instead of lm_gridset (with just treeId)
            tree = self._createTree(row, idxs)
            shpId = self._get_column_value(row, idxs, ['layerid'])
            grdid = self._get_column_value(row, idxs, ['gridsetid'])
            usr = self._get_column_value(row, idxs, ['userid'])
            name = self._get_column_value(row, idxs, ['grdname', 'name'])
            dloc = self._get_column_value(row, idxs, ['grddlocation', 'dlocation'])
            epsg = self._get_column_value(row, idxs, ['grdepsgcode', 'epsgcode'])
            meta = self._get_column_value(row, idxs, ['grdmetadata', 'metadata'])
            mtime = self._get_column_value(row, idxs, ['grdmodtime', 'modtime'])
            grdset = Gridset(name=name, metadata=meta, shapeGrid=shp,
                                  shapeGridId=shpId, tree=tree,
                                  dlocation=dloc, epsgcode=epsg, userId=usr,
                                  gridsetId=grdid, mod_time=mtime)
        return grdset

    # ................................
    def _createTree(self, row, idxs):
        """
        @summary: Create a Tree from a database Tree record
        @todo: Do we want to use binary attributes without reading data?
        """
        tree = None
        if row is not None:
            treeid = self._get_column_value(row, idxs, ['treeid'])
            if treeid is not None:
                usr = self._get_column_value(row, idxs, ['treeuserid', 'userid'])
                name = self._get_column_value(row, idxs, ['treename', 'name'])
                dloc = self._get_column_value(row, idxs, ['treedlocation', 'dlocation'])
                isbin = self._get_column_value(row, idxs, ['isbinary'])
                isultra = self._get_column_value(row, idxs, ['isultrametric'])
                haslen = self._get_column_value(row, idxs, ['hasbranchlengths'])
                meta = self._get_column_value(row, idxs, ['treemetadata', 'metadata'])
                mod_time = self._get_column_value(row, idxs, ['treemodtime', 'modtime'])
                tree = Tree(name, metadata=meta, dlocation=dloc, userId=usr,
                                treeId=treeid, mod_time=mod_time)
        return tree

    # ................................
    def _createLMMatrix(self, row, idxs):
        """
        @summary: Create an LMMatrix from a database Matrix record, or lm_matrix,
                     lm_fullMatrix or lm_gridset view
        """
        mtx = None
        if row is not None:
            grdset = self._createGridset(row, idxs)
            mtxid = self._get_column_value(row, idxs, ['matrixid'])
            mtype = self._get_column_value(row, idxs, ['matrixtype'])
            scenid = self._get_column_value(row, idxs, ['scenarioid'])
#             TODO: replace 3 Codes with scenarioId
            gcm = self._get_column_value(row, idxs, ['gcmcode'])
            rcp = self._get_column_value(row, idxs, ['altpredcode'])
            dt = self._get_column_value(row, idxs, ['datecode'])
            alg = self._get_column_value(row, idxs, ['algorithmcode'])
            dloc = self._get_column_value(row, idxs, ['matrixiddlocation'])
            meta = self._get_column_value(row, idxs, ['mtxmetadata', 'metadata'])
            usr = self._get_column_value(row, idxs, ['userid'])
            stat = self._get_column_value(row, idxs, ['mtxstatus', 'status'])
            stattime = self._get_column_value(row, idxs, ['mtxstatusmodtime',
                                                        'statusmodtime'])
            mtx = LMMatrix(None, matrixType=mtype,
                           scenarioid=scenid,
                           gcmCode=gcm, altpredCode=rcp, dateCode=dt,
                           algCode=alg,
                           metadata=meta, dlocation=dloc, userId=usr,
                           gridset=grdset, matrixId=mtxid,
                           status=stat, status_mod_time=stattime)
        return mtx

    # ................................
    def _createMatrixColumn(self, row, idxs):
        """
        @summary: Create an MatrixColumn from a lm_matrixcolumn view
        """
        mtxcol = None
        if row is not None:
            # Returned by only some functions
            inputLyr = self._createLayer(row, idxs)
            # Ids of joined input layers
            lyrid = self._get_column_value(row, idxs, ['layerid'])
            shpgrdid = self._get_column_value(row, idxs, ['shplayerid'])
            mtxcolid = self._get_column_value(row, idxs, ['matrixcolumnid'])
            mtxid = self._get_column_value(row, idxs, ['matrixid'])
            mtxIndex = self._get_column_value(row, idxs, ['matrixindex'])
            squid = self._get_column_value(row, idxs, ['mtxcolsquid', 'squid'])
            ident = self._get_column_value(row, idxs, ['mtxcolident', 'ident'])
            mtxcolmeta = self._get_column_value(row, idxs, ['mtxcolmetatadata'])
            intparams = self._get_column_value(row, idxs, ['intersectparams'])
            mtxcolstat = self._get_column_value(row, idxs, ['mtxcolstatus'])
            mtxcolstattime = self._get_column_value(row, idxs, ['mtxcolstatusmodtime'])
            usr = self._get_column_value(row, idxs, ['userid'])

            mtxcol = MatrixColumn(mtxIndex, mtxid, usr,
                                layer=inputLyr, layerId=lyrid, shapeGridId=shpgrdid,
                                intersectParams=intparams,
                                squid=squid, ident=ident,
                                processType=None, metadata=mtxcolmeta,
                                matrixColumnId=mtxcolid, status=mtxcolstat,
                                status_mod_time=mtxcolstattime)
        return mtxcol

    # ................................
    def _getLayerInputs(self, row, idxs):
        """
        @summary: Create Raster or Vector layer from a Layer or view in the Borg. 
        @note: OccurrenceSet and SDMProject objects do not use this function
        @note: used with Layer, lm_envlayer, lm_scenlayer, lm_sdmproject, lm_shapegrid
        """
        dbid = self._get_column_value(row, idxs, ['layerid'])
        usr = self._get_column_value(row, idxs, ['lyruserid', 'userid'])
        verify = self._get_column_value(row, idxs, ['lyrverify', 'verify'])
        squid = self._get_column_value(row, idxs, ['lyrsquid', 'squid'])
        name = self._get_column_value(row, idxs, ['lyrname', 'name'])
        dloc = self._get_column_value(row, idxs, ['lyrdlocation', 'dlocation'])
        meta = self._get_column_value(row, idxs, ['lyrmetadata', 'metadata'])
        vtype = self._get_column_value(row, idxs, ['ogrtype'])
        rtype = self._get_column_value(row, idxs, ['gdaltype'])
        vunits = self._get_column_value(row, idxs, ['valunits'])
        vattr = self._get_column_value(row, idxs, ['valattribute'])
        nodata = self._get_column_value(row, idxs, ['nodataval'])
        minval = self._get_column_value(row, idxs, ['minval'])
        maxval = self._get_column_value(row, idxs, ['maxval'])
        fformat = self._get_column_value(row, idxs, ['dataformat'])
        epsg = self._get_column_value(row, idxs, ['epsgcode'])
        munits = self._get_column_value(row, idxs, ['mapunits'])
        res = self._get_column_value(row, idxs, ['resolution'])
        dtmod = self._get_column_value(row, idxs, ['lyrmodtime', 'modtime'])
        bbox = self._get_column_value(row, idxs, ['bbox'])
        return (dbid, usr, verify, squid, name, dloc, meta, vtype, rtype,
                  vunits, vattr, nodata, minval, maxval, fformat, epsg, munits, res,
                  dtmod, bbox)

    # ................................
    def _createLayer(self, row, idxs):
        """
        @summary: Create Raster or Vector layer from a Layer or view in the Borg. 
        @note: OccurrenceSet and SDMProject objects do not use this function
        @note: used with Layer, lm_envlayer, lm_scenlayer, lm_sdmproject, lm_shapegrid
        """
        lyr = None
        if row is not None:
            dbid = self._get_column_value(row, idxs, ['layerid'])
            name = self._get_column_value(row, idxs, ['lyrname', 'name'])
            usr = self._get_column_value(row, idxs, ['lyruserid', 'userid'])
            epsg = self._get_column_value(row, idxs, ['epsgcode'])
            # Layer may be optional
            if (dbid is not None and name is not None and usr is not None and epsg is not None):
                verify = self._get_column_value(row, idxs, ['lyrverify', 'verify'])
                squid = self._get_column_value(row, idxs, ['lyrsquid', 'squid'])
                dloc = self._get_column_value(row, idxs, ['lyrdlocation', 'dlocation'])
                meta = self._get_column_value(row, idxs, ['lyrmetadata', 'metadata'])
                vtype = self._get_column_value(row, idxs, ['ogrtype'])
                rtype = self._get_column_value(row, idxs, ['gdaltype'])
                vunits = self._get_column_value(row, idxs, ['valunits'])
                vattr = self._get_column_value(row, idxs, ['valattribute'])
                nodata = self._get_column_value(row, idxs, ['nodataval'])
                minval = self._get_column_value(row, idxs, ['minval'])
                maxval = self._get_column_value(row, idxs, ['maxval'])
                fformat = self._get_column_value(row, idxs, ['dataformat'])
                munits = self._get_column_value(row, idxs, ['mapunits'])
                res = self._get_column_value(row, idxs, ['resolution'])
                dtmod = self._get_column_value(row, idxs, ['lyrmodtime', 'modtime'])
                bbox = self._get_column_value(row, idxs, ['lyrbbox', 'bbox'])

                if fformat in LMFormat.OGRDrivers():
                    lyr = Vector(name, usr, epsg, lyrId=dbid, squid=squid, verify=verify,
                                     dlocation=dloc, metadata=meta, dataFormat=fformat,
                                     ogrType=vtype, valUnits=vunits, valAttribute=vattr,
                                     nodataVal=nodata, minVal=minval, maxVal=maxval,
                                     mapunits=munits, resolution=res, bbox=bbox,
                                     mod_time=dtmod)
                elif fformat in LMFormat.GDALDrivers():
                    lyr = Raster(name, usr, epsg, lyrId=dbid, squid=squid, verify=verify,
                                     dlocation=dloc, metadata=meta, dataFormat=fformat,
                                     gdalType=rtype, valUnits=vunits, nodataVal=nodata,
                                     minVal=minval, maxVal=maxval, mapunits=munits,
                                     resolution=res, bbox=bbox, mod_time=dtmod)
        return lyr

    # ................................
    def _createEnvLayer(self, row, idxs):
        """
        Create an EnvLayer from a lm_envlayer or lm_scenlayer record in the Borg
        """
        envRst = None
        envLayerId = self._get_column_value(row, idxs, ['envlayerid'])
        if row is not None:
            scenid = self._get_column_value(row, idxs, ['scenarioid'])
            scencode = self._get_column_value(row, idxs, ['scenariocode'])
            rst = self._createLayer(row, idxs)
            if rst is not None:
                etype = self._createEnvType(row, idxs)
                envRst = EnvLayer.initFromParts(rst, etype, envLayerId=envLayerId,
                                                          scencode=scencode)
        return envRst

    # ................................
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
                            self._get_column_value(row, idxs, ['cellsides']),
                            self._get_column_value(row, idxs, ['cellsize']),
                            siteId=self._get_column_value(row, idxs, ['idattribute']),
                            siteX=self._get_column_value(row, idxs, ['xattribute']),
                            siteY=self._get_column_value(row, idxs, ['yattribute']),
                            size=self._get_column_value(row, idxs, ['vsize']),
                            # todo: will these ever be accessed without 'shpgrd' prefix?
                            status=self._get_column_value(row, idxs, ['shpgrdstatus', 'status']),
                            status_mod_time=self._get_column_value(row, idxs,
                                                    ['shpgrdstatusmodtime', 'statusmodtime']))
        return shg

    # ................................
    def _createOccurrenceLayer(self, row, idxs):
        """
        @note: takes OccurrenceSet, lm_occMatrixcolumn, or lm_sdmproject record
        """
        occ = None
        if row is not None:
            name = self._get_column_value(row, idxs, ['displayname'])
            usr = self._get_column_value(row, idxs, ['occuserid', 'userid'])
            epsg = self._get_column_value(row, idxs, ['occepsgcode', 'epsgcode'])
            qcount = self._get_column_value(row, idxs, ['querycount'])
            occ = OccurrenceLayer(name, usr, epsg, qcount,
                    squid=self._get_column_value(row, idxs, ['occsquid', 'squid']),
                    verify=self._get_column_value(row, idxs, ['occverify', 'verify']),
                    dlocation=self._get_column_value(row, idxs, ['occdlocation', 'dlocation']),
                    rawDLocation=self._get_column_value(row, idxs, ['rawdlocation']),
                    bbox=self._get_column_value(row, idxs, ['occbbox', 'bbox']),
                    occurrenceSetId=self._get_column_value(row, idxs, ['occurrencesetid']),
                    occMetadata=self._get_column_value(row, idxs, ['occmetadata', 'metadata']),
                    status=self._get_column_value(row, idxs, ['occstatus', 'status']),
                    status_mod_time=self._get_column_value(row, idxs, ['occstatusmodtime',
                                                                                'statusmodtime']))
        return occ

    # ................................
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
                        projMetadata=self._get_column_value(row, idxs, ['prjmetadata']),
                        status=self._get_column_value(row, idxs, ['prjstatus']),
                        status_mod_time=self._get_column_value(row, idxs, ['prjstatusmodtime']),
                        sdmProjectionId=self._get_column_value(row, idxs, ['sdmprojectid']))
        return prj

    # ................................
    def findOrInsertAlgorithm(self, alg, mod_time):
        """
        @summary Inserts an Algorithm into the database
        @param alg: The algorithm to add
        @return: new or existing Algorithm
        """
        if not mod_time:
            mod_time = gmt().mjd
        meta = alg.dumpAlgMetadata()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertAlgorithm',
                                                              alg.code, meta, mod_time)
        algo = self._createAlgorithm(row, idxs)
        return algo

    # ................................
    def findOrInsertTaxonSource(self, taxonSourceName, taxonSourceUrl):
        """
        @summary Finds or inserts a Taxonomy Source record into the database
        @param taxonSourceName: Name for Taxonomy Source
        @param taxonSourceUrl: URL for Taxonomy Source
        @return: record id for the new or existing Taxonomy Source 
        """
        taxSourceId = None
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertTaxonSource',
                                                              taxonSourceName, taxonSourceUrl,
                                                              gmt().mjd)
        if row is not None:
            taxSourceId = self._get_column_value(row, idxs, ['taxonomysourceid'])
        return taxSourceId

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_getLayer', lyrid, lyrverify,
                                                                lyruser, lyrname, epsgcode)
        lyr = self._createLayer(row, idxs)
        return lyr

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countLayers',
                                                userId, squid, afterTime, beforeTime, epsg)
        return self._getCount(row)

    # ................................
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

    # ................................
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
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertScenPackage',
                                    scenPkg.getUserId(), scenPkg.name, meta,
                                    scenPkg.mapUnits, scenPkg.epsgcode,
                                    scenPkg.getCSVExtentString(), wkt,
                                    scenPkg.mod_time)
        newOrExistingScenPkg = self._createScenPackage(row, idxs)
        return newOrExistingScenPkg

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countScenPackages', userId,
                                                                afterTime, beforeTime, epsg,
                                                                scenId)
        return self._getCount(row)

    # ................................
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
                objs.append(self._createScenPackage(r, idxs))
                # objs.append(self._createScenario(r, idxs))
        return objs

    # ................................
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
            scenPkgId = scenPkg.get_id()
            userId = scenPkg.getUserId()
            scenPkgName = scenPkg.name
        row, idxs = self.execute_select_one_function('lm_getScenPackage',
                                                                  scenPkgId, userId, scenPkgName)
        foundScenPkg = self._createScenPackage(row, idxs)
        if foundScenPkg:
            scens = self.getScenariosForScenPackage(foundScenPkg, None, None, None,
                                                                 fillLayers)
            foundScenPkg.setScenarios(scens)
        return foundScenPkg

    # ................................
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
            scenId = scen.get_id()
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

    # ................................
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
            scenPkgId = scenPkg.get_id()
            userId = scenPkg.getUserId()
            scenPkgName = scenPkg.name
        rows, idxs = self.executeSelectManyFunction('lm_getScenariosForScenPackage',
                                                                  scenPkgId, userId, scenPkgName)
        for r in rows:
            scen = self._createScenario(r, idxs, isForModel=False)
            if scen is not None and fillLayers:
                lyrs = self.getScenarioLayers(scen.get_id())
                scen.setLayers(lyrs)
            scens.append(scen)

        return scens

    # ................................
    def findOrInsertScenario(self, scen, scenPkgId):
        """
        @summary Inserts a scenario and any layers present into the database
        @param scen: The scenario to insert
        @return: new or existing Scenario
        """
        scen.mod_time = gmt().mjd
        wkt = None
        if scen.epsgcode == DEFAULT_EPSG:
            wkt = scen.getWkt()
        meta = scen.dumpScenMetadata()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertScenario',
                                    scen.getUserId(), scen.code, meta,
                                    scen.gcmCode, scen.altpredCode, scen.dateCode,
                                    scen.mapUnits, scen.resolution, scen.epsgcode,
                                    scen.getCSVExtentString(), wkt, scen.mod_time)
        newOrExistingScen = self._createScenario(row, idxs)
        if scenPkgId is not None:
            scenarioId = self._get_column_value(row, idxs, ['scenarioid'])
            joinId = self.executeModifyReturnValue(
                            'lm_joinScenPackageScenario', scenPkgId, scenarioId)
            if joinId < 0:
                raise LMError('Failed to join ScenPackage {} to Scenario {}'
                                  .format(scenPkgId, scenarioId))
        return newOrExistingScen

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countScenarios', userId,
                                                                afterTime, beforeTime, epsg,
                                                                gcmCode, altpredCode, dateCode,
                                                                scenPackageId)
        return self._getCount(row)

    # ................................
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

    # ................................
    def getEnvironmentalType(self, typeId, typecode, usrid):
        try:
            if typeId is not None:
                row, idxs = self.execute_select_one_function('lm_getLayerType', typeId)
            else:
                row, idxs = self.execute_select_one_function('lm_getLayerType',
                                                                        usrid, typecode)
        except:
            envType = None
        else:
            envType = self._createLayerType(row, idxs)
        return envType

    # ................................
    def findOrInsertEnvType(self, envtype):
        """
        @summary: Insert or find EnvType values.
        @param envtype: An EnvType or EnvLayer object
        @return: new or existing EnvironmentalType
        """
        currtime = gmt().mjd
        meta = envtype.dumpParamMetadata()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertEnvType',
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

    # ................................
    def findOrInsertLayer(self, lyr):
        """
        @summary: Find or insert a Layer into the database
        @param lyr: Raster or Vector layer to insert
        @return: new or existing Raster or Vector.
        """
        wkt = None
        if lyr.dataFormat in LMFormat.OGRDrivers() and lyr.epsgcode == DEFAULT_EPSG:
            wkt = lyr.getWkt()
        meta = lyr.dumpLyrMetadata()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertLayer',
                                    lyr.get_id(), lyr.getUserId(), lyr.squid, lyr.verify,
                                    lyr.name, lyr.getDLocation(), meta, lyr.dataFormat,
                                    lyr.gdalType, lyr.ogrType, lyr.valUnits,
                                    lyr.nodataVal, lyr.minVal, lyr.maxVal,
                                    lyr.epsgcode, lyr.mapUnits, lyr.resolution,
                                    lyr.getCSVExtentString(), wkt, lyr.mod_time)
        updatedLyr = self._createLayer(row, idxs)
        return updatedLyr

    # ................................
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
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertShapeGrid',
                                    shpgrd.get_id(), shpgrd.getUserId(),
                                    shpgrd.squid, shpgrd.verify, shpgrd.name,
                                    shpgrd.getDLocation(), meta,
                                    shpgrd.dataFormat, gdaltype, shpgrd.ogrType,
                                    valunits, nodataval, minval, maxval,
                                    shpgrd.epsgcode, shpgrd.mapUnits, shpgrd.resolution,
                                    shpgrd.getCSVExtentString(), wkt, shpgrd.mod_time,
                                    shpgrd.cellsides, shpgrd.cellsize, shpgrd.size,
                                    shpgrd.siteId, shpgrd.siteX, shpgrd.siteY,
                                    shpgrd.status, shpgrd.status_mod_time)
        updatedShpgrd = self._createShapeGrid(row, idxs)
        return updatedShpgrd

    # ................................
    def findOrInsertGridset(self, grdset):
        """
        @summary: Find or insert a Gridset into the database
        @param grdset: Gridset to insert
        @return: Updated new or existing Gridset.
        """
        meta = grdset.dumpGrdMetadata()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertGridset',
                                                                            grdset.get_id(),
                                                                            grdset.getUserId(),
                                                                            grdset.name,
                                                                            grdset.shapeGridId,
                                                                            grdset.getDLocation(),
                                                                            grdset.epsgcode,
                                                                            meta,
                                                                            grdset.mod_time)
        updatedGrdset = self._createGridset(row, idxs)
        # Populate dlocation in obj then db if this is a new Gridset
        if updatedGrdset._dlocation is None:
            updatedGrdset.getDLocation()
            success = self.updateGridset(updatedGrdset)
        return updatedGrdset

    # ................................
    def findUserGridsets(self, userid, obsolete_time=None):
        """
        @summary: Finds Gridset identifiers for user, optionally, filtered by cutoff date
        @param userid: User for gridsets to query
        @param obsolete_time: time before which objects are considered obsolete 
        @return: List of gridset ids for user
        """
        grdids = []
        rows, idxs = self.executeSelectManyFunction('lm_findUserGridsets', userid,
                                                    obsolete_time)
        for r in rows:
            if r[0] is not None:
                grdids.append(r[0])

        return grdids

    # ................................
    def deleteGridsetReturnFilenames(self, gridsetId):
        """
        @summary: Deletes Gridset, Matrices, and Makeflows
        @param gridsetId: Gridset for which to delete objects
        @return: List of filenames for all deleted objects
        """
        filenames = []
        rows, idxs = self.executeSelectAndModifyManyFunction('lm_deleteGridset', gridsetId)
        self.log.info('Returned {} files to be deleted for gridset {}'
                      .format(len(rows), gridsetId))
        for r in rows:
            if r[0] is not None:
                filenames.append(r[0])

        return filenames

    # ................................
    def deleteGridsetReturnMtxcolids(self, gridsetId):
        """
        @summary: Deletes SDM MatrixColumns (PAVs) for a Gridset
        @param gridsetId: Gridset for which to delete objects
        @return: List of ids for all deleted MatrixColumns
        """
        mtxcolids = []
        rows, idxs = self.executeSelectAndModifyManyFunction('lm_deleteGridsetMatrixColumns',
                                                    gridsetId)
        self.log.info('Returned {} matrixcolumn ids deleted from gridset {}'
                      .format(len(rows), gridsetId))

        for r in rows:
            if r[0] is not None:
                mtxcolids.append(r[0])

        return mtxcolids

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_getGridset', gridsetId,
                                                  userId, name)
        fullGset = self._createGridset(row, idxs)
        if fullGset is not None and fillMatrices:
            mtxs = self.getMatricesForGridset(fullGset.get_id(), None)
            for m in mtxs:
                # addMatrix sets userid
                fullGset.addMatrix(m)
        return fullGset

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countGridsets', userId,
                                    shpgrdLyrid, metamatch, afterTime, beforeTime, epsg)
        return self._getCount(row)

    # ................................
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

    # ................................
    def updateGridset(self, grdset):
        """
        @summary: Update a LmServer.legion.Gridset
        @param grdset: the LmServer.legion.Gridset object to update
        @return: Boolean success/failure
        """
        meta = grdset.dumpGrdMetadata()
        success = self.executeModifyFunction('lm_updateGridset',
                                             grdset.get_id(),
                                             grdset.treeId,
                                             grdset.getDLocation(),
                                             meta, grdset.mod_time)
        return success

    # ................................
    def getMatrix(self, mtxId, gridsetId, gridsetName, userId, mtxType,
                      gcmCode, altpredCode, dateCode, algCode):
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
        @param algCode: algorithm code of the LMMatrix
        @return: Existing LmServer.legion.lmmatrix.LMMatrix
        """
        row, idxs = self.execute_select_one_function('lm_getMatrix', mtxId,
                mtxType, gridsetId, gcmCode, altpredCode, dateCode, algCode,
                gridsetName, userId)
        fullMtx = self._createLMMatrix(row, idxs)
        return fullMtx

    # ................................
    def updateShapeGrid(self, shpgrd):
        """
        @summary: Update Shapegrid attributes: 
            verify, dlocation, metadata, mod_time, size, status, status_mod_time
        @param shpgrd: ShapeGrid to be updated.  
        @return: Updated record for successful update.
        """
        meta = shpgrd.dumpLyrMetadata()
        success = self.executeModifyFunction('lm_updateShapeGrid',
                                shpgrd.get_id(), shpgrd.verify, shpgrd.getDLocation(),
                                meta, shpgrd.mod_time, shpgrd.size,
                                shpgrd.status, shpgrd.status_mod_time)
        return success

    # ................................
    def getShapeGrid(self, lyrId, userId, lyrName, epsg):
        """
        @summary: Find or insert a ShapeGrid into the database
        @param shpgrdId: ShapeGrid database id
        @return: new or existing ShapeGrid.
        """
        row, idxs = self.execute_insert_and_select_one_function('lm_getShapeGrid',
                                                                    lyrId, userId, lyrName, epsg)
        shpgrid = self._createShapeGrid(row, idxs)
        return shpgrid

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countShapegrids', userId,
                                        cellsides, cellsize, afterTime, beforeTime, epsg)
        return self._getCount(row)

    # ................................
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

    # ................................
    def findOrInsertEnvLayer(self, lyr, scenarioId):
        """
        @summary Insert or find a layer's metadata in the Borg. 
        @param lyr: layer to insert
        @return: new or existing EnvironmentalLayer
        """
        lyr.mod_time = gmt().mjd
        wkt = None
        if lyr.epsgcode == DEFAULT_EPSG:
            wkt = lyr.getWkt()
        envmeta = lyr.dumpParamMetadata()
        lyrmeta = lyr.dumpLyrMetadata()
        row, idxs = self.execute_insert_and_select_one_function(
                                    'lm_findOrInsertEnvLayer', lyr.get_id(),
                                    lyr.getUserId(), lyr.squid, lyr.verify, lyr.name,
                                    lyr.getDLocation(),
                                    lyrmeta, lyr.dataFormat, lyr.gdalType, lyr.ogrType,
                                    lyr.valUnits, lyr.nodataVal, lyr.minVal, lyr.maxVal,
                                    lyr.epsgcode, lyr.mapUnits, lyr.resolution,
                                    lyr.getCSVExtentString(), wkt, lyr.mod_time,
                                    lyr.getParamId(), lyr.envCode, lyr.gcmCode,
                                    lyr.altpredCode, lyr.dateCode, envmeta,
                                    lyr.paramModTime)
        newOrExistingLyr = self._createEnvLayer(row, idxs)
        if scenarioId is not None:
            jrow, jidxs = self.execute_insert_and_select_one_function(
                        'lm_joinScenarioLayer', scenarioId,
                        newOrExistingLyr.getLayerId(), newOrExistingLyr.getParamId())
        return newOrExistingLyr

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_getEnvLayer', envlyrId,
                                            lyrid, lyrverify, lyruser, lyrname, epsgcode)
        lyr = self._createEnvLayer(row, idxs)
        return lyr

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countEnvLayers',
                                    userId, envCode, gcmcode, altpredCode, dateCode,
                                    afterTime, beforeTime, epsg, envTypeId, scenarioCode)
        return self._getCount(row)

    # ................................
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

    # ................................
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
                                                         envlyr.get_id())
        return success

    # ................................
    def findOrInsertUser(self, usr):
        """
        @summary: Insert a user of the Lifemapper system. 
        @param usr: LMUser object to insert
        @return: new or existing LMUser
        """
        usr.mod_time = gmt().mjd
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertUser',
                                        usr.userid, usr.firstName, usr.lastName,
                                        usr.institution, usr.address1, usr.address2,
                                        usr.address3, usr.phone, usr.email, usr.mod_time,
                                        usr.getPassword())
        newOrExistingUsr = self._createUser(row, idxs)
        if usr.userid != newOrExistingUsr.userid:
            self.log.info('Failed to add new user {}; matching email for user {}'
                              .format(usr.userid, newOrExistingUsr.userid))
        return newOrExistingUsr

    # ................................
    def updateUser(self, usr):
        """
        @summary: Insert a user of the Lifemapper system. 
        @param usr: LMUser object to update
        @return: updated LMUser
        """
        usr.mod_time = gmt().mjd
        success = self.executeModifyFunction('lm_updateUser',
                                        usr.userid, usr.firstName, usr.lastName,
                                        usr.institution, usr.address1, usr.address2,
                                        usr.address3, usr.phone, usr.email, usr.mod_time,
                                        usr.getPassword())
        return success

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_findUserForObject',
                            layerId, scenCode, occId, matrixId, gridsetId, mfprocessId)
        userId = row[0]
        return userId

    # ................................
    def findUser(self, usrid, email):
        """
        @summary: find a user with either a matching userId or email address
        @param usrid: the database primary key of the LMUser in the Borg
        @param email: the email address of the LMUser in the Borg
        @return: a LMUser object
        """
        row, idxs = self.execute_select_one_function('lm_findUser', usrid, email)
        usr = self._createUser(row, idxs)
        return usr

    # ................................
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

    # ................................
    def clearUser(self, userId):
        """
        @summary: Deletes all User data
        @param userId: User for whom to delete data
        @return: True/False for success of operation
        """
        success = False
        delCount = self.executeModifyReturnValue('lm_clearUserData', userId)
        self.log.info('Deleted {} data objects for user {}'
                          .format(delCount, userId))

        if delCount > 0:
            success = True
        return success

    # ................................
    def countJobChains(self, status, userId=None):
        """
        @summary: Return the number of jobchains fitting the given filter conditions
        @param status: include only jobs with this status
        @param userId: (optional) include only jobs with this userid
        @return: number of jobs fitting the given filter conditions
        """
        row, idxs = self.execute_select_one_function('lm_countJobChains',
                                                                userId, status)
        return self._getCount(row)

    # ................................
    def findTaxonSource(self, taxonSourceName):
        """
        @summary: Return the taxonomy source info given the name
        @param taxonSourceName: unique name of this taxonomy source
        @return: database id, url, and modification time of this source
        """
        txSourceId = url = moddate = None
        if taxonSourceName is not None:
            try:
                row, idxs = self.execute_select_one_function('lm_findTaxonSource',
                                                                        taxonSourceName)
            except Exception as e:
                if not isinstance(e, LMError):
                    e = LMError(e, line_num=self.get_line_num())
                raise e
            if row is not None:
                txSourceId = self._get_column_value(row, idxs, ['taxonomysourceid'])
                url = self._get_column_value(row, idxs, ['url'])
                moddate = self._get_column_value(row, idxs, ['modtime'])
        return txSourceId, url, moddate

    # ................................
    def getTaxonSource(self, tsId, tsName, tsUrl):
        """
        @summary: Return the taxonomy source info given the id, name or url
        @param tsId: database id of this taxonomy source
        @param tsName: unique name of this taxonomy source
        @param tsName: unique url of this taxonomy source
        @return: named tuple with database id, name, url, and 
                    modification time of this source
        """
        ts = None
        try:
            row, idxs = self.execute_select_one_function('lm_getTaxonSource',
                                                                    tsId, tsName, tsUrl)
        except Exception as e:
            if not isinstance(e, LMError):
                e = LMError(e, line_num=self.get_line_num())
            raise e
        if row is not None:
            fldnames = []
            for key, _ in sorted(iter(idxs.items()), key=lambda k_v: (k_v[1], k_v[0])):
                fldnames.append(key)
            TaxonSource = namedtuple('TaxonSource', fldnames)
            ts = TaxonSource(*row)
        return ts

    # ................................
    def findTaxon(self, taxonSourceId, taxonkey):
        try:
            row, idxs = self.execute_select_one_function('lm_findOrInsertTaxon',
                                taxonSourceId, taxonkey, None, None, None, None, None,
                                None, None, None, None, None, None, None, None, None,
                                None, None)
        except Exception as e:
            raise e
        sciname = self._createScientificName(row, idxs)
        return sciname

    # ................................
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
        currtime = gmt().mjd
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
            row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertTaxon',
                                                                taxonSourceId, taxonKey,
                                                                usr, squid, kingdom, phylum,
                                                                cls, ordr, family, genus, rank,
                                                                canname, sciname, genkey, spkey,
                                                                keyhierarchy, lastcount,
                                                                currtime)
        except Exception as e:
            raise e
        else:
            scientificname = self._createScientificName(row, idxs)

        return scientificname

    # ................................
    def updateTaxon(self, sciName):
        """
        @summary: Update a taxon in the database.  
        @param sciName: ScientificName object with taxonomy information for this taxon
        @return: updated ScientificName
        @note: Does not modify any foreign key (squid), or unique-constraint  
                 values, (taxonomySource, taxonKey, userId, sciname).
        """
        success = self.executeModifyFunction('lm_updateTaxon',
                                                         sciName.get_id(),
                                                         sciName.kingdom,
                                                         sciName.phylum,
                                                         sciName.txClass,
                                                         sciName.txOrder,
                                                         sciName.family,
                                                         sciName.genus,
                                                         sciName.rank,
                                                         sciName.canonicalName,
                                                         sciName.sourceGenusKey,
                                                         sciName.sourceSpeciesKey,
                                                         sciName.sourceKeyHierarchy,
                                                         sciName.lastOccurrenceCount,
                                                         gmt().mjd)
        return success

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_getTaxon', squid,
                                                taxonSourceId, taxonKey, userId, taxonName)
        scientificname = self._createScientificName(row, idxs)

        return scientificname

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_getScenario', scenid,
                                                                userId, code)
        scen = self._createScenario(row, idxs)
        if scen is not None and fillLayers:
            lyrs = self.getScenarioLayers(scen.get_id())
            scen.setLayers(lyrs)
        return scen

    # ................................
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

    # ................................
    def getOccurrenceSet(self, occId, squid, userId, epsg):
        """
        @summary: get an occurrenceset for the given id or squid and User
        @param occId: the database primary key of the Occurrence record
        @param squid: a species identifier, tied to a ScientificName
        @param userId: the database primary key of the LMUser
        @param epsg: Spatial reference system code from EPSG
        """
        row, idxs = self.execute_select_one_function('lm_getOccurrenceSet',
                                                                  occId, userId, squid, epsg)
        occ = self._createOccurrenceLayer(row, idxs)
        return occ

    # ................................
    def updateOccurrenceSet(self, occ):
        """
        @summary: Update OccurrenceLayer attributes: 
                     verify, displayName, dlocation, rawDlocation, queryCount, 
                     bbox, metadata, status, status_mod_time, geometries if valid
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
                                                             occ.get_id(),
                                                             occ.verify,
                                                             occ.displayName,
                                                             occ.getDLocation(),
                                                             occ.getRawDLocation(),
                                                             occ.queryCount,
                                                             occ.getCSVExtentString(),
                                                             occ.epsgcode,
                                                             metadata,
                                                             occ.status,
                                                             occ.status_mod_time,
                                                             polyWkt,
                                                             pointsWkt)
        except Exception as e:
            raise e
        return success

    # ................................
    def getSDMProject(self, layerid):
        """
        @summary: get a projection for the given id
        @param layerid: Database id for the SDMProject layer record
        """
        row, idxs = self.execute_select_one_function('lm_getSDMProjectLayer', layerid)
        proj = self._createSDMProjection(row, idxs)
        return proj

    # ................................
    def updateSDMProject(self, proj):
        """
        @summary Method to update an SDMProjection object in the database with 
                    the verify hash, metadata, data extent and values, status/status_mod_time.
        @param proj the SDMProjection object to update
        """
        success = False
        lyrmeta = proj.dumpLyrMetadata()
        prjmeta = proj.dumpParamMetadata()
        try:
            success = self.executeModifyFunction('lm_updateSDMProjectLayer',
                                                             proj.getParamId(),
                                                             proj.get_id(),
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
                                                             proj.mod_time,
                                                             prjmeta,
                                                             proj.status,
                                                             proj.status_mod_time)
        except Exception as e:
            raise e
        return success

    # ................................
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

        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertOccurrenceSet',
                                        occ.get_id(), occ.getUserId(), occ.squid,
                                        occ.verify, occ.displayName,
                                        occ.getDLocation(), occ.getRawDLocation(),
                                        pointtotal, occ.getCSVExtentString(), occ.epsgcode,
                                        occ.dumpLyrMetadata(),
                                        occ.status, occ.status_mod_time, polywkt, pointswkt)
        newOrExistingOcc = self._createOccurrenceLayer(row, idxs)
        return newOrExistingOcc

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countOccSets', userId, squid,
                                                                minOccurrenceCount, displayName,
                                                                afterTime, beforeTime, epsg,
                                                                afterStatus, beforeStatus,
                                                                gridsetId)
        return self._getCount(row)

    # ................................
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

    # ................................
    def summarizeOccurrenceSetsForGridset(self, gridsetid):
        """
        @summary: Count all OccurrenceSets for a gridset by status
        @param gridsetid: a database ID for the LmServer.legion.Gridset
        @return: a list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.executeSelectManyFunction('lm_summarizeOccSetsForGridset',
                                                    gridsetid, MatrixType.PAM,
                                                    MatrixType.ROLLING_PAM)
        for r in rows:
            status_total_pairs.append((r[idxs['status']], r[idxs['total']]))
        return status_total_pairs

    # ................................
    def deleteOccurrenceSet(self, occ):
        """
        @summary: Deletes OccurrenceSet and any dependent SDMProjects (with Layer).  
        @param occ: OccurrenceSet to delete
        @return: True/False for success of operation
        @note: If dependent SDMProject is input to a MatrixColumn of a 
                 Rolling (Global) PAM, the MatrixColumn will also be deleted.
        """
        pavDelcount = self._deleteOccsetDependentMatrixCols(occ.get_id(), occ.getUserId())
        success = self.executeModifyFunction('lm_deleteOccurrenceSet', occ.get_id())
        return success

    # ................................
    def deleteObsoleteSDMDataReturnIds(self, userid, beforetime, max_num):
        """
        @summary: Deletes OccurrenceSets, any dependent SDMProjects (with Layer)
                  and SDMProject-dependent MatrixColumns.  
        @param userid: User for whom to delete SDM data
        @param beforetime: delete SDM data modified before or at this time
        @param maxNum: limit on number of occsets to process
        @return: list of occurrenceset ids for deleted data.
        """
        occids = []
        time_str = LmTime.from_mjd(beforetime).strftime()
        rows, idxs = self.executeSelectAndModifyManyFunction(
            'lm_clearSomeObsoleteSpeciesDataForUser2', userid, beforetime, max_num)
        for r in rows:
            if r[0] is not None and r[0] != '':
                occids.append(r[0])

        self.log.info('''Deleted {} Occurrencesets older than {} and dependent 
        objects for User {}; returning occurrencesetids'''
        .format(len(rows), time_str, userid))
        return occids

    # ................................
    def deleteObsoleteSDMMtxcolsReturnIds(self, userid, beforetime, max_num):
        """
        @summary: Deletes SDMProject-dependent MatrixColumns for obsolete occurrencesets
        @param userid: User for whom to delete SDM data
        @param beforetime: delete SDM data modified before or at this time
        @param maxNum: limit on number of occsets to process
        @return: list of occurrenceset ids for deleted data.
        """
        mtxcolids = []
        time_str = LmTime.from_mjd(beforetime).strftime()
        rows, idxs = self.executeSelectAndModifyManyFunction(
            'lm_clearSomeObsoleteMtxcolsForUser', userid, beforetime, max_num)
        for r in rows:
            if r[0] is not None and r[0] != '':
                mtxcolids.append(r[0])

        self.log.info('''Deleted {} MatrixColumns for Occurrencesets older 
        than {}, returning matrixColumnIds'''
        .format(len(rows), time_str, userid, mtxcolids))
        return mtxcolids

    # ................................
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
                layerid = prj.get_id()
                pavs = self.listMatrixColumns(0, 500, usr, None, None, None, None,
                                                        None, None, None, None, layerid, False)
        if not returnProjs:
            prjs = []
        return prjs, pavs

    # ................................
    def findOrInsertSDMProject(self, proj):
        """
        @summary: Find existing (from projectID, layerid, OR usr/layername/epsg) 
                     OR save a new SDMProjection
        @param proj: the SDMProjection object to update
        @return new or existing SDMProjection 
        @note: assumes that pre- or post-processing layer inputs have already been 
                 inserted
        """
        lyrmeta = proj.dumpLyrMetadata()
        prjmeta = proj.dumpParamMetadata()
        algparams = proj.dumpAlgorithmParametersAsString()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertSDMProjectLayer',
                            proj.getParamId(), proj.get_id(), proj.getUserId(),
                            proj.squid, proj.verify, proj.name, proj.getDLocation(),
                            lyrmeta, proj.dataFormat, proj.gdalType,
                            proj.ogrType, proj.valUnits, proj.nodataVal, proj.minVal,
                            proj.maxVal, proj.epsgcode, proj.mapUnits, proj.resolution,
                            proj.getCSVExtentString(), proj.getWkt(), proj.mod_time,
                            proj.getOccurrenceSetId(), proj.algorithmCode, algparams,
                            proj.getModelScenarioId(),
                            proj.getProjScenarioId(), prjmeta,
                            proj.processType, proj.status, proj.status_mod_time)
        newOrExistingProj = self._createSDMProjection(row, idxs)
        return newOrExistingProj

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countSDMProjects',
                                    userId, squid, displayName, afterTime, beforeTime, epsg,
                                    afterStatus, beforeStatus, occsetId, algCode,
                                    mdlscenCode, prjscenCode, gridsetId)
        return self._getCount(row)

    # ................................
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

    # ................................
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
                newOrExistingLyr = self.findOrInsertLayer(mtxcol.layer)
                lyrid = newOrExistingLyr.getLayerId()

                # Shapegrid is already in db
                shpid = mtxcol.shapegrid.get_id()

        mcmeta = mtxcol.dumpParamMetadata()
        intparams = mtxcol.dumpIntersectParams()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertMatrixColumn',
                            mtxcol.getParamUserId(), mtxcol.getParamId(), mtxcol.parentId,
                            mtxcol.getMatrixIndex(), lyrid, mtxcol.squid, mtxcol.ident,
                            mcmeta, intparams,
                            mtxcol.status, mtxcol.status_mod_time)
        newOrExistingMtxCol = self._createMatrixColumn(row, idxs)
        # Put shapegrid into updated matrixColumn
        newOrExistingMtxCol.shapegrid = mtxcol.shapegrid
        newOrExistingMtxCol.processType = mtxcol.processType
        return newOrExistingMtxCol

    # ................................
    def updateMatrixColumn(self, mtxcol):
        """
        @summary: Update a MatrixColumn
        @param mtxcol: the LmServer.legion.MatrixColumn object to update
        @return: Boolean success/failure
        """
        meta = mtxcol.dumpParamMetadata()
        intparams = mtxcol.dumpIntersectParams()
        success = self.executeModifyFunction('lm_updateMatrixColumn',
                                                         mtxcol.get_id(),
                                                         mtxcol.getMatrixIndex(),
                                                         meta, intparams,
                                                         mtxcol.status, mtxcol.status_mod_time)
        return success

    # ................................
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
            row, idxs = self.execute_select_one_function('lm_getMatrixColumn',
                                                                    mtxcol.get_id(),
                                                                    mtxcol.parentId,
                                                                    mtxcol.getMatrixIndex(),
                                                                    mtxcol.getLayerId(),
                                                                    intparams)
        elif mtxcolId is not None:
            row, idxs = self.execute_select_one_function('lm_getMatrixColumn',
                                                            mtxcolId, None, None, None, None)
        mtxColumn = self._createMatrixColumn(row, idxs)
        return mtxColumn

    # ................................
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

    # ................................
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

    # ................................
    def summarizeSDMProjectsForGridset(self, gridsetid):
        """
        @summary: Count all SDMProjections for a gridset by status
        @param gridsetid: a database ID for the LmServer.legion.Gridset
        @return: a list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.executeSelectManyFunction('lm_summarizeSDMColumnsForGridset',
                                                    gridsetid, MatrixType.PAM,
                                                    MatrixType.ROLLING_PAM)
        for r in rows:
            status_total_pairs.append((r[idxs['status']], r[idxs['total']]))
        return status_total_pairs

    # ................................
    def summarizeMtxColumnsForGridset(self, gridsetid, mtx_type):
        """
        @summary: Count all MatrixColumns for a gridset by status
        @param gridsetid: a database ID for the LmServer.legion.Gridset
        @param mtx_type: optional filter for type of matrix to count
        @return: a list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.executeSelectManyFunction('lm_summarizeMtxColsForGridset',
                                                    gridsetid, mtx_type)
        for r in rows:
            status_total_pairs.append((r[idxs['status']], r[idxs['total']]))
        return status_total_pairs

    # ................................
    def summarizeMatricesForGridset(self, gridsetid, mtx_type):
        """
        @summary: Count all matrices for a gridset by status
        @param gridsetid: a database ID for the LmServer.legion.Gridset
        @param mtx_type: optional filter for type of matrix to count
        @return: a list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.executeSelectManyFunction('lm_summarizeMatricesForGridset',
                                                    gridsetid, mtx_type)
        for r in rows:
            status_total_pairs.append((r[idxs['status']], r[idxs['total']]))
        return status_total_pairs

    # ................................
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

    # ................................
    def countMatrixColumns(self, userId, squid, ident, afterTime, beforeTime,
                                  epsg, afterStatus, beforeStatus,
                                  gridsetId, matrixId, layerId):
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
        row, idxs = self.execute_select_one_function('lm_countMtxCols', userId,
                                        squid, ident, afterTime, beforeTime, epsg,
                                        afterStatus, beforeStatus,
                                        gridsetId, matrixId, layerId)
        return self._getCount(row)

    # ................................
    def listMatrixColumns(self, firstRecNum, maxNum, userId, squid, ident,
                                 afterTime, beforeTime, epsg, afterStatus, beforeStatus,
                                 gridsetId, matrixId, layerId, atom):
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
                            afterTime, beforeTime, epsg, afterStatus, beforeStatus,
                            gridsetId, matrixId, layerId)
            objs = self._getAtoms(rows, idxs, LMServiceType.MATRIX_COLUMNS)
        else:
            objs = []
            rows, idxs = self.executeSelectManyFunction('lm_listMtxColObjects',
                                firstRecNum, maxNum, userId, squid, ident,
                                afterTime, beforeTime, epsg, afterStatus, beforeStatus,
                                gridsetId, matrixId, layerId)
            for r in rows:
                objs.append(self._createMatrixColumn(r, idxs))
        return objs

    # ................................
    def updateMatrix(self, mtx):
        """
        @summary: Update a LMMatrix
        @param mtxcol: the LmServer.legion.LMMatrix object to update
        @return: Boolean success/failure
        @TODO: allow update of MatrixType, gcmCode, altpredCode, dateCode?
        """
        meta = mtx.dumpMtxMetadata()
        success = self.executeModifyFunction('lm_updateMatrix',
                                                         mtx.get_id(), mtx.getDLocation(),
                                                         meta, mtx.status, mtx.status_mod_time)
        return success

    # ................................
    def countMatrices(self, userId, matrixType, gcmCode, altpredCode, dateCode,
                      algCode, metastring, gridsetId, afterTime, beforeTime,
                      epsg, afterStatus, beforeStatus):
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
        row, idxs = self.execute_select_one_function('lm_countMatrices', userId,
                            matrixType, gcmCode, altpredCode, dateCode, algCode,
                            metamatch, gridsetId, afterTime, beforeTime, epsg,
                            afterStatus, beforeStatus)
        return self._getCount(row)

    # ................................
    def listMatrices(self, firstRecNum, maxNum, userId, matrixType, gcmCode,
                     altpredCode, dateCode, algCode, metastring, gridsetId,
                     afterTime, beforeTime, epsg, afterStatus, beforeStatus,
                     atom):
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
                                gcmCode, altpredCode, dateCode, algCode,
                                metamatch, gridsetId, afterTime, beforeTime,
                                epsg, afterStatus, beforeStatus)
            objs = self._getAtoms(rows, idxs, LMServiceType.MATRICES)
        else:
            objs = []
            rows, idxs = self.executeSelectManyFunction('lm_listMatrixObjects',
                                firstRecNum, maxNum, userId, matrixType,
                                gcmCode, altpredCode, dateCode, algCode,
                                metamatch, gridsetId, afterTime, beforeTime,
                                epsg, afterStatus, beforeStatus)
            for r in rows:
                objs.append(self._createLMMatrix(r, idxs))
        return objs

    # ................................
    def findOrInsertMatrix(self, mtx):
        """
        @summary: Find existing OR save a new Matrix
        @param mtx: the Matrix object to insert
        @return new or existing Matrix
        """
        meta = mtx.dumpMtxMetadata()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertMatrix',
                            mtx.get_id(), mtx.matrixType, mtx.parentId,
                            mtx.gcmCode, mtx.altpredCode, mtx.dateCode,
                            mtx.algorithmCode,
                            mtx.getDLocation(), meta, mtx.status,
                            mtx.status_mod_time)
        newOrExistingMtx = self._createLMMatrix(row, idxs)
        return newOrExistingMtx

    # ................................
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
        row, idxs = self.execute_select_one_function('lm_countTrees', userId,
                                            afterTime, beforeTime, name, metamatch,
                                            isBinary, isUltrametric, hasBranchLengths)
        return self._getCount(row)

    # ................................
    def listTrees(self, firstRecNum, maxNum, userId, afterTime, beforeTime,
                      name, metastring, isBinary, isUltrametric, hasBranchLengths,
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

    # ................................
    def findOrInsertTree(self, tree):
        """
        @summary: Find existing OR save a new Tree
        @param tree: the Tree object to insert
        @return new or existing Tree
        """
        meta = tree.dumpTreeMetadata()
        row, idxs = self.execute_insert_and_select_one_function('lm_findOrInsertTree',
                            tree.get_id(), tree.getUserId(), tree.name,
                            tree.getDLocation(), tree.isBinary(), tree.isUltrametric(),
                            tree.hasBranchLengths(), meta, tree.mod_time)
        newOrExistingTree = self._createTree(row, idxs)
        return newOrExistingTree

    # ................................
    def getTree(self, tree, treeId):
        """
        @summary: Retrieve a Tree from the database
        @param tree: Tree to retrieve
        @param treeId: Database ID of Tree to retrieve
        @return: Existing Tree
        """
        row = None
        if tree is not None:
            row, idxs = self.execute_select_one_function('lm_getTree', tree.get_id(),
                                                                    tree.getUserId(),
                                                                    tree.name)
        else:
            row, idxs = self.execute_select_one_function('lm_getTree', treeId,
                                                                    None, None)
        existingTree = self._createTree(row, idxs)
        return existingTree

    # ................................
    def insertMFChain(self, mfchain, gridsetId):
        """
        @summary: Inserts a MFChain into database
        @return: updated MFChain object
        """
        meta = mfchain.dumpMfMetadata()
        row, idxs = self.execute_insert_and_select_one_function('lm_insertMFChain',
                                                            mfchain.getUserId(),
                                                            gridsetId,
                                                            mfchain.getDLocation(),
                                                            mfchain.priority,
                                                            meta, mfchain.status,
                                                            mfchain.status_mod_time)
        mfchain = self._createMFChain(row, idxs)
        return mfchain

    # ................................
    def countMFChains(self, userId, gridsetId, metastring, afterStat, beforeStat,
                      afterTime, beforeTime):
        """
        @summary: Return the number of MFChains fitting the given filter conditions
        @param userId: filter by LMUser 
        @param gridsetId: filter by a Gridset
        @param metastring: find gridsets containing this word in the metadata
        @param afterStat: filter by status >= to this value
        @param beforeStat: filter by status <= to this value
        @param afterTime: filter by modified at or after this time
        @param beforeTime: filter by modified at or before this time
        @return: count of MFChains fitting the given filter conditions
        """
        metamatch = None
        if metastring is not None:
            metamatch = '%{}%'.format(metastring)
        row, idxs = self.execute_select_one_function('lm_countMFProcess', userId,
                                                  gridsetId, metamatch,
                                                  afterStat, beforeStat,
                                                  afterTime, beforeTime)
        return self._getCount(row)

    # ................................
    def countPriorityMFChains(self, gridsetId):
        """
        @summary: Return the number of MFChains to be run before those for the 
                  specified gridset
        @param gridsetId: Gridset id for which to check higher priority mfchains
        @return: count of MFChains with higher priority or 
                 same priority and earlier timestamp
        """
        row, idxs = self.execute_select_one_function('lm_countMFProcessAhead',
                                                  gridsetId, JobStatus.COMPLETE)
        return self._getCount(row)

    # ................................
    def listMFChains(self, firstRecNum, maxNum, userId, gridsetId, metastring,
                     afterStat, beforeStat, afterTime, beforeTime, atom):
        """
        @summary: Return MFChain Objects or Atoms matching filter conditions 
        @param firstRecNum: The first record to return, 0 is the first record
        @param maxNum: Maximum number of records to return
        @param userId: User (owner) for which to return shapegrids.  
        @param gridsetId: filter by a Gridset
        @param metastring: find gridsets containing this word in the metadata
        @param afterStat: filter by status >= to this value
        @param beforeStat: filter by status <= to this value
        @param afterTime: filter by modified at or after this time
        @param beforeTime: filter by modified at or before this time
        @param atom: True if return objects will be Atoms, False if full objects
        @return: a list of MFProcess/Gridset atoms or full objects
        """
        if atom:
            rows, idxs = self.executeSelectManyFunction('lm_listMFProcessAtoms',
                            firstRecNum, maxNum, userId, gridsetId, metastring,
                            afterStat, beforeStat, afterTime, beforeTime)
            objs = self._getAtoms(rows, idxs, None)
        else:
            objs = []
            rows, idxs = self.executeSelectManyFunction('lm_listMFProcessObjects',
                            firstRecNum, maxNum, userId, gridsetId, metastring,
                            afterStat, beforeStat, afterTime, beforeTime)
            for r in rows:
                objs.append(self._createMFChain(r, idxs))
        return objs

    # ................................
    def summarizeMFChainsForGridset(self, gridsetid):
        """
        @summary: Count all mfprocesses for a gridset by status
        @param gridsetid: a database ID for the LmServer.legion.Gridset
        @return: a list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.executeSelectManyFunction('lm_summarizeMFProcessForGridset',
                                                    gridsetid)
        for r in rows:
            status_total_pairs.append((r[idxs['status']], r[idxs['total']]))
        return status_total_pairs

    # ................................
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
        mod_time = gmt().mjd
        rows, idxs = self.executeSelectManyFunction('lm_findMFChains', count,
                                                    userId, oldStatus, newStatus,
                                                    mod_time)
        for r in rows:
            mfchain = self._createMFChain(r, idxs)
            mfchainList.append(mfchain)
        return mfchainList

    # ................................
    def deleteMFChainsReturnFilenames(self, gridsetid):
        """
        @summary: Deletes MFChains for a gridset, returns filenames 
        @param gridsetid: Pull only MFChains for this gridset
        @return: list of MFChains filenames
        """
        flist = []
        rows, idxs = self.executeSelectAndModifyManyFunction(
            'lm_deleteMFChainsForGridsetReturnFilenames', gridsetid)
        for r in rows:
            fname = r[0]
            flist.append(fname)
        return flist

    # ................................
    def getMFChain(self, mfprocessid):
        """
        @summary: Retrieves MFChain from database
        @param mfprocessid: Database ID of MFChain to pull
        @return: LmServer.legion.processchain.MFChains
        """
        row, idxs = self.executeSelectManyFunction('lm_getMFChain', mfprocessid)
        mfchain = self._createMFChain(row, idxs)
        return mfchain

    # ................................
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
        # TODO: Handle if MatrixColumn changes to inherit from LMMatrix
        elif isinstance(obj, MatrixColumn):
            success = self.updateMatrixColumn(obj)
        elif isinstance(obj, LMMatrix):
            success = self.updateMatrix(obj)
        elif isinstance(obj, ScientificName):
            success = self.updateTaxon(obj)
        elif isinstance(obj, Tree):
            meta = obj.dumpTreeMetadata()
            success = self.executeModifyFunction('lm_updateTree', obj.get_id(),
                                                             obj.getDLocation(),
                                                             obj.isBinary(),
                                                             obj.isUltrametric(),
                                                             obj.hasBranchLengths(),
                                                             meta, obj.mod_time)
        elif isinstance(obj, MFChain):
            success = self.executeModifyFunction('lm_updateMFChain', obj.objId,
                                                             obj.getDLocation(),
                                                             obj.status, obj.status_mod_time)
        elif isinstance(obj, Gridset):
            success = self.updateGridset(obj)
        else:
            raise LMError('Unsupported update for object {}'.format(type(obj)))
        return success

    # ................................
    def deleteObject(self, obj):
        """
        @summary: Deletes object from database
        @return: True/False for success of operation
        @note: OccurrenceSet delete cascades to SDMProject but not MatrixColumn
        @note: MatrixColumns for Global PAM should be deleted or reset on 
                 OccurrenceSet delete or recalc 
        """
        try:
            objid = obj.get_id()
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
                                                                 lyr.get_id(), objid)
            success = self.executeModifyFunction('lm_deleteScenario', objid)
        elif isinstance(obj, MatrixColumn):
            success = self.executeModifyFunction('lm_deleteMatrixColumn', objid)
        elif isinstance(obj, Tree):
            success = self.executeModifyFunction('lm_deleteTree', objid)
        else:
            raise LMError('Unsupported delete for object {}'.format(type(obj)))
        return success

    # ................................
    def getMatricesForGridset(self, gridsetid, mtxType):
        """
        @summary Return all LmServer.legion.LMMatrix objects that are part of a 
                    gridset
        @param gridsetid: Id of the gridset organizing these data matrices
        @param mtxType: optional filter, LmCommon.common.lmconstants.MatrixType 
               for one type of LMMatrix
        """
        mtxs = []
        rows, idxs = self.executeSelectManyFunction('lm_getMatricesForGridset',
                                                    gridsetid, mtxType)
        for r in rows:
            mtxs.append(self._createLMMatrix(r, idxs))
        return mtxs

