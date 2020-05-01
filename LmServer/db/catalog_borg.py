"""Module containing lower level functions for accessing database
"""
from collections import namedtuple

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import MatrixType, LMFormat, JobStatus
from LmCommon.common.time import gmt, LmTime
from LmServer.base.dbpgsql import DbPostgresql
from LmServer.base.layer import Raster, Vector
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import DB_STORE, LM_SCHEMA_BORG, LMServiceType
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import DEFAULT_EPSG
from LmServer.legion.algorithm import Algorithm
from LmServer.legion.env_layer import EnvType, EnvLayer
from LmServer.legion.gridset import Gridset
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.occ_layer import OccurrenceLayer
from LmServer.legion.process_chain import MFChain
from LmServer.legion.scenario import Scenario, ScenPackage
from LmServer.legion.sdm_proj import SDMProjection
from LmServer.legion.shapegrid import Shapegrid
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
        """Created only from an algorithm, model, lm_fullModel, or lm_fullProjection
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
    def _create_scen_package(self, row, idxs):
        """Create a scenario package from a database row
        """
        scen = None
        pkg_id = self._get_column_value(row, idxs, ['scenpackageid'])
        usr = self._get_column_value(row, idxs, ['userid'])
        name = self._get_column_value(row, idxs, ['pkgname', 'name'])
        meta = self._get_column_value(row, idxs, ['pkgmetadata', 'metadata'])
        epsg = self._get_column_value(row, idxs, ['pkgepsgcode', 'epsgcode'])
        bbox = self._get_column_value(row, idxs, ['pkgbbox', 'bbox'])
        units = self._get_column_value(row, idxs, ['pkgunits', 'units'])
        mod_time = self._get_column_value(row, idxs, ['pkgmodtime', 'modtime'])

        if row is not None:
            scen = ScenPackage(
                name, usr, metadata=meta, epsg_code=epsg, bbox=bbox,
                map_units=units, mod_time=mod_time, scen_package_id=pkg_id)
        return scen

    # ................................
    def _create_env_type(self, row, idxs):
        """Create an _EnvironmentalType from a database record
        """
        lyr_type = None
        if row is not None:
            env_code = self._get_column_value(row, idxs, ['envcode'])
            gcm_code = self._get_column_value(row, idxs, ['gcmcode'])
            alt_code = self._get_column_value(row, idxs, ['altpredcode'])
            dt_code = self._get_column_value(row, idxs, ['datecode'])
            meta = self._get_column_value(
                row, idxs, ['envmetadata', 'metadata'])
            mod_time = self._get_column_value(
                row, idxs, ['envmodtime', 'modtime'])
            usr = self._get_column_value(row, idxs, ['envuserid', 'userid'])
            lt_id = self._get_column_value(row, idxs, ['envtypeid'])
            lyr_type = EnvType(
                env_code, usr, gcm_code=gcm_code, alt_pred_code=alt_code,
                date_code=dt_code, metadata=meta, mod_time=mod_time,
                env_type_id=lt_id)
        return lyr_type

    # ................................
    def _create_gridset(self, row, idxs):
        """Create a Gridset from a database Gridset record or lm_gridset view

        Note:
            This does not return tree object data, only tree_id
        """
        gridset = None
        if row is not None:
            shp = self._create_shapegrid(row, idxs)
            # TODO: return lm_tree instead of lm_gridset (with just tree_id)
            tree = self._create_tree(row, idxs)
            shp_id = self._get_column_value(row, idxs, ['layerid'])
            grid_id = self._get_column_value(row, idxs, ['gridsetid'])
            usr = self._get_column_value(row, idxs, ['userid'])
            name = self._get_column_value(row, idxs, ['grdname', 'name'])
            dloc = self._get_column_value(
                row, idxs, ['grddlocation', 'dlocation'])
            epsg = self._get_column_value(
                row, idxs, ['grdepsgcode', 'epsgcode'])
            meta = self._get_column_value(
                row, idxs, ['grdmetadata', 'metadata'])
            mtime = self._get_column_value(
                row, idxs, ['grdmodtime', 'modtime'])
            gridset = Gridset(
                name=name, metadata=meta, shapegrid=shp, shapegrid_id=shp_id,
                tree=tree, dlocation=dloc, epsg_code=epsg, user_id=usr,
                gridset_id=grid_id, mod_time=mtime)
        return gridset

    # ................................
    def _create_tree(self, row, idxs):
        """Create a Tree from a database Tree record

        Todo:
            Do we want to use binary attributes without reading data?
        """
        tree = None
        if row is not None:
            tree_id = self._get_column_value(row, idxs, ['treeid'])
            if tree_id is not None:
                usr = self._get_column_value(
                    row, idxs, ['treeuserid', 'userid'])
                name = self._get_column_value(row, idxs, ['treename', 'name'])
                dloc = self._get_column_value(
                    row, idxs, ['treedlocation', 'dlocation'])
                # is_bin = self._get_column_value(row, idxs, ['isbinary'])
                # is_ultra = self._get_column_value(
                #    row, idxs, ['isultrametric'])
                # has_len = self._get_column_value(
                #     row, idxs, ['hasbranchlengths'])
                meta = self._get_column_value(
                    row, idxs, ['treemetadata', 'metadata'])
                mod_time = self._get_column_value(
                    row, idxs, ['treemodtime', 'modtime'])
                tree = Tree(
                    name, metadata=meta, dlocation=dloc, user_id=usr,
                    tree_id=tree_id, mod_time=mod_time)
        return tree

    # ................................
    def _create_lm_matrix(self, row, idxs):
        """Create an LMMatrix object from a database record
        """
        mtx = None
        if row is not None:
            gridset = self._create_gridset(row, idxs)
            mtx_id = self._get_column_value(row, idxs, ['matrixid'])
            mtype = self._get_column_value(row, idxs, ['matrixtype'])
            scen_id = self._get_column_value(row, idxs, ['scenarioid'])
            # TODO: replace 3 Codes with scenario_id
            gcm = self._get_column_value(row, idxs, ['gcmcode'])
            rcp = self._get_column_value(row, idxs, ['altpredcode'])
            date_code = self._get_column_value(row, idxs, ['datecode'])
            alg = self._get_column_value(row, idxs, ['algorithmcode'])
            dloc = self._get_column_value(row, idxs, ['matrixiddlocation'])
            meta = self._get_column_value(
                row, idxs, ['mtxmetadata', 'metadata'])
            usr = self._get_column_value(row, idxs, ['userid'])
            stat = self._get_column_value(row, idxs, ['mtxstatus', 'status'])
            stat_time = self._get_column_value(
                row, idxs, ['mtxstatusmodtime', 'statusmodtime'])
            mtx = LMMatrix(
                None, matrix_type=mtype, scenario_id=scen_id, gcm_code=gcm,
                alt_pred_code=rcp, date_code=date_code, alg_code=alg,
                metadata=meta, dlocation=dloc, user_id=usr, gridset=gridset,
                matrix_id=mtx_id, status=stat, status_mod_time=stat_time)
        return mtx

    # ................................
    def _create_matrix_column(self, row, idxs):
        """Create an MatrixColumn from a lm_matrixcolumn view
        """
        mtx_col = None
        if row is not None:
            # Returned by only some functions
            input_lyr = self._create_layer(row, idxs)
            # Ids of joined input layers
            lyr_id = self._get_column_value(row, idxs, ['layerid'])
            shapegrid_id = self._get_column_value(row, idxs, ['shplayerid'])
            mtx_col_id = self._get_column_value(row, idxs, ['matrixcolumnid'])
            mtx_id = self._get_column_value(row, idxs, ['matrixid'])
            mtx_index = self._get_column_value(row, idxs, ['matrixindex'])
            squid = self._get_column_value(row, idxs, ['mtxcolsquid', 'squid'])
            ident = self._get_column_value(row, idxs, ['mtxcolident', 'ident'])
            mtx_col_meta = self._get_column_value(
                row, idxs, ['mtxcolmetatadata'])
            int_params = self._get_column_value(row, idxs, ['intersectparams'])
            mtx_col_stat = self._get_column_value(row, idxs, ['mtxcolstatus'])
            mtx_col_stat_time = self._get_column_value(
                row, idxs, ['mtxcolstatusmodtime'])
            usr = self._get_column_value(row, idxs, ['userid'])

            mtx_col = MatrixColumn(
                mtx_index, mtx_id, usr, layer=input_lyr, layer_id=lyr_id,
                # shapegrid_id=shapegrid_id,
                intersect_params=int_params,
                squid=squid, ident=ident, process_type=None,
                metadata=mtx_col_meta, matrix_column_id=mtx_col_id,
                status=mtx_col_stat, status_mod_time=mtx_col_stat_time)
        return mtx_col

    # ................................
    def _get_layer_inputs(self, row, idxs):
        """Create Raster or Vector layer from a Layer or view in the Borg.

        Note:
            - OccurrenceSet and SDMProject objects do not use this function
            - used with Layer, lm_envlayer, lm_scenlayer, lm_sdmproject,
                lm_shapegrid
        """
        db_id = self._get_column_value(row, idxs, ['layerid'])
        usr = self._get_column_value(row, idxs, ['lyruserid', 'userid'])
        verify = self._get_column_value(row, idxs, ['lyrverify', 'verify'])
        squid = self._get_column_value(row, idxs, ['lyrsquid', 'squid'])
        name = self._get_column_value(row, idxs, ['lyrname', 'name'])
        dloc = self._get_column_value(row, idxs, ['lyrdlocation', 'dlocation'])
        meta = self._get_column_value(row, idxs, ['lyrmetadata', 'metadata'])
        v_type = self._get_column_value(row, idxs, ['ogrtype'])
        r_type = self._get_column_value(row, idxs, ['gdaltype'])
        v_units = self._get_column_value(row, idxs, ['valunits'])
        v_attr = self._get_column_value(row, idxs, ['valattribute'])
        nodata = self._get_column_value(row, idxs, ['nodataval'])
        min_val = self._get_column_value(row, idxs, ['minval'])
        max_val = self._get_column_value(row, idxs, ['maxval'])
        f_format = self._get_column_value(row, idxs, ['dataformat'])
        epsg = self._get_column_value(row, idxs, ['epsgcode'])
        m_units = self._get_column_value(row, idxs, ['mapunits'])
        res = self._get_column_value(row, idxs, ['resolution'])
        dt_mod = self._get_column_value(row, idxs, ['lyrmodtime', 'modtime'])
        bbox = self._get_column_value(row, idxs, ['bbox'])
        return (
            db_id, usr, verify, squid, name, dloc, meta, v_type, r_type,
            v_units, v_attr, nodata, min_val, max_val, f_format, epsg, m_units,
            res, dt_mod, bbox)

    # ................................
    def _create_layer(self, row, idxs):
        """Create Raster or Vector layer from a Layer or view in the Borg.

        Note:
            - OccurrenceSet and SDMProject objects do not use this function
            - Used with Layer, lm_envlayer, lm_scenlayer, lm_sdmproject,
                lm_shapegrid
        """
        lyr = None
        if row is not None:
            db_id = self._get_column_value(row, idxs, ['layerid'])
            name = self._get_column_value(row, idxs, ['lyrname', 'name'])
            usr = self._get_column_value(row, idxs, ['lyruserid', 'userid'])
            epsg = self._get_column_value(row, idxs, ['epsgcode'])
            # Layer may be optional
            if all([db_id, name, usr, epsg]):
                verify = self._get_column_value(
                    row, idxs, ['lyrverify', 'verify'])
                squid = self._get_column_value(
                    row, idxs, ['lyrsquid', 'squid'])
                dloc = self._get_column_value(
                    row, idxs, ['lyrdlocation', 'dlocation'])
                meta = self._get_column_value(
                    row, idxs, ['lyrmetadata', 'metadata'])
                v_type = self._get_column_value(row, idxs, ['ogrtype'])
                r_type = self._get_column_value(row, idxs, ['gdaltype'])
                v_units = self._get_column_value(row, idxs, ['valunits'])
                v_attr = self._get_column_value(row, idxs, ['valattribute'])
                nodata = self._get_column_value(row, idxs, ['nodataval'])
                min_val = self._get_column_value(row, idxs, ['minval'])
                max_val = self._get_column_value(row, idxs, ['maxval'])
                f_format = self._get_column_value(row, idxs, ['dataformat'])
                m_units = self._get_column_value(row, idxs, ['mapunits'])
                res = self._get_column_value(row, idxs, ['resolution'])
                dt_mod = self._get_column_value(
                    row, idxs, ['lyrmodtime', 'modtime'])
                bbox = self._get_column_value(row, idxs, ['lyrbbox', 'bbox'])

                if f_format in LMFormat.ogr_drivers():
                    lyr = Vector(
                        name, usr, epsg, lyr_id=db_id, squid=squid,
                        verify=verify, dlocation=dloc, metadata=meta,
                        data_format=f_format, ogr_type=v_type,
                        val_units=v_units, val_attribute=v_attr,
                        nodata_val=nodata, min_val=min_val, max_val=max_val,
                        map_units=m_units, resolution=res, bbox=bbox,
                        mod_time=dt_mod)
                elif f_format in LMFormat.gdal_drivers():
                    lyr = Raster(
                        name, usr, epsg, lyr_id=db_id, squid=squid,
                        verify=verify, dlocation=dloc, metadata=meta,
                        data_format=f_format, gdal_type=r_type,
                        val_units=v_units, nodata_val=nodata, min_val=min_val,
                        max_val=max_val, map_units=m_units, resolution=res,
                        bbox=bbox, mod_time=dt_mod)
        return lyr

    # ................................
    def _create_env_layer(self, row, idxs):
        """Create an EnvLayer object from a database row
        """
        env_rst = None
        env_layer_id = self._get_column_value(row, idxs, ['envlayerid'])
        if row is not None:
            # scen_id = self._get_column_value(row, idxs, ['scenarioid'])
            scen_code = self._get_column_value(row, idxs, ['scenariocode'])
            rst = self._create_layer(row, idxs)
            if rst is not None:
                e_type = self._create_env_type(row, idxs)
                env_rst = EnvLayer.init_from_parts(
                    rst, e_type, env_layer_id=env_layer_id,
                    scen_code=scen_code)
        return env_rst

    # ................................
    def _create_shapegrid(self, row, idxs):
        """Create a shapegrid from a database record
        """
        shg = None
        if row is not None:
            lyr = self._create_layer(row, idxs)
            # Shapegrid may be optional
            if lyr is not None:
                shg = Shapegrid.init_from_parts(
                    lyr, self._get_column_value(row, idxs, ['cellsides']),
                    self._get_column_value(row, idxs, ['cellsize']),
                    site_id=self._get_column_value(row, idxs, ['idattribute']),
                    site_x=self._get_column_value(row, idxs, ['xattribute']),
                    site_y=self._get_column_value(row, idxs, ['yattribute']),
                    size=self._get_column_value(row, idxs, ['vsize']),
                    # todo: will these ever be accessed without 'shpgrd'
                    #    prefix?
                    status=self._get_column_value(
                        row, idxs, ['shpgrdstatus', 'status']),
                    status_mod_time=self._get_column_value(
                        row, idxs, ['shpgrdstatusmodtime', 'statusmodtime']))
        return shg

    # ................................
    def _create_occurrence_layer(self, row, idxs):
        """Create an OccurrenceLayer from a database row
        """
        occ = None
        if row is not None:
            name = self._get_column_value(row, idxs, ['displayname'])
            usr = self._get_column_value(row, idxs, ['occuserid', 'userid'])
            epsg = self._get_column_value(
                row, idxs, ['occepsgcode', 'epsgcode'])
            q_count = self._get_column_value(row, idxs, ['querycount'])
            occ = OccurrenceLayer(
                name, usr, epsg, q_count,
                squid=self._get_column_value(row, idxs, ['occsquid', 'squid']),
                verify=self._get_column_value(
                    row, idxs, ['occverify', 'verify']),
                dlocation=self._get_column_value(
                    row, idxs, ['occdlocation', 'dlocation']),
                raw_dlocation=self._get_column_value(
                    row, idxs, ['rawdlocation']),
                bbox=self._get_column_value(row, idxs, ['occbbox', 'bbox']),
                occ_layer_id=self._get_column_value(
                    row, idxs, ['occurrencesetid']),
                occ_metadata=self._get_column_value(
                    row, idxs, ['occmetadata', 'metadata']),
                status=self._get_column_value(
                    row, idxs, ['occstatus', 'status']),
                status_mod_time=self._get_column_value(
                    row, idxs, ['occstatusmodtime', 'statusmodtime']))
        return occ

    # ................................
    def _create_sdm_projection(self, row, idxs, layer=None):
        """Create an SDMProjection object from a database row."""
        prj = None
        if row is not None:
            occ = self._create_occurrence_layer(row, idxs)
            alg = self._create_algorithm(row, idxs)
            mdl_scen = self._create_scenario(row, idxs, is_for_model=True)
            prj_scen = self._create_scenario(row, idxs, is_for_model=False)
            if layer is None:
                layer = self._create_layer(row, idxs)
            prj = SDMProjection.init_from_parts(
                occ, alg, mdl_scen, prj_scen, layer,
                proj_metadata=self._get_column_value(
                    row, idxs, ['prjmetadata']),
                status=self._get_column_value(row, idxs, ['prjstatus']),
                status_mod_time=self._get_column_value(
                    row, idxs, ['prjstatusmodtime']),
                sdm_proj_id=self._get_column_value(
                    row, idxs, ['sdmprojectid']))
        return prj

    # ................................
    def find_or_insert_algorithm(self, alg, mod_time):
        """Inserts an Algorithm into the database

        Args:
            alg: The algorithm to add
            mod_time: Modification time of this algorithm

        Returns:
            New or existing Algorithm
        """
        if not mod_time:
            mod_time = gmt().mjd
        meta = alg.dump_algorithm_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertAlgorithm', alg.code, meta, mod_time)
        algo = self._create_algorithm(row, idxs)
        return algo

    # ................................
    def find_or_insert_taxon_source(self, taxon_source_name, taxon_source_url):
        """Finds or inserts a Taxonomy Source record into the database

        Args:
            taxon_source_name: Name for Taxonomy Source
            taxon_source_url: URL for Taxonomy Source

        Returns:
            int - Record id for the new or existing taxonomy source
        """
        tax_source_id = None
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertTaxonSource', taxon_source_name, taxon_source_url,
            gmt().mjd)
        if row is not None:
            tax_source_id = self._get_column_value(
                row, idxs, ['taxonomysourceid'])
        return tax_source_id

    # ................................
    def get_base_layer(self, lyr_id, lyr_verify, lyr_user, lyr_name,
                       epsg_code):
        """Get and fill a layer

        Args:
            lyr_id: Layer database id
            lyr_verify: SHASUM hash of layer data
            lyr_user: Layer user id
            lyr_name: Layer name
            lyr_id: Layer EPSG code

        Returns:
            _Layer base object
        """
        row, idxs = self.execute_select_one_function(
            'lm_getLayer', lyr_id, lyr_verify, lyr_user, lyr_name, epsg_code)
        lyr = self._create_layer(row, idxs)
        return lyr

    # ................................
    def count_layers(self, user_id, squid, after_time, before_time, epsg):
        """Count all Layers matching the filter conditions

        Args:
            user_id: User (owner) for which to return occurrencesets.
            squid: a species identifier, tied to a ScientificName
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code

        Returns:
            int - A count of occurrence sets
        """
        row, _ = self.execute_select_one_function(
            'lm_countLayers', user_id, squid, after_time, before_time, epsg)
        return self._get_count(row)

    # ................................
    def list_layers(self, first_rec_num, max_num, user_id, squid, after_time,
                    before_time, epsg, atom):
        """Return Layer Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return occurrencesets.
            squid: a species identifier, tied to a ScientificName
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            list of Atom objects for matching layers
        """
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listLayerAtoms', first_rec_num, max_num, user_id, squid,
                after_time, before_time, epsg)
            objs = self._get_atoms(rows, idxs, LMServiceType.LAYERS)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listLayerObjects', first_rec_num, max_num, user_id, squid,
                after_time, before_time, epsg)
            for row in rows:
                objs.append(self._create_layer(row, idxs))
        return objs

    # ................................
    def find_or_insert_scen_package(self, scen_pkg):
        """Inserts a ScenPackage into the database

        Args:
            scen_pkg: The ScenPackage to insert

        Returns:
            new or existing ScenPackage

        Note:
            - This returns the updated ScenPackage
            - This Borg function inserts only the ScenPackage; the calling
                Scribe method also adds and joins Scenarios present
        """
        wkt = None
        if scen_pkg.epsg_code == DEFAULT_EPSG:
            wkt = scen_pkg.get_wkt()
        meta = scen_pkg.dump_scenpkg_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertScenPackage', scen_pkg.get_user_id(),
            scen_pkg.name, meta, scen_pkg.map_units, scen_pkg.epsg_code,
            scen_pkg.get_csv_extent_string(), wkt, scen_pkg.mod_time)
        new_or_existing_scen_pkg = self._create_scen_package(row, idxs)
        return new_or_existing_scen_pkg

    # ................................
    def count_scen_packages(self, user_id, after_time, before_time, epsg,
                            scen_id):
        """Return the number of ScenarioPackages matching the criteria

        Args:
            user_id: filter by LMUser
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by the EPSG spatial reference system code
            scen_id: filter by a Scenario

        Returns:
            int - The number of scenario packages matching the specified
                criteria
        """
        row, _ = self.execute_select_one_function(
            'lm_countScenPackages', user_id, after_time, before_time, epsg,
            scen_id)
        return self._get_count(row)

    # ................................
    def list_scen_packages(self, first_rec_num, max_num, user_id, after_time,
                           before_time, epsg, scen_id, atom):
        """Return ScenPackage Objects or Atoms fitting the given filters

        Args:
            first_rec_num: start at this record
            max_num: maximum number of records to return
            user_id: filter by LMUser
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by the EPSG spatial reference system code
            scen_id: filter by a Scenario
            atom: True if return objects will be Atoms, False if full objects

        Note:
            - Returned ScenPackage Objects contain Scenario objects, not filled
                with layers.
        """
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listScenPackageAtoms', first_rec_num, max_num, user_id,
                after_time, before_time, epsg, scen_id)
            objs = self._get_atoms(rows, idxs, LMServiceType.SCEN_PACKAGES)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listScenPackageObjects', first_rec_num, max_num, user_id,
                after_time, before_time, epsg, scen_id)
            for row in rows:
                objs.append(self._create_scen_package(row, idxs))
        return objs

    # ................................
    def get_scen_package(self, scen_pkg, scen_pkg_id, user_id, scen_pkg_name,
                         fill_layers):
        """Find all ScenPackages that contain the given Scenario

        Args:
            scen_pkg: The The LmServer.legion.scenario.ScenPackage to find
            scen_pkg_id: The database Id for the ScenPackage
            user_id: The user_id for the ScenPackages
            scen_pkg_name: The name for the ScenPackage
            fill_layers (bool): Should scenario layers be filled

        Returns:
            ScenPackage object filled with scenarios
        """
        if scen_pkg:
            scen_pkg_id = scen_pkg.get_id()
            user_id = scen_pkg.get_user_id()
            scen_pkg_name = scen_pkg.name
        row, idxs = self.execute_select_one_function(
            'lm_getScenPackage', scen_pkg_id, user_id, scen_pkg_name)
        found_scen_pkg = self._create_scen_package(row, idxs)
        if found_scen_pkg:
            scens = self.get_scenarios_for_scen_package(
                found_scen_pkg, None, None, None, fill_layers)
            found_scen_pkg.set_scenarios(scens)
        return found_scen_pkg

    # ................................
    def get_scen_packages_for_scenario(self, scen, scen_id, user_id, scen_code,
                                       fill_layers):
        """Find all ScenPackages that contain the given Scenario

        Args:
            scen: The Scenario to find scenarios packages for
            scen_id: The database Id for the Scenario to find ScenPackages
            user_id: The user_id for the Scenario to find ScenPackages
            scen_code: Find scenario packages for the scenario with this code

        Returns:
            list of ScenPackage - A list of scenario packages matching the
                criteria and filled with Scenarios
        """
        scen_pkgs = []
        if scen:
            scen_id = scen.get_id()
            user_id = scen.get_user_id()
            scen_code = scen.code
        rows, idxs = self.execute_select_many_function(
            'lm_getScenPackagesForScenario', scen_id, user_id, scen_code)
        for row in rows:
            pkg = self._create_scen_package(row, idxs)
            scens = self.get_scenarios_for_scen_package(
                pkg, None, None, None, fill_layers)
            pkg.set_scenarios(scens)
            scen_pkgs.append(pkg)
        return scen_pkgs

    # ................................
    def get_scenarios_for_scen_package(self, scen_pkg, scen_pkg_id, user_id,
                                       scen_pkg_name, fill_layers):
        """Find all scenarios that are part of the given ScenPackage

        Args:
            scen_pkg: The ScenPackage to find scenarios for
            scen_pkg_id: The database Id for the ScenPackage to find scenarios
            user_id: The user_id for the ScenPackage to find scenarios
            scen_pkg_name: The name for the ScenPackage to find scenarios
            fill_layers (bool): Should scenario layers be filled

        Returns:
            list of Scenarios
        """
        scens = []
        if scen_pkg:
            scen_pkg_id = scen_pkg.get_id()
            user_id = scen_pkg.get_user_id()
            scen_pkg_name = scen_pkg.name
        rows, idxs = self.execute_select_many_function(
            'lm_getScenariosForScenPackage', scen_pkg_id, user_id,
            scen_pkg_name)
        for row in rows:
            scen = self._create_scenario(row, idxs, is_for_model=False)
            if scen is not None and fill_layers:
                lyrs = self.get_scenario_layers(scen.get_id())
                scen.set_layers(lyrs)
            scens.append(scen)

        return scens

    # ................................
    def find_or_insert_scenario(self, scen, scen_pkg_id):
        """Inserts a scenario and any layers present into the database

        Args:
            scen: The scenario to insert
            scen_pkg_id: The id of the scenario package that this scenario
                belongs to.

        Returns:
            Scenario - New or existing scenario
        """
        scen.mod_time = gmt().mjd
        wkt = None
        if scen.epsg_code == DEFAULT_EPSG:
            wkt = scen.get_wkt()
        meta = scen.dump_scenario_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertScenario', scen.get_user_id(), scen.code, meta,
            scen.gcm_code, scen.alt_pred_code, scen.date_code, scen.map_units,
            scen.resolution, scen.epsg_code, scen.get_csv_extent_string(), wkt,
            scen.mod_time)
        new_or_existing_scen = self._create_scenario(row, idxs)
        if scen_pkg_id is not None:
            scenario_id = self._get_column_value(row, idxs, ['scenarioid'])
            join_id = self.execute_modify_return_value(
                'lm_joinScenPackageScenario', scen_pkg_id, scenario_id)
            if join_id < 0:
                raise LMError(
                    'Failed to join ScenPackage {} to Scenario {}'.format(
                        scen_pkg_id, scenario_id))
        return new_or_existing_scen

    # ................................
    def count_scenarios(self, user_id, after_time, before_time, epsg,
                        gcm_code, alt_pred_code, date_code, scen_package_id):
        """Return the number of scenarios fitting the given filter conditions

        Args:
            user_id: filter by LMUser
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by the EPSG spatial reference system code
            gcm_code: filter by the Global Climate Model code
            alt_pred_code: filter by the alternate predictor code (i.e. IPCC
                RCP)
            date_code: filter by the date code
            scen_package_id: filter by a ScenPackage

        Returns:
            Number of scenarios fitting the given filter conditions
        """
        row, _ = self.execute_select_one_function(
            'lm_countScenarios', user_id, after_time, before_time, epsg,
            gcm_code, alt_pred_code, date_code, scen_package_id)
        return self._get_count(row)

    # ................................
    def list_scenarios(self, first_rec_num, max_num, user_id, after_time,
                       before_time, epsg, gcm_code, alt_pred_code, date_code,
                       scen_package_id, atom):
        """Return scenario Objects or Atoms fitting the given filters

        Args:
            first_rec_num: start at this record
            max_num: maximum number of records to return
            user_id: filter by LMUser
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by the EPSG spatial reference system code
            gcm_code: filter by the Global Climate Model code
            alt_pred_code: filter by the alternate predictor code (i.e. IPCC
                RCP)
            date_code: filter by the date code
            scen_package_id: filter by a ScenPackage
            atom: True if return objects will be Atoms, False if full objects
        """
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listScenarioAtoms', first_rec_num, max_num, user_id,
                after_time, before_time, epsg, gcm_code, alt_pred_code,
                date_code, scen_package_id)
            objs = self._get_atoms(rows, idxs, LMServiceType.SCENARIOS)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listScenarioObjects', first_rec_num, max_num, user_id,
                after_time, before_time, epsg, gcm_code, alt_pred_code,
                date_code, scen_package_id)
            for row in rows:
                objs.append(self._create_scenario(row, idxs))
        return objs

    # ................................
    def find_or_insert_env_type(self, env_type):
        """Insert or find EnvType values.

        Args:
             env_type: An EnvType or EnvLayer object

        Returns:
            New or existing EnvironmentalType

        Todo:
            Is this used?  self._create_layer_type does not exist
        """
        currtime = gmt().mjd
        meta = env_type.dump_param_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertEnvType', env_type.get_param_id(),
            env_type.get_param_user_id(), env_type.type_code,
            env_type.gcm_code, env_type.alt_pred_code, env_type.date_code,
            meta, currtime)
        new_or_existing_env_type = self._create_layer_type(row, idxs)
        return new_or_existing_env_type

    # ................................
    def find_or_insert_layer(self, lyr):
        """Find or insert a Layer into the database

        Args:
            lyr: Raster or Vector layer to insert

        Returns:
            New or existing Raster or Vector.
        """
        wkt = None
        if lyr.data_format in LMFormat.ogr_drivers() and \
                lyr.epsg_code == DEFAULT_EPSG:
            wkt = lyr.get_wkt()
        meta = lyr.dump_layer_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertLayer', lyr.get_id(), lyr.get_user_id(), lyr.squid,
            lyr.verify, lyr.name, lyr.get_dlocation(), meta, lyr.data_format,
            lyr.gdal_type, lyr.ogr_type, lyr.val_units, lyr.nodata_val,
            lyr.min_val, lyr.max_val, lyr.epsg_code, lyr.map_units,
            lyr.resolution, lyr.get_csv_extent_string(), wkt, lyr.mod_time)
        updated_lyr = self._create_layer(row, idxs)
        return updated_lyr

    # ................................
    def find_or_insert_shapegrid(self, shpgrd, cutout):
        """Find or insert a ShapeGrid into the database

        Args:
            shpgrd: ShapeGrid to insert

        Returns:
            New or existing ShapeGrid.
        """
        wkt = None
        if shpgrd.epsg_code == DEFAULT_EPSG:
            wkt = shpgrd.get_wkt()
        meta = shpgrd.dump_param_metadata()
        gdal_type = val_units = nodata_val = min_val = max_val = None
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertShapeGrid', shpgrd.get_id(), shpgrd.get_user_id(),
            shpgrd.squid, shpgrd.verify, shpgrd.name, shpgrd.get_dlocation(),
            meta, shpgrd.data_format, gdal_type, shpgrd.ogr_type, val_units,
            nodata_val, min_val, max_val, shpgrd.epsg_code, shpgrd.map_units,
            shpgrd.resolution, shpgrd.get_csv_extent_string(), wkt,
            shpgrd.mod_time, shpgrd.cell_sides, shpgrd.cell_size, shpgrd.size,
            shpgrd.site_id, shpgrd.site_x, shpgrd.site_y, shpgrd.status,
            shpgrd.status_mod_time)
        updated_shapegrid = self._create_shapegrid(row, idxs)
        return updated_shapegrid

    # ................................
    def find_or_insert_gridset(self, grdset):
        """Find or insert a Gridset into the database

        Args:
            grdset: Gridset to insert
        Returns:Updated new or existing Gridset.
        """
        meta = grdset.dump_grid_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertGridset', grdset.get_id(), grdset.get_user_id(),
            grdset.name, grdset.shapegrid_id, grdset.get_dlocation(),
            grdset.epsg_code, meta, grdset.mod_time)
        updated_gridset = self._create_gridset(row, idxs)
        # Populate dlocation in obj then db if this is a new Gridset
        if updated_gridset._dlocation is None:
            updated_gridset.get_dlocation()
            _ = self.update_gridset(updated_gridset)
        return updated_gridset

    # ................................
    def find_user_gridsets(self, userid, obsolete_time=None):
        """Find gridsets for a user

        Args:
            userid: User for gridsets to query
            obsolete_time: time before which objects are considered obsolete

        Returns:
            List of gridset ids for user
        """
        grid_ids = []
        rows, _ = self.execute_select_many_function(
            'lm_findUserGridsets', userid, obsolete_time)
        for row in rows:
            if row[0] is not None:
                grid_ids.append(row[0])

        return grid_ids

    # ................................
    def delete_gridset_return_filenames(self, gridset_id):
        """Deletes Gridset, Matrices, and Makeflows

        Args:
            gridset_id: Gridset for which to delete objects

        Returns:
            List of filenames for all deleted objects
        """
        filenames = []
        rows, _ = self.execute_select_and_modify_many_function(
            'lm_deleteGridset', gridset_id)
        self.log.info(
            'Returned {} files to be deleted for gridset {}'.format(
                len(rows), gridset_id))
        for row in rows:
            if row[0] is not None:
                filenames.append(row[0])

        return filenames

    # ................................
    def delete_gridset_return_mtx_col_ids(self, gridset_id):
        """Deletes SDM MatrixColumns (PAVs) for a Gridset

        Args:
            gridset_id: Gridset for which to delete objects

        Returns:
            List of ids for all deleted MatrixColumns
        """
        mtx_col_ids = []
        rows, _ = self.execute_select_and_modify_many_function(
            'lm_deleteGridsetMatrixColumns', gridset_id)
        self.log.info(
            'Returned {} matrixcolumn ids deleted from gridset {}'.format(
                len(rows), gridset_id))

        for row in rows:
            if row[0] is not None:
                mtx_col_ids.append(row[0])

        return mtx_col_ids

    # ................................
    def get_gridset(self, gridset_id, user_id, name, fill_matrices):
        """Retrieve a Gridset from the database

        Args:
            gridset_id: Database id of the Gridset to retrieve
            user_id: UserId of the Gridset to retrieve
            name: Name of the Gridset to retrieve
            fill_matrices: True/False indicating whether to find and attach any
                matrices associated with this Gridset

        Returns:
            Existing LmServer.legion.gridset.Gridset
        """
        row, idxs = self.execute_select_one_function(
            'lm_getGridset', gridset_id, user_id, name)
        full_gridset = self._create_gridset(row, idxs)
        if full_gridset is not None and fill_matrices:
            mtxs = self.get_matrices_for_gridset(full_gridset.get_id(), None)
            for mtx in mtxs:
                # addMatrix sets userid
                full_gridset.add_matrix(mtx)
        return full_gridset

    # ................................
    def count_gridsets(self, user_id, shapegrid_layer_id, meta_string,
                       after_time, before_time, epsg):
        """Count Matrices matching filter conditions

        Args:
            user_id: User (owner) for which to return MatrixColumns.
            shapegrid_layer_id: filter by ShapeGrid with Layer database ID
            meta_string: find gridsets containing this word in the metadata
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code

        Returns:
            A count of Matrices
        """
        meta_match = None
        if meta_string is not None:
            meta_match = '%{}%'.format(meta_string)
        row, _ = self.execute_select_one_function(
            'lm_countGridsets', user_id, shapegrid_layer_id, meta_match,
            after_time, before_time, epsg)
        return self._get_count(row)

    # ................................
    def list_gridsets(self, first_rec_num, max_num, user_id,
                      shapegrid_layer_id, meta_string, after_time, before_time,
                      epsg, atom):
        """Return Matrix Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return MatrixColumns.
            shapegrid_layer_id: filter by ShapeGrid with Layer database ID
            meta_string: find matrices containing this word in the metadata
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of Matrix atoms or full objects
        """
        meta_match = None
        if meta_string is not None:
            meta_match = '%{}%'.format(meta_string)
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listGridsetAtoms', first_rec_num, max_num, user_id,
                shapegrid_layer_id, meta_match, after_time, before_time, epsg)
            objs = self._get_atoms(rows, idxs, LMServiceType.GRIDSETS)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listGridsetObjects', first_rec_num, max_num, user_id,
                shapegrid_layer_id, meta_match, after_time, before_time, epsg)
            for row in rows:
                objs.append(self._create_gridset(row, idxs))
        return objs

    # ................................
    def update_gridset(self, gridset):
        """Update a LmServer.legion.Gridset

        Args:
            gridset: the LmServer.legion.Gridset object to update

        Returns:
            Boolean success/failure
        """
        meta = gridset.dump_grid_metadata()
        success = self.execute_modify_function(
            'lm_updateGridset', gridset.get_id(), gridset.tree_id,
            gridset.get_dlocation(), meta, gridset.mod_time)
        return success

    # ................................
    def get_matrix(self, mtx_id, gridset_id, gridset_name, user_id, mtx_type,
                   gcm_code, alt_pred_code, date_code, alg_code):
        """Retrieve an LMMatrix object with its gridset from the database

        Args:
            mtx_id: database ID for the LMMatrix
            gridset_id: database ID for the Gridset containing the LMMatrix
            gridset_name: name of the Gridset containing the LMMatrix
            user_id: userID for the Gridset containing the LMMatrix
            mtx_type: LmCommon.common.lmconstants.MatrixType of the LMMatrix
            gcm_code: Global Climate Model Code of the LMMatrix
            alt_pred_code: alternate prediction code of the LMMatrix
            date_code: date code of the LMMatrix
            alg_code: algorithm code of the LMMatrix

        Returns:
            Existing LmServer.legion.lm_matrix.LMMatrix
        """
        row, idxs = self.execute_select_one_function(
            'lm_getMatrix', mtx_id, mtx_type, gridset_id, gcm_code,
            alt_pred_code, date_code, alg_code, gridset_name, user_id)
        full_mtx = self._create_lm_matrix(row, idxs)
        return full_mtx

    # ................................
    def update_shapegrid(self, shpgrd):
        """Update a shapegrid

        Returns:
            Updated record for successful update.
        """
        meta = shpgrd.dump_layer_metadata()
        success = self.execute_modify_function(
            'lm_updateShapeGrid', shpgrd.get_id(), shpgrd.verify,
            shpgrd.get_dlocation(), meta, shpgrd.mod_time, shpgrd.size,
            shpgrd.status, shpgrd.status_mod_time)
        return success

    # ................................
    def get_shapegrid(self, lyr_id, user_id, lyr_name, epsg):
        """Find or insert a ShapeGrid into the database

        Returns:
            New or existing ShapeGrid.
        """
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_getShapeGrid', lyr_id, user_id, lyr_name, epsg)
        shpgrid = self._create_shapegrid(row, idxs)
        return shpgrid

    # ................................
    def count_shapegrids(self, user_id, cell_sides, cell_size, after_time,
                         before_time, epsg):
        """Count all Layers matching the filter conditions

        Args:
            user_id: User (owner) for which to return occurrencesets.
            cell_sides: number of sides of each cell, 4=square, 6=hexagon
            cell_size: size of one side of cell in map_units
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code

        Returns:
            A count of OccurrenceSets
        """
        row, _ = self.execute_select_one_function(
            'lm_countShapegrids', user_id, cell_sides, cell_size, after_time,
            before_time, epsg)
        return self._get_count(row)

    # ................................
    def list_shapegrids(self, first_rec_num, max_num, user_id, cell_sides,
                        cell_size, after_time, before_time, epsg, atom):
        """Return Layer Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return shapegrids.
            cell_sides: number of sides of each cell, 4=square, 6=hexagon
            cell_size: size of one side of cell in map_units
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of Layer atoms or full objects
        """
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listShapegridAtoms', first_rec_num, max_num, user_id,
                cell_sides, cell_size, after_time, before_time, epsg)
            objs = self._get_atoms(rows, idxs, LMServiceType.SHAPEGRIDS)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listShapegridObjects', first_rec_num, max_num, user_id,
                cell_sides, cell_size, after_time, before_time, epsg)
            for row in rows:
                objs.append(self._create_shapegrid(row, idxs))
        return objs

    # ................................
    def find_or_insert_env_layer(self, lyr, scenario_id):
        """Find or insert a layer's metadata in the database and optionally
        join it to the indicated scenario.

        Args:
            lyr: layer to insert
            scenario_id: database id for scenario if joining layer

        Returns:
            New or existing EnvironmentalLayer
        """
        lyr.mod_time = gmt().mjd
        wkt = None
        if lyr.epsg_code == DEFAULT_EPSG:
            wkt = lyr.get_wkt()
        envmeta = lyr.dump_param_metadata()
        lyrmeta = lyr.dump_layer_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertEnvLayer', lyr.get_id(), lyr.get_user_id(),
            lyr.squid, lyr.verify, lyr.name, lyr.get_dlocation(), lyrmeta,
            lyr.data_format, lyr.gdal_type, lyr.ogr_type, lyr.val_units,
            lyr.nodata_val, lyr.min_val, lyr.max_val, lyr.epsg_code,
            lyr.map_units, lyr.resolution, lyr.get_csv_extent_string(), wkt,
            lyr.mod_time, lyr.get_param_id(), lyr.env_code, lyr.gcm_code,
            lyr.alt_pred_code, lyr.date_code, envmeta, lyr.param_mod_time)
        new_or_existing_layer = self._create_env_layer(row, idxs)
        if scenario_id is not None:
            _ = self.execute_insert_and_select_one_function(
                'lm_joinScenarioLayer', scenario_id,
                new_or_existing_layer.get_layer_id(),
                new_or_existing_layer.get_param_id())
        return new_or_existing_layer

    # ................................
    def get_env_layer(self, env_lyr_id, lyr_id, lyr_verify, lyr_user, lyr_name,
                      epsg_code):
        """Get and fill a Layer

        Args:
            env_lyr_id: EnvLayer join id
            lyr_id: Layer database id
            lyr_verify: SHASUM hash of layer data
            lyr_user: Layer user id
            lyr_name: Layer name
            lyr_id: Layer EPSG code

        Returns:
            LmServer.base.layer._Layer object
        """
        row, idxs = self.execute_select_one_function(
            'lm_getEnvLayer', env_lyr_id, lyr_id, lyr_verify, lyr_user,
            lyr_name, epsg_code)
        lyr = self._create_env_layer(row, idxs)
        return lyr

    # ................................
    def count_env_layers(self, user_id, env_code, gcm_code, alt_pred_code,
                         date_code, after_time, before_time, epsg, env_type_id,
                         scenario_code):
        """Count all EnvLayers matching the filter conditions

        Args:
            user_id: User (owner) for which to return occurrencesets.
            env_code: filter by the environmental code (i.e. bio13)
            gcm_code: filter by the Global Climate Model code
            alt_pred_code: filter by the alternate predictor code (i.e. IPCC
                RCP)
            date_code: filter by the date code
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            env_type_id: filter by the DB id of EnvironmentalType

        Returns:
            A count of EnvLayers
        """
        row, _ = self.execute_select_one_function(
            'lm_countEnvLayers', user_id, env_code, gcm_code, alt_pred_code,
            date_code, after_time, before_time, epsg, env_type_id,
            scenario_code)
        return self._get_count(row)

    # ................................
    def list_env_layers(self, first_rec_num, max_num, user_id, env_code,
                        gcm_code, alt_pred_code, date_code, after_time,
                        before_time, epsg, env_type_id, scen_code, atom):
        """List all EnvLayer objects or atoms matching the filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return occurrencesets.
            env_code: filter by the environmental code (i.e. bio13)
            gcm_code: filter by the Global Climate Model code
            alt_pred_code: filter by the alternate predictor code (i.e. IPCC
                RCP)
            date_code: filter by the date code
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            env_type_id: filter by the DB id of EnvironmentalType
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of EnvLayer objects or atoms
        """
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listEnvLayerAtoms', first_rec_num, max_num, user_id,
                env_code, gcm_code, alt_pred_code, date_code, after_time,
                before_time, epsg, env_type_id, scen_code)
            objs = self._get_atoms(
                rows, idxs, LMServiceType.ENVIRONMENTAL_LAYERS)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listEnvLayerObjects', first_rec_num, max_num, user_id,
                env_code, gcm_code, alt_pred_code, date_code, after_time,
                before_time, epsg, env_type_id, scen_code)
            for row in rows:
                objs.append(self._create_env_layer(row, idxs))
        return objs

    # ................................
    def delete_env_layer(self, env_lyr):
        """Delete an environmental layer

        Un-joins EnvLayer from scenario (if not None) and deletes Layer if it
        is not in any Scenarios or MatrixColumns

        Args:
            envlyr: EnvLayer to delete (if orphaned)

        Returns:
            True/False for success of operation

        Note:
            - The layer will not be removed if it is used in any scenarios
            - If the EnvType is orphaned, it will also be removed
        """
        success = self.execute_modify_function(
            'lm_deleteEnvLayer', env_lyr.get_id())
        return success

    # ................................
    def find_or_insert_user(self, usr):
        """Insert a user of the Lifemapper system.

        Args:
            usr: LMUser object to insert

        Returns:
            New or existing LMUser
        """
        usr.mod_time = gmt().mjd
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertUser', usr.user_id, usr.first_name, usr.last_name,
            usr.institution, usr.address_1, usr.address_2, usr.address_3,
            usr.phone, usr.email, usr.mod_time, usr.get_password())
        new_or_existing_user = self._create_user(row, idxs)
        if usr.user_id != new_or_existing_user.user_id:
            self.log.info(
                'Failed to add new user {}; matching email for user {}'.format(
                    usr.user_id, new_or_existing_user.user_id))
        return new_or_existing_user

    # ................................
    def update_user(self, usr):
        """Insert a user of the Lifemapper system.

        Args:
            usr: LMUser object to update

        Returns:
            Updated LMUser
        """
        usr.mod_time = gmt().mjd
        success = self.execute_modify_function(
            'lm_updateUser', usr.user_id, usr.first_name, usr.last_name,
            usr.institution, usr.address_1, usr.address_2, usr.address_3,
            usr.phone, usr.email, usr.mod_time, usr.get_password())
        return success

    # ................................
    def find_user_for_object(self, layer_id, scen_code, occ_id, matrix_id,
                             gridset_id, mf_process_id):
        """Find a user_id for an LM Object identifier in the database

        Args:
            layer_id: the database primary key for a Layer
            scen_code: the code for a Scenario
            occ_id: the database primary key for a Layer in the database
            matrix_id: the database primary key for a Matrix
            gridset_id: the database primary key for a Gridset
            mf_process_id: the database primary key for a MFProcess

        Returns:
            A user_id string
        """
        row, _ = self.execute_select_one_function(
            'lm_findUserForObject', layer_id, scen_code, occ_id, matrix_id,
            gridset_id, mf_process_id)
        user_id = row[0]
        return user_id

    # ................................
    def find_user(self, user_id, email):
        """Find a user with either a matching user_id or email address

        Args:
            user_id: the database primary key of the LMUser in the Borg
            email: the email address of the LMUser in the Borg

        Returns:
            A LMUser object
        """
        row, idxs = self.execute_select_one_function(
            'lm_findUser', user_id, email)
        usr = self._create_user(row, idxs)
        return usr

    # ................................
    def delete_computed_user_data(self, user_id):
        """Deletes computed user data

        Args:
            user_id: User for whom to delete SDM records, MatrixColumns,
                Makeflows

        Returns:
            True/False for success of operation

        Note:
            All makeflows will be deleted, regardless
        """
        success = False
        occ_del_count = self.execute_modify_function(
            'lm_clearComputedUserData', user_id)
        self.log.info(
            'Deleted {} occs, dependent objects, and mfs for user {}'.format(
                occ_del_count, user_id))

        if occ_del_count > 0:
            success = True
        return success

    # ................................
    def clear_user(self, user_id):
        """Deletes all User data

        Args:
            user_id: User for whom to delete data

        Returns:
            True/False for success of operation
        """
        success = False
        del_count = self.execute_modify_return_value(
            'lm_clearUserData', user_id)
        self.log.info(
            'Deleted {} data objects for user {}'.format(del_count, user_id))

        if del_count > 0:
            success = True
        return success

    # ................................
    def count_job_chains(self, status, user_id=None):
        """Return the number of jobchains fitting the given filter conditions

        Args:
            status: include only jobs with this status
            user_id: (optional) include only jobs with this userid

        Returns:
            Number of jobs fitting the given filter conditions
        """
        row, _ = self.execute_select_one_function(
            'lm_countJobChains', user_id, status)
        return self._get_count(row)

    # ................................
    def find_taxon_source(self, taxon_source_name):
        """Return the taxonomy source info given the name

        Args:
            taxon_source_name: unique name of this taxonomy source

        Returns:
            Database id, url, and modification time of this source
        """
        tax_source_id = url = mod_date = None
        if taxon_source_name is not None:
            try:
                row, idxs = self.execute_select_one_function(
                    'lm_findTaxonSource', taxon_source_name)
            except LMError as err:
                raise err
            except Exception as err:
                raise LMError(err, line_num=self.get_line_num())
            if row is not None:
                tax_source_id = self._get_column_value(
                    row, idxs, ['taxonomysourceid'])
                url = self._get_column_value(row, idxs, ['url'])
                mod_date = self._get_column_value(row, idxs, ['modtime'])
        return tax_source_id, url, mod_date

    # ................................
    def get_taxon_source(self, tax_src_id, tax_src_name, tax_src_url):
        """Return the taxonomy source info given the id, name or url

        Args:
            tax_src_id: database id of this taxonomy source
            tax_src_name: unique name of this taxonomy source
            tax_src_url: unique url of this taxonomy source

        Returns:
            Named tuple with database id, name, url, and modification time of
                this source
        """
        tax_src = None
        try:
            row, idxs = self.execute_select_one_function(
                'lm_getTaxonSource', tax_src_id, tax_src_name, tax_src_url)
        except LMError as err:
            raise err
        except Exception as err:
            raise LMError(err, line_num=self.get_line_num())
        if row is not None:
            fld_names = []
            for key, _ in sorted(
                    iter(idxs.items()), key=lambda k_v: (k_v[1], k_v[0])):
                fld_names.append(key)
            TaxonSource = namedtuple('TaxonSource', fld_names)
            tax_src = TaxonSource(*row)
        return tax_src

    # ................................
    def find_taxon(self, taxon_source_id, taxon_key):
        """Find a taxon in the database
        """
        try:
            row, idxs = self.execute_select_one_function(
                'lm_findOrInsertTaxon', taxon_source_id, taxon_key, None, None,
                None, None, None, None, None, None, None, None, None, None,
                None, None, None, None)
        except Exception as err:
            raise LMError('Error retrieving taxon name', err)
        sciname = self._create_scientific_name(row, idxs)
        return sciname

    # ................................
    def find_or_insert_taxon(self, taxon_source_id, taxon_key, sci_name):
        """Insert a taxon associated with a TaxonomySource into the database

        Args:
            taxon_source_id: Lifemapper database ID of the TaxonomySource
            taxon_key: unique identifier of the taxon in the (external)
                TaxonomySource
            sci_name: ScientificName object with taxonomy information for this
                taxon

        Returns:
            New or existing ScientificName
        """
        scientific_name = None
        curr_time = gmt().mjd
        usr = squid = last_count = None
        kingdom = phylum = class_ = order_ = family = genus = None
        rank = can_name = sci_namestr = gen_key = sp_key = key_hierarchy = None
        
        try:
            if not taxon_source_id:
                taxon_source_id = sci_name.taxonomy_source_id
            if not taxon_key:
                taxon_key = sci_name.source_taxon_key
            usr = sci_name.user_id
            squid = sci_name.squid
            kingdom = sci_name.kingdom
            phylum = sci_name.phylum
            class_ = sci_name.class_
            order_ = sci_name.order_
            family = sci_name.family
            genus = sci_name.genus
            rank = sci_name.rank
            can_name = sci_name.canonical_name
            sci_namestr = sci_name.scientific_name
            gen_key = sci_name.source_genus_key
            sp_key = sci_name.source_species_key
            key_hierarchy = sci_name.source_key_hierarchy
            last_count = sci_name.last_occurrence_count
        except:
            pass

        try:
            row, idxs = self.execute_insert_and_select_one_function(
                'lm_findOrInsertTaxon', taxon_source_id, taxon_key, usr, squid,
                kingdom, phylum, class_, order_, family, genus, rank, can_name,
                sci_namestr, gen_key, sp_key, key_hierarchy, last_count,
                curr_time)
        except Exception as e:
            raise e
        else:
            scientific_name = self._create_scientific_name(row, idxs)

        return scientific_name

    # ................................
    def update_taxon(self, sci_name):
        """Update a taxon in the database.

        Args:
            sci_name: ScientificName object with taxonomy information for this
                taxon

        Returns:
            Updated ScientificName

        Note:
            Does not modify any foreign key (squid), or unique-constraint
                values, (taxonomySource, taxonKey, user_id, sciname).
        """
        success = self.execute_modify_function(
            'lm_updateTaxon', sci_name.get_id(), sci_name.kingdom,
            sci_name.phylum, sci_name.class_, sci_name.order_, sci_name.family,
            sci_name.genus, sci_name.rank, sci_name.canonical_name,
            sci_name.source_genus_key, sci_name.source_species_key,
            sci_name.source_key_hierarchy, sci_name.last_occurrence_count,
            gmt().mjd)
        return success

    # ................................
    def get_taxon(self, squid, taxon_source_id, taxon_key, user_id,
                  taxon_name):
        """Find a taxon associated with a TaxonomySource from database.

        Args:
            squid: Hash value of either taxonSourceId+taxonKey or
                user_id+taxonName
            taxonSourceId: Lifemapper database ID of the TaxonomySource
            taxonKey: unique identifier of the taxon in the (external)
                TaxonomySource
            user_id: User id for the scenario to be fetched.
            taxon_ame: name string for this taxon

        Returns:
            Existing ScientificName
        """
        row, idxs = self.execute_select_one_function(
            'lm_getTaxon', squid, taxon_source_id, taxon_key, user_id,
            taxon_name)
        scientific_name = self._create_scientific_name(row, idxs)

        return scientific_name

    # ................................
    def get_scenario(self, scen_id=None, user_id=None, code=None,
                     fill_layers=False):
        """Get and fill a scenario from its user and code or database id.

        Args:
            scen_id: ScenarioId for the scenario to be fetched.
            code: Code for the scenario to be fetched.
            user_id: User id for the scenario to be fetched.
            fillLayers: Boolean indicating whether to retrieve and populate
                layers from to be fetched.
        Returns:
            A LmServer.legion.scenario.Scenario object

        Note:
            If  fill_layers is true, populate the layers in the object.
        """
        row, idxs = self.execute_select_one_function(
            'lm_getScenario', scen_id, user_id, code)
        scen = self._create_scenario(row, idxs)
        if scen is not None and fill_layers:
            lyrs = self.get_scenario_layers(scen.get_id())
            scen.set_layers(lyrs)
        return scen

    # ................................
    def get_scenario_layers(self, scen_id):
        """Return a scenario by its db id or code, filling its layers.

        Args:
            code: Code for the scenario to be fetched.
            scen_id: ScenarioId for the scenario to be fetched.
        """
        lyrs = []
        rows, idxs = self.execute_select_many_function(
            'lm_getEnvLayersForScenario', scen_id)
        for row in rows:
            lyr = self._create_env_layer(row, idxs)
            lyrs.append(lyr)
        return lyrs

    # ................................
    def get_occurrence_set(self, occ_id, squid, user_id, epsg):
        """Get an occurrenceset for the given id or squid and User

        Args:
            occ_id: the database primary key of the Occurrence record
            squid: a species identifier, tied to a ScientificName
            user_id: the database primary key of the LMUser
            epsg: Spatial reference system code from EPSG
        """
        row, idxs = self.execute_select_one_function(
            'lm_getOccurrenceSet', occ_id, user_id, squid, epsg)
        occ = self._create_occurrence_layer(row, idxs)
        return occ

    # ................................
    def update_occurrence_set(self, occ):
        """Update OccurrenceLayer attributes

        Args:
            occ: OccurrenceLayer to be updated.

        Note:
            Does not update the userid, squid, and epsg_code
                (unique constraint)

        Returns:
            True/False for successful update.
        """
        success = False
        poly_wkt = points_wkt = None
        metadata = occ.dump_layer_metadata()
        try:
            poly_wkt = occ.get_convex_hull_wkt()
        except Exception:
            pass
#         try:
#             points_wkt = occ.get_multipoint_wkt(LMFormat.SHAPE.driver)
#         except Exception:
#             pass
        try:
            success = self.execute_modify_function(
                'lm_updateOccurrenceSet', occ.get_id(), occ.verify,
                occ.display_name, occ.get_dlocation(), occ.get_raw_dlocation(),
                occ.query_count, occ.get_csv_extent_string(), occ.epsg_code,
                metadata, occ.status, occ.status_mod_time, poly_wkt,
                points_wkt)
        except Exception as err:
            raise LMError('Failed to update occurrence set', err)
        return success

    # ................................
    def get_sdm_project(self, layer_id):
        """Get a projection for the given id

        Args:
            layer_id: Database id for the SDMProject layer record
        """
        row, idxs = self.execute_select_one_function(
            'lm_getSDMProjectLayer', layer_id)
        proj = self._create_sdm_projection(row, idxs)
        return proj

    # ................................
    def update_sdm_project(self, proj):
        """Method to update an SDMProjection object in the database

        Args:
            proj: The SDMProjection object to update
        """
        success = False
        lyr_meta = proj.dump_layer_metadata()
        prj_meta = proj.dump_param_metadata()
        try:
            success = self.execute_modify_function(
                'lm_updateSDMProjectLayer', proj.get_param_id(), proj.get_id(),
                proj.verify, proj.get_dlocation(), lyr_meta, proj.val_units,
                proj.nodata_val, proj.min_val, proj.max_val, proj.epsg_code,
                proj.get_csv_extent_string(), proj.get_wkt(), proj.mod_time,
                prj_meta, proj.status, proj.status_mod_time)
        except Exception as err:
            raise LMError('Failed to update SDM projection', err)
        return success

    # ................................
    def find_or_insert_occurrence_set(self, occ):
        """Find or insert an occurrence set

        Args:
            occ: New OccurrenceSet to save

        Returns:
            New or existing OccurrenceLayer
        """
        poly_wkt = points_wkt = None
        point_total = occ.query_count
        if occ.get_features():
            point_total = occ.feature_count
            poly_wkt = occ.get_convex_hull_wkt()
            points_wkt = occ.get_wkt()

        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertOccurrenceSet', occ.get_id(), occ.get_user_id(),
            occ.squid, occ.verify, occ.display_name, occ.get_dlocation(),
            occ.get_raw_dlocation(), point_total, occ.get_csv_extent_string(),
            occ.epsg_code, occ.dump_layer_metadata(), occ.status,
            occ.status_mod_time, poly_wkt, points_wkt)
        new_or_existing_occ = self._create_occurrence_layer(row, idxs)
        return new_or_existing_occ

    # ................................
    def count_occurrence_sets(self, user_id, squid, min_occurrence_count,
                              display_name, after_time, before_time, epsg,
                              after_status, before_status, gridset_id):
        """Count all OccurrenceSets matching the filter conditions

        Args:
            user_id: User (owner) for which to return occurrencesets.
            min_occurrence_count: filter by minimum number of points in set.
            display_name: filter by display name *starting with* this string
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            after_status: filter by status >= value
            before_status: filter by status <= value
            gridset_id: filter by occurrenceset used by this gridset

        Returns:
            A list of OccurrenceSet atoms or full objects
        """
        if display_name is not None:
            display_name = display_name.strip() + '%'
        row, _ = self.execute_select_one_function(
            'lm_countOccSets', user_id, squid, min_occurrence_count,
            display_name, after_time, before_time, epsg, after_status,
            before_status, gridset_id)
        return self._get_count(row)

    # ................................
    def list_occurrence_sets(self, first_rec_num, max_num, user_id, squid,
                             min_occurrence_count, display_name, after_time,
                             before_time, epsg, after_status, before_status,
                             gridset_id, atom):
        """Return OccurrenceSet Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return occurrencesets.
            min_occurrence_count: filter by minimum number of points in set.
            display_name: filter by display name
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            after_status: filter by status >= value
            before_status: filter by status <= value
            gridset_id: filter by occurrenceset used by this gridset
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of OccurrenceSet atoms or full objects
        """
        if display_name is not None:
            display_name = display_name.strip() + '%'
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listOccSetAtoms', first_rec_num, max_num, user_id, squid,
                min_occurrence_count, display_name, after_time, before_time,
                epsg, after_status, before_status, gridset_id)
            objs = self._get_atoms(rows, idxs, LMServiceType.OCCURRENCES)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listOccSetObjects', first_rec_num, max_num, user_id, squid,
                min_occurrence_count, display_name, after_time, before_time,
                epsg, after_status, before_status, gridset_id)
            for row in rows:
                objs.append(self._create_occurrence_layer(row, idxs))
        return objs

    # ................................
    def summarize_occurrence_sets_for_gridset(self, gridset_id):
        """Count all OccurrenceSets for a gridset by status

        Args:
            gridset_id: a database ID for the LmServer.legion.Gridset

        Returns:
            A list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.execute_select_many_function(
            'lm_summarizeOccSetsForGridset', gridset_id, MatrixType.PAM,
            MatrixType.ROLLING_PAM)
        for row in rows:
            status_total_pairs.append(
                (row[idxs['status']], row[idxs['total']]))
        return status_total_pairs

    # ................................
    def delete_occurrence_set(self, occ):
        """Deletes OccurrenceSet and any dependent SDMProjects (with Layer).

        Args:
            occ: OccurrenceSet to delete

        Returns:
            True/False for success of operation

        Note:
            If dependent SDMProject is input to a MatrixColumn of a Rolling
                (Global) PAM, the MatrixColumn will also be deleted.
        """
        # TODO: Does this called function exist?
        _ = self._delete_occset_dependent_matrix_cols(
            occ.get_id(), occ.get_user_id())
        success = self.execute_modify_function(
            'lm_deleteOccurrenceSet', occ.get_id())
        return success

    # ................................
    def delete_obsolete_sdm_data_return_ids(self, user_id, before_time,
                                            max_num):
        """Deletes obsolete SDM data and return ids

        Args:
            userid: User for whom to delete SDM data
            before_time: delete SDM data modified before or at this time
            max_num: limit on number of occsets to process

        Returns:
            List of occurrenceset ids for deleted data.
        """
        occ_ids = []
        time_str = LmTime.from_mjd(before_time).strftime()
        rows, _ = self.execute_select_and_modify_many_function(
            'lm_clearSomeObsoleteSpeciesDataForUser2', user_id, before_time,
            max_num)
        for row in rows:
            if row[0] is not None and row[0] != '':
                occ_ids.append(row[0])

        self.log.info(
            'Deleted {} occ older than {} and depencies for {}'.format(
                len(rows), time_str, user_id))
        return occ_ids

    # ................................
    def delete_obsolete_sdm_mtx_cols_return_ids(self, user_id, before_time,
                                                max_num):
        """Deletes SDMProject-dependent MatrixColumns for obsolete occ

        Args:
            user_id: User for whom to delete SDM data
            before_time: delete SDM data modified before or at this time
            max_num: limit on number of occsets to process

        Returns:
            List of occurrenceset ids for deleted data.
        """
        mtx_col_ids = []
        time_str = LmTime.from_mjd(before_time).strftime()
        rows, _ = self.execute_select_and_modify_many_function(
            'lm_clearSomeObsoleteMtxcolsForUser', user_id, before_time,
            max_num)
        for row in rows:
            if row[0] is not None and row[0] != '':
                mtx_col_ids.append(row[0])

        self.log.info(
            'Deleted {} matrix columns for occ older than {}'.format(
                len(rows), time_str))
        return mtx_col_ids

    # ................................
    def _find_occset_dependents(self, occ_id, usr, return_projs=True,
                                return_mtx_cols=True):
        """Finds any dependent SDMProjects and MatrixColumns for the occ

        Args:
            occ_id: OccurrenceSet for which to find dependents
            usr: User (owner) of the OccurrenceSet for which to find dependents
            return_projs: flag indicating whether to return projection objects
                (True) or empty list (False)
            return_mtx_cols: flag indicating whether to return MatrixColumn
                objects (True) or empty list (False)

        Returns:
            List of projection atoms/objects, list of MatrixColumns
        """
        pavs = []
        prjs = self.list_sdm_projects(
            0, 500, usr, None, None, None, None, None, None, None, occ_id,
            None, None, None, None, not return_projs)
        if return_mtx_cols:
            for prj in prjs:
                layer_id = prj.get_id()
                pavs = self.list_matrix_columns(
                    0, 500, usr, None, None, None, None, None, None, None,
                    None, None, layer_id, False)
        if not return_projs:
            prjs = []
        return prjs, pavs

    # ................................
    def find_or_insert_sdm_project(self, proj):
        """Find or insert an SDM Projection

        Args:
            proj: the SDMProjection object to update

        Returns:
            New or existing SDMProjection

        Note:
            Assumes that pre- or post-processing layer inputs have already been
                inserted
        """
        lyr_meta = proj.dump_layer_metadata()
        prj_meta = proj.dump_param_metadata()
        alg_params = proj.dump_algorithm_parameter_string()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertSDMProjectLayer', proj.get_param_id(),
            proj.get_id(), proj.get_user_id(), proj.squid, proj.verify,
            proj.name, proj.get_dlocation(), lyr_meta, proj.data_format,
            proj.gdal_type, proj.ogr_type, proj.val_units, proj.nodata_val,
            proj.min_val, proj.max_val, proj.epsg_code, proj.map_units,
            proj.resolution, proj.get_csv_extent_string(), proj.get_wkt(),
            proj.mod_time, proj.get_occ_layer_id(), proj.algorithm_code,
            alg_params, proj.get_model_scenario_id(),
            proj.get_proj_scenario_id(), prj_meta, proj.process_type,
            proj.status, proj.status_mod_time)
        new_or_existing_proj = self._create_sdm_projection(row, idxs)
        return new_or_existing_proj

    # ................................
    def count_sdm_projects(self, user_id, squid, display_name, after_time,
                           before_time, epsg, after_status, before_status,
                           occset_id, alg_code, mdl_scen_code, prj_scen_code,
                           gridset_id):
        """Count all SDMProjects matching the filter conditions

        Args:
            user_id: User (owner) for which to return occurrencesets.
            squid: a species identifier, tied to a ScientificName
            display_name: filter by display name *starting with* this string
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            after_status: filter by status >= value
            before_status: filter by status <= value
            occsetId: filter by occ_layer identifier
            alg_code: filter by algorithm code
            mdl_scen_code: filter by model scenario code
            prj_scen_code: filter by projection scenario code
            gridset_id: filter by projection included in this gridset

        Returns:
            A count of SDMProjects
        """
        if display_name is not None:
            display_name = display_name.strip() + '%'
        row, _ = self.execute_select_one_function(
            'lm_countSDMProjects', user_id, squid, display_name, after_time,
            before_time, epsg, after_status, before_status, occset_id,
            alg_code, mdl_scen_code, prj_scen_code, gridset_id)
        return self._get_count(row)

    # ................................
    def list_sdm_projects(self, first_rec_num, max_num, user_id, squid,
                          display_name, after_time, before_time, epsg,
                          after_status, before_status, occset_id, alg_code,
                          mdl_scen_code, prj_scen_code, gridset_id, atom):
        """Return SDMProjects Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return occurrencesets.
            squid: a species identifier, tied to a ScientificName
            display_name: filter by display name
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            after_status: filter by status >= value
            before_status: filter by status <= value
            occsetId: filter by occ_layer identifier
            alg_code: filter by algorithm code
            mdl_scen_code: filter by model scenario code
            prj_scen_code: filter by projection scenario code
            gridset_id: filter by projection included in this gridset
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of SDMProjects atoms or full objects
        """
        if display_name is not None:
            display_name = display_name.strip() + '%'
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listSDMProjectAtoms', first_rec_num, max_num, user_id,
                squid, display_name, after_time, before_time, epsg,
                after_status, before_status, occset_id, alg_code,
                mdl_scen_code, prj_scen_code, gridset_id)
            objs = self._get_atoms(rows, idxs, LMServiceType.PROJECTIONS)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listSDMProjectObjects', first_rec_num, max_num, user_id,
                squid, display_name, after_time, before_time, epsg,
                after_status, before_status, occset_id, alg_code,
                mdl_scen_code, prj_scen_code, gridset_id)
            for row in rows:
                objs.append(self._create_sdm_projection(row, idxs))
        return objs

    # ................................
    def find_or_insert_matrix_column(self, mtx_col):
        """Find or insert a matrix column

        Returns:
            New or existing MatrixColumn object
        """
        lyr_id = None
        if mtx_col.layer is not None:
            # Check for existing id before pulling from db
            lyr_id = mtx_col.layer.get_layer_id()
            if lyr_id is None:
                new_or_existing_lyr = self.find_or_insert_layer(mtx_col.layer)
                lyr_id = new_or_existing_lyr.get_layer_id()

        mc_meta = mtx_col.dump_param_metadata()
        int_params = mtx_col.dump_intersect_params()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertMatrixColumn', mtx_col.get_param_user_id(),
            mtx_col.get_param_id(), mtx_col.parent_id,
            mtx_col.get_matrix_index(), lyr_id, mtx_col.squid, mtx_col.ident,
            mc_meta, int_params, mtx_col.status, mtx_col.status_mod_time)
        new_or_existing_mtx_col = self._create_matrix_column(row, idxs)
        # Put shapegrid into updated matrixColumn
        new_or_existing_mtx_col.shapegrid = mtx_col.shapegrid
        new_or_existing_mtx_col.process_type = mtx_col.process_type
        return new_or_existing_mtx_col

    # ................................
    def update_matrix_column(self, mtxcol):
        """Update a MatrixColumn

        Args:
            mtxcol: the LmServer.legion.MatrixColumn object to update

        Returns:
            Boolean success/failure
        """
        meta = mtxcol.dump_param_metadata()
        int_params = mtxcol.dump_intersect_params()
        return self.execute_modify_function(
            'lm_updateMatrixColumn', mtxcol.get_id(),
            mtxcol.get_matrix_index(), meta, int_params, mtxcol.status,
            mtxcol.status_mod_time)

    # ................................
    def get_matrix_column(self, mtx_col, mtx_col_id):
        """Get an existing MatrixColumn

        Args:
            mtx_col: a MatrixColumn object with unique parameters matching the
                existing MatrixColumn to return
            mtx_col_id: a database ID for the LmServer.legion.MatrixColumn
                object to return

        Returns:
            A LmServer.legion.MatrixColumn object
        """
        row = None
        if mtx_col is not None:
            int_params = mtx_col.dump_intersect_params()
            row, idxs = self.execute_select_one_function(
                'lm_getMatrixColumn', mtx_col.get_id(), mtx_col.parent_id,
                mtx_col.get_matrix_index(), mtx_col.get_layer_id(), int_params)
        elif mtx_col_id is not None:
            row, idxs = self.execute_select_one_function(
                'lm_getMatrixColumn', mtx_col_id, None, None, None, None)
        mtx_column = self._create_matrix_column(row, idxs)
        return mtx_column

    # ................................
    def get_columns_for_matrix(self, mtx_id):
        """Get all existing MatrixColumns for a Matrix

        Args:
            mtx_id: a database ID for the LmServer.legion.LMMatrix object to
                return columns for

        Returns:
            A list of LmServer.legion.MatrixColumn objects
        """
        mtx_columns = []
        if mtx_id is not None:
            rows, idxs = self.execute_select_many_function(
                'lm_getColumnsForMatrix', mtx_id)
            for row in rows:
                mtx_col = self._create_matrix_column(row, idxs)
                mtx_columns.append(mtx_col)
        return mtx_columns

    # ................................
    def get_sdm_columns_for_matrix(self, mtx_id, return_columns,
                                   return_projections):
        """Get all existing MatrixColumns built from that have SDMProjections

        Args:
            mtx_id: a database ID for the LMMatrix object to return columns for
            return_Columns: option to return MatrixColumn objects
            return_Projections: option to return SDMProjection objects

        Returns:
            A list of tuples containing MatrixColumn and SDMProjection objects.
        """
        col_prj_pairs = []
        if mtx_id is not None:
            rows, idxs = self.execute_select_many_function(
                'lm_getSDMColumnsForMatrix', mtx_id)
            for row in rows:
                mtx_col = sdm_prj = layer = None
                if return_columns:
                    mtx_col = self._create_matrix_column(row, idxs)
                    layer = mtx_col.layer
                if return_projections:
                    sdm_prj = self._create_sdm_projection(
                        row, idxs, layer=layer)
                col_prj_pairs.append((mtx_col, sdm_prj))
        return col_prj_pairs

    # ................................
    def summarize_sdm_projects_for_gridset(self, gridset_id):
        """Count all SDMProjections for a gridset by status

        Args:
            gridset_id: a database ID for the LmServer.legion.Gridset

        Returns:
            A list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.execute_select_many_function(
            'lm_summarizeSDMColumnsForGridset', gridset_id, MatrixType.PAM,
            MatrixType.ROLLING_PAM)
        for row in rows:
            status_total_pairs.append(
                (row[idxs['status']], row[idxs['total']]))
        return status_total_pairs

    # ................................
    def summarize_mtx_columns_for_gridset(self, gridset_id, mtx_type):
        """Count all MatrixColumns for a gridset by status

        Args:
            gridset_id: a database ID for the LmServer.legion.Gridset
            mtx_type: optional filter for type of matrix to count

        Returns:
            A list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.execute_select_many_function(
            'lm_summarizeMtxColsForGridset', gridset_id, mtx_type)
        for row in rows:
            status_total_pairs.append(
                (row[idxs['status']], row[idxs['total']]))
        return status_total_pairs

    # ................................
    def summarize_matrices_for_gridset(self, gridset_id, mtx_type):
        """Count all matrices for a gridset by status

        Args:
            gridset_id: a database ID for the LmServer.legion.Gridset
            mtx_type: optional filter for type of matrix to count

        Returns:
            A list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.execute_select_many_function(
            'lm_summarizeMatricesForGridset', gridset_id, mtx_type)
        for row in rows:
            status_total_pairs.append(
                (row[idxs['status']], row[idxs['total']]))
        return status_total_pairs

    # ................................
    def get_occ_layers_for_matrix(self, mtx_id):
        """Get all occurrence layer objects for a matrix

        Args:
            mtx_id: A database ID for the LMMatrix object to return columns for

        Returns:
            A list of LmServer.legion.OccurrenceLayer objects
        """
        occ_sets = []
        if mtx_id is not None:
            rows, idxs = self.execute_select_many_function(
                'lm_getOccLayersForMatrix', mtx_id)
            for row in rows:
                occ_sets.append(self._create_occurrence_layer(row, idxs))
        return occ_sets

    # ................................
    def count_matrix_columns(self, user_id, squid, ident, after_time,
                             before_time, epsg, after_status, before_status,
                             gridset_id, matrix_id, layer_id):
        """Return count of MatrixColumns matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return MatrixColumns.
            squid: a species identifier, tied to a ScientificName
            ident: a layer identifier for non-species data
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            after_status: filter by status >= value
            before_status: filter by status <= value
            matrix_id: filter by Matrix identifier
            layer_id: filter by Layer input identifier

        Returns:
            A count of MatrixColumns
        """
        row, _ = self.execute_select_one_function(
            'lm_countMtxCols', user_id, squid, ident, after_time, before_time,
            epsg, after_status, before_status, gridset_id, matrix_id, layer_id)
        return self._get_count(row)

    # ................................
    def list_matrix_columns(self, first_rec_num, max_num, user_id, squid,
                            ident, after_time, before_time, epsg, after_status,
                            before_status, gridset_id, matrix_id, layer_id,
                            atom):
        """Return MatrixColumn Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return MatrixColumns.
            squid: a species identifier, tied to a ScientificName
            ident: a layer identifier for non-species data
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            after_status: filter by status >= value
            before_status: filter by status <= value
            matrix_id: filter by Matrix identifier
            layer_id: filter by Layer input identifier
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of MatrixColumn atoms or full objects
        """
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listMtxColAtoms', first_rec_num, max_num, user_id, squid,
                ident, after_time, before_time, epsg, after_status,
                before_status, gridset_id, matrix_id, layer_id)
            objs = self._get_atoms(rows, idxs, LMServiceType.MATRIX_COLUMNS)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listMtxColObjects', first_rec_num, max_num, user_id, squid,
                ident, after_time, before_time, epsg, after_status,
                before_status, gridset_id, matrix_id, layer_id)
            for row in rows:
                objs.append(self._create_matrix_column(row, idxs))
        return objs

    # ................................
    def update_matrix(self, mtx):
        """Update a LMMatrix

        Args:
            mtx: The LMMatrix object to update

        Returns:
            Boolean success/failure

        Todo:
            Allow update of MatrixType, gcm_code, alt_pred_code, date_code?
        """
        meta = mtx.dump_matrix_metadata()
        success = self.execute_modify_function(
            'lm_updateMatrix', mtx.get_id(), mtx.get_dlocation(), meta,
            mtx.status, mtx.status_mod_time)
        return success

    # ................................
    def count_matrices(self, user_id, matrix_type, gcm_code, alt_pred_code,
                       date_code, alg_code, meta_string, gridset_id,
                       after_time, before_time, epsg, after_status,
                       before_status):
        """Count Matrices matching filter conditions

        Args:
            user_id: User (owner) for which to return MatrixColumns.
            matrix_type: filter by LmCommon.common.lmconstants.MatrixType
            gcm_code: filter by the Global Climate Model code
            alt_pred_code: filter by the alternate predictor code (i.e. IPCC
                RCP)
            date_code: filter by the date code
            meta_string: find matrices containing this word in the metadata
            gridset_id: find matrices in the Gridset with this identifier
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            after_status: filter by status >= value
            before_status: filter by status <= value

        Returns:
            A count of Matrices
        """
        metamatch = None
        if meta_string is not None:
            metamatch = '%{}%'.format(meta_string)
        row, _ = self.execute_select_one_function(
            'lm_countMatrices', user_id, matrix_type, gcm_code, alt_pred_code,
            date_code, alg_code, metamatch, gridset_id, after_time,
            before_time, epsg, after_status, before_status)
        return self._get_count(row)

    # ................................
    def list_matrices(self, first_rec_num, max_num, user_id, matrix_type,
                      gcm_code, alt_pred_code, date_code, alg_code,
                      meta_string, gridset_id, after_time, before_time, epsg,
                      after_status, before_status, atom):
        """Return Matrix Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return MatrixColumns.
            matrix_type: filter by LmCommon.common.lmconstants.MatrixType
            gcm_code: filter by the Global Climate Model code
            alt_pred_code: filter by the alternate predictor code (i.e. IPCC
                RCP)
            date_code: filter by the date code
            meta_string: find matrices containing this word in the metadata
            gridset_id: find matrices in the Gridset with this identifier
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            epsg: filter by this EPSG code
            after_status: filter by status >= value
            before_status: filter by status <= value
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of Matrix atoms or full objects
        """
        metamatch = None
        if meta_string is not None:
            metamatch = '%{}%'.format(meta_string)
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listMatrixAtoms', first_rec_num, max_num, user_id,
                matrix_type, gcm_code, alt_pred_code, date_code, alg_code,
                metamatch, gridset_id, after_time, before_time, epsg,
                after_status, before_status)
            objs = self._get_atoms(rows, idxs, LMServiceType.MATRICES)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listMatrixObjects', first_rec_num, max_num, user_id,
                matrix_type, gcm_code, alt_pred_code, date_code, alg_code,
                metamatch, gridset_id, after_time, before_time, epsg,
                after_status, before_status)
            for row in rows:
                objs.append(self._create_lm_matrix(row, idxs))
        return objs

    # ................................
    def find_or_insert_matrix(self, mtx):
        """Find existing OR save a new Matrix

        Args:
            mtx: the Matrix object to insert

        Returns:
            New or existing Matrix
        """
        meta = mtx.dump_matrix_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertMatrix', mtx.get_id(), mtx.matrix_type,
            mtx.parent_id, mtx.gcm_code, mtx.alt_pred_code, mtx.date_code,
            mtx.algorithm_code, mtx.get_dlocation(), meta, mtx.status,
            mtx.status_mod_time)
        new_or_existing_mtx = self._create_lm_matrix(row, idxs)
        return new_or_existing_mtx

    # ................................
    def count_trees(self, user_id, name, is_binary, is_ultrametric,
                    has_branch_lengths, meta_string, after_time, before_time):
        """Count Trees matching filter conditions

        Args:
            user_id: User (owner) for which to return Trees.
            name: filter by name
            is_binary: filter by boolean binary attribute
            is_ultrametric: filter by boolean ultrametric attribute
            has_branch_lengths: filter by boolean has_branch_lengths attribute
            meta_string: find trees containing this word in the metadata
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time

        Returns:
            A count of Tree
        """
        metamatch = None
        if meta_string is not None:
            metamatch = '%{}%'.format(meta_string)
        row, _ = self.execute_select_one_function(
            'lm_countTrees', user_id, after_time, before_time, name, metamatch,
            is_binary, is_ultrametric, has_branch_lengths)
        return self._get_count(row)

    # ................................
    def list_trees(self, first_rec_num, max_num, user_id, after_time,
                   before_time, name, meta_string, is_binary, is_ultrametric,
                   has_branch_lengths, atom):
        """Return Tree Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return Trees.
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            is_binary: filter by boolean binary attribute
            is_ultrametric: filter by boolean ultrametric attribute
            has_branch_lengths: filter by boolean has_branch_lengths attribute
            name: filter by name
            meta_string: find trees containing this word in the metadata
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of Tree atoms or full objects
        """
        metamatch = None
        if meta_string is not None:
            metamatch = '%{}%'.format(meta_string)
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listTreeAtoms', first_rec_num, max_num, user_id,
                after_time, before_time, name, metamatch, is_binary,
                is_ultrametric, has_branch_lengths)
            objs = self._get_atoms(rows, idxs, LMServiceType.TREES)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listTreeObjects', first_rec_num, max_num, user_id,
                after_time, before_time, name, metamatch, is_binary,
                is_ultrametric, has_branch_lengths)
            for row in rows:
                objs.append(self._create_tree(row, idxs))
        return objs

    # ................................
    def find_or_insert_tree(self, tree):
        """Find existing OR save a new Tree

        Args:
            tree: the Tree object to insert

        Return:
            New or existing Tree
        """
        meta = tree.dump_tree_metadata()
        shrub = tree.get_tree_object()
        is_binary = False
        is_ultrametric = False
        has_branch_lengths = False
        if shrub is not None:
            is_binary = shrub.is_binary()
            is_ultrametric = bool(shrub.is_ultrametric())
            has_branch_lengths = shrub.has_branch_lengths()

        row, idxs = self.execute_insert_and_select_one_function(
            'lm_findOrInsertTree', tree.get_id(), tree.get_user_id(),
            tree.name, tree.get_dlocation(), is_binary,
            is_ultrametric, has_branch_lengths, meta,
            tree.mod_time)
        return self._create_tree(row, idxs)

    # ................................
    def get_tree(self, tree, tree_id):
        """Retrieve a Tree from the database

        Args:
            tree: Tree to retrieve
            tree_id: Database ID of Tree to retrieve

        Returns:
            Existing Tree
        """
        row = None
        if tree is not None:
            row, idxs = self.execute_select_one_function(
                'lm_getTree', tree.get_id(), tree.get_user_id(), tree.name)
        else:
            row, idxs = self.execute_select_one_function(
                'lm_getTree', tree_id, None, None)
        return self._create_tree(row, idxs)

    # ................................
    def insert_mf_chain(self, mf_chain, gridset_id):
        """Inserts a MFChain into database

        Returns:
            Updated MFChain object
        """
        meta = mf_chain.dump_makeflow_metadata()
        row, idxs = self.execute_insert_and_select_one_function(
            'lm_insertMFChain', mf_chain.get_user_id(), gridset_id,
            mf_chain.get_dlocation(), mf_chain.priority, meta, mf_chain.status,
            mf_chain.status_mod_time)
        return self._create_mf_chain(row, idxs)

    # ................................
    def count_mf_chains(self, user_id, gridset_id, meta_string, after_stat,
                        before_stat, after_time, before_time):
        """Return the number of MFChains fitting the given filter conditions

        Args:
            user_id: filter by LMUser
            gridset_id: filter by a Gridset
            meta_string: find gridsets containing this word in the metadata
            after_stat: filter by status >= to this value
            before_stat: filter by status <= to this value
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time

        Returns:
            Count of MFChains fitting the given filter conditions
        """
        metamatch = None
        if meta_string is not None:
            metamatch = '%{}%'.format(meta_string)
        row, _ = self.execute_select_one_function(
            'lm_countMFProcess', user_id, gridset_id, metamatch, after_stat,
            before_stat, after_time, before_time)
        return self._get_count(row)

    # ................................
    def count_priority_mf_chains(self, gridset_id):
        """Return the number of MFChains to run before this gridset

        Args:
            gridset_id: Gridset id for which to check higher priority mfchains

        Returns:
            int - The number of MFChains with higher priority or earlier
                timestamp
        """
        row, _ = self.execute_select_one_function(
            'lm_countMFProcessAhead', gridset_id, JobStatus.COMPLETE)
        return self._get_count(row)

    # ................................
    def list_mf_chains(self, first_rec_num, max_num, user_id, gridset_id,
                       meta_string, after_stat, before_stat, after_time,
                       before_time, atom):
        """Return MFChain Objects or Atoms matching filter conditions

        Args:
            first_rec_num: The first record to return, 0 is the first record
            max_num: Maximum number of records to return
            user_id: User (owner) for which to return shapegrids.
            gridset_id: filter by a Gridset
            meta_string: find gridsets containing this word in the metadata
            after_stat: filter by status >= to this value
            before_stat: filter by status <= to this value
            after_time: filter by modified at or after this time
            before_time: filter by modified at or before this time
            atom: True if return objects will be Atoms, False if full objects

        Returns:
            A list of MFProcess/Gridset atoms or full objects
        """
        if atom:
            rows, idxs = self.execute_select_many_function(
                'lm_listMFProcessAtoms', first_rec_num, max_num, user_id,
                gridset_id, meta_string, after_stat, before_stat, after_time,
                before_time)
            objs = self._get_atoms(rows, idxs, None)
        else:
            objs = []
            rows, idxs = self.execute_select_many_function(
                'lm_listMFProcessObjects', first_rec_num, max_num, user_id,
                gridset_id, meta_string, after_stat, before_stat, after_time,
                before_time)
            for row in rows:
                objs.append(self._create_mf_chain(row, idxs))
        return objs

    # ................................
    def summarize_mf_chains_for_gridset(self, gridset_id):
        """Count all mfprocesses for a gridset by status

        Args:
            gridset_id: a database ID for the LmServer.legion.Gridset

        Returns:
            A list of tuples containing count, status
        """
        status_total_pairs = []
        rows, idxs = self.execute_select_many_function(
            'lm_summarizeMFProcessForGridset', gridset_id)
        for row in rows:
            status_total_pairs.append(
                (row[idxs['status']], row[idxs['total']]))
        return status_total_pairs

    # ................................
    def find_mf_chains(self, count, user_id, old_status, new_status):
        """Retrieves MFChains from database

        Args:
            count: Number of MFChains to pull
            user_id: If not None, filter by this user
            old_status: Pull only MFChains at this status
            new_status: Update MFChains to this status

        Returns:
            List of MFChains
        """
        mf_chains = []
        mod_time = gmt().mjd
        rows, idxs = self.execute_select_many_function(
            'lm_findMFChains', count, user_id, old_status, new_status,
            mod_time)
        for row in rows:
            mf_chain = self._create_mf_chain(row, idxs)
            mf_chains.append(mf_chain)
        return mf_chains

    # ................................
    def delete_mf_chains_return_filenames(self, gridset_id):
        """Deletes MFChains for a gridset, returns filenames

        Args:
            gridset_id: Pull only MFChains for this gridset

        Returns:
            List of MFChains filenames
        """
        flist = []
        rows, _ = self.execute_select_and_modify_many_function(
            'lm_deleteMFChainsForGridsetReturnFilenames', gridset_id)
        for row in rows:
            fname = row[0]
            flist.append(fname)
        return flist

    # ................................
    def get_mf_chain(self, mf_process_id):
        """Retrieves MFChain from database

        Args:
            mf_process_id: Database ID of MFChain to pull

        Returns:
            LmServer.legion.process_chain.MFChains
        """
        row, idxs = self.execute_select_many_function(
            'lm_getMFChain', mf_process_id)
        return self._create_mf_chain(row, idxs)

    # ................................
    def update_object(self, obj):
        """Updates object in database

        Returns:
            True/False for success of operation
        """
        if isinstance(obj, OccurrenceLayer):
            success = self.update_occurrence_set(obj)
        elif isinstance(obj, SDMProjection):
            success = self.update_sdm_project(obj)
        elif isinstance(obj, Shapegrid):
            success = self.update_shapegrid(obj)
        # TODO: Handle if MatrixColumn changes to inherit from LMMatrix
        elif isinstance(obj, MatrixColumn):
            success = self.update_matrix_column(obj)
        elif isinstance(obj, LMMatrix):
            success = self.update_matrix(obj)
        elif isinstance(obj, ScientificName):
            success = self.update_taxon(obj)
        elif isinstance(obj, Tree):
            meta = obj.dump_tree_metadata()
            shrub = obj.get_tree_object()
            is_binary = False
            is_ultrametric = False
            has_branch_lengths = False
            if shrub is not None:
                is_binary = shrub.is_binary()
                is_ultrametric = bool(shrub.is_ultrametric())
                has_branch_lengths = shrub.has_branch_lengths()

            success = self.execute_modify_function(
                'lm_updateTree', obj.get_id(), obj.get_dlocation(),
                is_binary, is_ultrametric, has_branch_lengths, meta,
                obj.mod_time)
        elif isinstance(obj, MFChain):
            success = self.execute_modify_function(
                'lm_updateMFChain', obj.obj_id, obj.get_dlocation(),
                obj.status, obj.status_mod_time)
        elif isinstance(obj, Gridset):
            success = self.update_gridset(obj)
        else:
            raise LMError('Unsupported update for object {}'.format(type(obj)))
        return success

    # ................................
    def delete_object(self, obj):
        """Deletes object from database

        Returns:
            True/False for success of operation

        Note:
            - OccurrenceSet delete cascades to SDMProject but not MatrixColumn
            - MatrixColumns for Global PAM should be deleted or reset on
                OccurrenceSet delete or recalc
        """
        try:
            obj_id = obj.get_id()
        except AttributeError:
            try:
                obj = obj.obj_id
            except AttributeError:
                raise LMError(
                    'Failed getting ID for {} object'.format(type(obj)))
        if isinstance(obj, MFChain):
            success = self.execute_modify_function('lm_deleteMFChain', obj_id)
        elif isinstance(obj, OccurrenceLayer):
            success = self.delete_occurrence_set(obj)
        elif isinstance(obj, SDMProjection):
            success = self.execute_modify_function(
                'lm_deleteSDMProjectLayer', obj_id)
        elif isinstance(obj, Shapegrid):
            success = self.execute_modify_function(
                'lm_deleteShapeGrid', obj_id)
        elif isinstance(obj, Scenario):
            # Deletes ScenarioLayer join; only deletes layers if they are
            #    orphaned
            for lyr in obj.layers:
                success = self.execute_modify_function(
                    'lm_deleteScenarioLayer', lyr.get_id(), obj_id)
            success = self.execute_modify_function('lm_deleteScenario', obj_id)
        elif isinstance(obj, MatrixColumn):
            success = self.execute_modify_function(
                'lm_deleteMatrixColumn', obj_id)
        elif isinstance(obj, Tree):
            success = self.execute_modify_function('lm_deleteTree', obj_id)
        else:
            raise LMError('Unsupported delete for object {}'.format(type(obj)))
        return success

    # ................................
    def get_matrices_for_gridset(self, gridset_id, mtx_type):
        """Return all LMMatrix objects that are part of a gridset

        Args:
            gridset_id: Id of the gridset organizing these data matrices
            mtx_type: optional filter, LmCommon.common.lmconstants.MatrixType
                for one type of LMMatrix
        """
        mtxs = []
        rows, idxs = self.execute_select_many_function(
            'lm_getMatricesForGridset', gridset_id, mtx_type)
        for row in rows:
            mtxs.append(self._create_lm_matrix(row, idxs))
        return mtxs
