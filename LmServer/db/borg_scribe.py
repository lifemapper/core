# coding=utf-8
"""Module containing functions for database access
"""
import socket

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import LMFormat, ProcessType
from LmCommon.common.time import gmt
from LmServer.base.layerset import MapLayerSet
from LmServer.base.taxon import ScientificName
from LmServer.common.data_locator import EarlJr
from LmServer.common.lmconstants import (
    DbUser, JobStatus, FileFix, LMFileType, MatrixType, NAME_SEPARATOR)
from LmServer.common.localconstants import (CONNECTION_PORT, DB_HOSTNAME,
                                            PUBLIC_USER)
from LmServer.db.catalog_borg import Borg
from LmServer.db.connect import HL_NAME
from LmServer.legion.env_layer import EnvLayer, EnvType
from LmServer.legion.mtx_column import MatrixColumn
from LmServer.legion.sdm_proj import SDMProjection


# .............................................................................
class BorgScribe(LMObject):
    """Class for interacting with the Lifemapper database."""
    # ................................
    def __init__(self, logger, db_user=DbUser.Pipeline):
        """Constructor

        Args:
            logger (LmLogger): A logger object for info and error reporting
            db_user (str): Database user for connection
        """
#         LMObject.__init__(self)
        self.log = logger
        self.hostname = socket.gethostname().lower()
        db_host = DB_HOSTNAME

        if db_user not in list(HL_NAME.keys()):
            raise LMError('Unknown database user {}'.format(db_user))

        self._borg = Borg(
            logger, db_host, CONNECTION_PORT, db_user, HL_NAME[db_user])

    # ................................
    @property
    def is_open(self):
        """Return boolean indicating if the database connection is open."""
        return self._borg.is_open

    # ................................
    def open_connections(self):
        """Open database connection."""
        try:
            self._borg.open()
        except Exception as e:
            self.log.error(
                'Failed to open Borg {}: {}'.format(
                    '(user={} dbname={} host={} port={})'.format(
                        self._borg.user, self._borg.db, self._borg.host,
                        self._borg.port), e.args))
            return False
        return True

    # ................................
    def close_connections(self):
        """Close database connections."""
        self._borg.close()

    # ................................
    def find_or_insert_algorithm(self, alg, mod_time=None):
        """Find or insert an algorithm into the database.

        Args:
            alg: LmServer.legion.algorithm object to insert
            mod_time: date/time in MJD
        """
        algo = self._borg.find_or_insert_algorithm(alg, mod_time)
        return algo

    # ................................
    def count_job_chains(self, status, user_ids=None):
        """Count the number of job chains at the specified status.

        Args:
            status (int): Return the number of job chains with this status
            user_ids (None or list of str): An optional list of user ids to
                count matching job chains for
        """
        total = 0
        if not user_ids:
            user_ids = [None]
        for usr in user_ids:
            total += self._borg.count_job_chains(status, usr)
        return total

    # ................................
    def find_or_insert_env_layer(self, lyr, scenario_id=None):
        """Find or insert a layer's metadata in the database and optionally
        join it to the indicated scenario.

        Args:
            lyr: layer to insert
            scenario_id: database id for scenario if joining layer

        Returns:
            New or existing EnvironmentalLayer
        """
        updated_lyr = None
        if isinstance(lyr, EnvLayer):
            if lyr.is_valid_dataset():
                updated_lyr = self._borg.find_or_insert_env_layer(
                    lyr, scenario_id)
            else:
                raise LMError(
                    'Invalid environmental layer: {}'.format(
                        lyr.get_dlocation()), line_num=self.get_line_num())
        return updated_lyr

    # ................................
    def find_or_insert_layer(self, lyr):
        """Find or insert a layer object.

        Args:
            lyr: _Layer object to find or insert into the database

        Return:
            updated layer object
        """
        return self._borg.find_or_insert_layer(lyr)

    # ................................
    def get_env_layer(self, env_lyr_id=None, lyr_id=None, lyr_verify=None,
                      user_id=None, lyr_name=None, epsg=None):
        """Get an environmental layer object."""
        return self._borg.get_env_layer(
            env_lyr_id, lyr_id, lyr_verify, user_id, lyr_name, epsg)

    # ................................
    def delete_scenario_layer(self, env_lyr, scenario_id):
        """Delete the scenario layer join from the database.

        Note:
            This deletes the join only, not the EnvLayer
        """
        return self._borg.delete_scenario_layer(env_lyr, scenario_id)

    # ................................
    def delete_env_layer(self, env_lyr, scenario_id=None):
        """Delete an environmental layer from the database."""
        if scenario_id is not None:
            _ = self.delete_scenario_layer(env_lyr, scenario_id=scenario_id)
        return self._borg.delete_env_layer(env_lyr)

    # ................................
    def count_env_layers(self, user_id=PUBLIC_USER, env_code=None,
                         gcm_code=None, alt_pred_code=None, date_code=None,
                         after_time=None, before_time=None, epsg=None,
                         env_type_id=None, scenario_code=None):
        """Count the number of environmental layers that match the criteria."""
        return self._borg.count_env_layers(
            user_id, env_code, gcm_code, alt_pred_code, date_code, after_time,
            before_time, epsg, env_type_id, scenario_code)

    # ................................
    def list_env_layers(self, first_rec_num, max_num, user_id=PUBLIC_USER,
                        env_code=None, gcm_code=None, alt_pred_code=None,
                        date_code=None, after_time=None, before_time=None,
                        epsg=None, env_type_id=None, scen_code=None,
                        atom=True):
        """Return a list of environmental layers matching the criteria."""
        return self._borg.list_env_layers(
            first_rec_num, max_num, user_id, env_code, gcm_code, alt_pred_code,
            date_code, after_time, before_time, epsg, env_type_id, scen_code,
            atom)

    # ................................
    def find_or_insert_env_type(self, env_type):
        """Find or insert an environmental layer type"""
        if isinstance(env_type, EnvType):
            return self._borg.find_or_insert_env_type(env_type=env_type)

        raise LMError('Invalid object for EnvType insertion')

    # ................................
    def count_scen_packages(self, user_id=PUBLIC_USER, after_time=None,
                            before_time=None, epsg=None, scen_id=None):
        """Count the scenario packages that match the specified criteria."""
        return self._borg.count_scen_packages(
            user_id, after_time, before_time, epsg, scen_id)

    # ................................
    def list_scen_packages(self, first_rec_num, max_num, user_id=PUBLIC_USER,
                           after_time=None, before_time=None, epsg=None,
                           scen_id=None, atom=True):
        """List scenario packages that match the specified criteria.
        """
        return self._borg.list_scen_packages(
            first_rec_num, max_num, user_id, after_time, before_time, epsg,
            scen_id, atom)

    # ................................
    def find_or_insert_scen_package(self, scen_package):
        """Find or insert a scenario package into the database.

        Note:
            This returns the updated ScenPackage **without** Scenarios and
                Layers
        """
        return self._borg.find_or_insert_scen_package(scen_package)

    # ................................
    def get_scen_packages_for_scenario(self, scen=None, scen_id=None,
                                       user_id=None, scen_code=None,
                                       fill_layers=False):
        """Get scenario packages that include the specified scenario."""
        return self._borg.get_scen_packages_for_scenario(
            scen, scen_id, user_id, scen_code, fill_layers)

    # ................................
    def get_scenarios_for_scen_package(self, scen_package=None,
                                       scen_package_id=None, user_id=None,
                                       scen_package_name=None):
        """Get scenarios in the specified scenario package."""
        return self._borg.get_scenarios_for_scen_package(
            scen_package, scen_package_id, user_id, scen_package_name, False)

    # ................................
    def get_scen_packages_for_user_codes(self, usr, scen_codes,
                                         fill_layers=False):
        """Get scenario packages for the specified user scenario codes.

        Note:
            This returns all ScenPackages containing this scenario.  All
                ScenPackages are filled with Scenarios.
        """
        scen_packages = []
        if scen_codes:
            first_code = scen_codes[0]
            new_list = scen_codes[1:]
            first_pkgs = self.get_scen_packages_for_scenario(
                user_id=usr, scen_code=first_code, fill_layers=fill_layers)
            for pkg in first_pkgs:
                bad_match = False
                for code in new_list:
                    found_scen = pkg.get_scenario(code=code)
                    if not found_scen:
                        bad_match = True
                        break
                if not bad_match:
                    scen_packages.append(pkg)
        return scen_packages

    # ................................
    def get_scen_package(self, scen_package=None, scen_package_id=None,
                         user_id=None, scen_package_name=None,
                         fill_layers=False):
        """Return a scenario package."""
        return self._borg.get_scen_package(
            scen_package, scen_package_id, user_id, scen_package_name,
            fill_layers)

    # ................................
    def find_or_insert_scenario(self, scen, scen_package_id=None):
        """Find or insert a scenario into the database.

        Note:
            This returns the updated Scenario filled with Layers
        """
        updated_scen = self._borg.find_or_insert_scenario(
            scen, scen_package_id)
        scen_id = updated_scen.get_id()
        for lyr in scen.layers:
            updated_lyr = self.find_or_insert_env_layer(lyr, scen_id)
            updated_scen.add_layer(updated_lyr)
        return updated_scen

    # ................................
    def delete_computed_user_data(self, user_id):
        """Delete computed user data."""
        return self._borg.delete_computed_user_data(user_id)

    # ................................
    def clear_user(self, user_id):
        """Clear all data for a user."""
        mtx_col_ids = []
        grid_ids = self._borg.find_user_gridsets(user_id)
        for grid_id in grid_ids:
            mtx_col_ids.extend(
                self._borg.delete_gridset_return_mtx_col_ids(grid_id))

        success = self._borg.clear_user(user_id)
        return success, mtx_col_ids

    # ................................
    def find_or_insert_user(self, usr):
        """Find or insert a user into the database."""
        return self._borg.find_or_insert_user(usr)

    # ................................
    def update_user(self, usr):
        """Update a user in the database."""
        return self._borg.update_user(usr)

    # ................................
    def find_user(self, user_id=None, email=None):
        """Find a user in the database by user id or email."""
        return self._borg.find_user(user_id, email)

    # ................................
    def find_user_for_object(self, layer_id=None, scen_code=None, occ_id=None,
                             matrix_id=None, gridset_id=None,
                             mf_process_id=None):
        """Find the user for a specified object."""
        return self._borg.find_user_for_object(
            layer_id, scen_code, occ_id, matrix_id, gridset_id, mf_process_id)

    # ................................
    def find_or_insert_taxon_source(self, tax_source_name, tax_source_url):
        """Find or insert a taxon source into the database."""
        return self._borg.find_or_insert_taxon_source(
            tax_source_name, tax_source_url)

    # ................................
    def find_or_insert_shapegrid(self, shapegrid, cutout=None):
        """Find or insert a shapegrid into the database."""
        return self._borg.find_or_insert_shapegrid(shapegrid, cutout)

    # ................................
    def find_or_insert_gridset(self, gridset):
        """Find or insert a gridset into the database."""
        return self._borg.find_or_insert_gridset(gridset)

    # ................................
    def find_or_insert_matrix(self, mtx):
        """Find or insert a matrix into the database."""
        return self._borg.find_or_insert_matrix(mtx)

    # ................................
    def get_shapegrid(self, lyr_id=None, user_id=None, lyr_name=None,
                      epsg=None):
        """Get a shapegrid from the database."""
        return self._borg.get_shapegrid(lyr_id, user_id, lyr_name, epsg)

    # ................................
    def count_shapegrids(self, user_id=PUBLIC_USER, cell_sides=None,
                         cell_size=None, after_time=None, before_time=None,
                         epsg=None):
        """Count the number of shapegrids that match the specified criteria."""
        return self._borg.count_shapegrids(
            user_id, cell_sides, cell_size, after_time, before_time, epsg)

    # ................................
    def list_shapegrids(self, first_rec_num, max_num, user_id=PUBLIC_USER,
                        cell_sides=None, cell_size=None, after_time=None,
                        before_time=None, epsg=None, atom=True):
        """Return a list of shapegrids matching the specified criteria."""
        return self._borg.list_shapegrids(
            first_rec_num, max_num, user_id, cell_sides, cell_size, after_time,
            before_time, epsg, atom)

    # ................................
    def get_layer(self, lyr_id=None, lyr_verify=None, user_id=None,
                  lyr_name=None, epsg=None):
        """Get a layer object from the database.
        """
        return self._borg.get_base_layer(
            lyr_id, lyr_verify, user_id, lyr_name, epsg)

    # ................................
    def count_layers(self, user_id=PUBLIC_USER, squid=None, after_time=None,
                     before_time=None, epsg=None):
        """Count layers matching the specified criteria."""
        return self._borg.count_layers(
            user_id, squid, after_time, before_time, epsg)

    # ................................
    def list_layers(self, first_rec_num, max_num, user_id=PUBLIC_USER,
                    squid=None, after_time=None, before_time=None, epsg=None,
                    atom=True):
        """Return a list of layers matching the specified criteria."""
        return self._borg.list_layers(
            first_rec_num, max_num, user_id, squid, after_time, before_time,
            epsg, atom)

    # ................................
    def get_matrix_column(self, mtx_col=None, mtx_col_id=None):
        """Get the specified matrix column."""
        return self._borg.get_matrix_column(mtx_col, mtx_col_id)

    # ................................
    def get_columns_for_matrix(self, mtx_id):
        """Get matrix columns for the specified matrix."""
        return self._borg.get_columns_for_matrix(mtx_id)

    # ................................
    def get_sdm_columns_for_matrix(self, mtx_id, return_columns=True,
                                   return_projections=True):
        """Get SDM-based matrix columns for the specified matrix."""
        return self._borg.get_sdm_columns_for_matrix(
            mtx_id, return_columns, return_projections)

    # ................................
    def get_occ_layers_for_matrix(self, mtx_id):
        """Get occurrence layers for the specified matrix.."""
        return self._borg.get_occ_layers_for_matrix(mtx_id)

    # ................................
    def count_matrix_columns(self, user_id=None, squid=None, ident=None,
                             after_time=None, before_time=None, epsg=None,
                             after_status=None, before_status=None,
                             gridset_id=None, matrix_id=None, layer_id=None):
        """Count matrix columns that match the specified criteria.
        """
        return self._borg.count_matrix_columns(
            user_id, squid, ident, after_time, before_time, epsg, after_status,
            before_status, gridset_id, matrix_id, layer_id)

    # ................................
    def list_matrix_columns(self, first_rec_num, max_num, user_id=None,
                            squid=None, ident=None, after_time=None,
                            before_time=None, epsg=None, after_status=None,
                            before_status=None, gridset_id=None,
                            matrix_id=None, layer_id=None, atom=True):
        """Return a list of matrix columns matching the specified criteria.
        """
        return self._borg.list_matrix_columns(
            first_rec_num, max_num, user_id, squid, ident, after_time,
            before_time, epsg, after_status, before_status, gridset_id,
            matrix_id, layer_id, atom)

    # ................................
    def get_matrix(self, mtx=None, mtx_id=None, gridset_id=None,
                   gridset_name=None, user_id=None, mtx_type=None,
                   gcm_code=None, alt_pred_code=None, date_code=None,
                   alg_code=None):
        """Get a matrix object from the database.

        Args:
            mtx: A LMMatrix object containing the unique parameters for which
                to retrieve the existing LMMatrix

        Note:
            If mtx parameter is present, it overrides individual parameters
        """
        if mtx is not None:
            mtx_id = mtx.get_id()
            mtx_type = mtx.matrix_type
            gridset_id = mtx.parent_id
            gcm_code = mtx.gcm_code
            alt_pred_code = mtx.alt_pred_code
            date_code = mtx.date_code
            alg_code = mtx.algorithm_code
        return self._borg.get_matrix(
            mtx_id, gridset_id, gridset_name, user_id, mtx_type, gcm_code,
            alt_pred_code, date_code, alg_code)

    # ................................
    def count_matrices(self, user_id=None, matrix_type=None,
                       gcm_code=None, alt_pred_code=None, date_code=None,
                       alg_code=None, keyword=None, gridset_id=None,
                       after_time=None, before_time=None, epsg=None,
                       after_status=None, before_status=None):
        """Count matrices that match the specified criteria.
        """
        return self._borg.count_matrices(
            user_id, matrix_type, gcm_code, alt_pred_code, date_code, alg_code,
            keyword, gridset_id, after_time, before_time, epsg, after_status,
            before_status)

    # ................................
    def list_matrices(self, first_rec_num, max_num, user_id=None,
                      matrix_type=None, gcm_code=None, alt_pred_code=None,
                      date_code=None, alg_code=None, keyword=None,
                      gridset_id=None, after_time=None, before_time=None,
                      epsg=None, after_status=None, before_status=None,
                      atom=True):
        """List matrices that match the specified criteria.
        """
        return self._borg.list_matrices(
            first_rec_num, max_num, user_id, matrix_type, gcm_code,
            alt_pred_code, date_code, alg_code, keyword, gridset_id,
            after_time, before_time, epsg, after_status, before_status, atom)

    # ................................
    def find_or_insert_tree(self, tree):
        """Find or insert a tree into the database.
        """
        return self._borg.find_or_insert_tree(tree)

    # ................................
    def get_tree(self, tree=None, tree_id=None):
        """Get a tree from the database.
        """
        existing_tree = self._borg.get_tree(tree, tree_id)
        if existing_tree and existing_tree.get_dlocation() is None:
            existing_tree.set_dlocation()
        return existing_tree

    # ................................
    def count_trees(self, user_id=PUBLIC_USER, name=None, is_binary=None,
                    is_ultrametric=None, has_branch_lengths=None,
                    meta_string=None, after_time=None, before_time=None):
        """Count trees matching the specified criteria.
        """
        return self._borg.count_trees(
            user_id, name, is_binary, is_ultrametric, has_branch_lengths,
            meta_string, after_time, before_time)

    # ................................
    def list_trees(self, first_rec_num, max_num, user_id=PUBLIC_USER,
                   name=None, is_binary=None, is_ultrametric=None,
                   has_branch_lengths=None, meta_string=None, after_time=None,
                   before_time=None, atom=True):
        """List trees matching the specified criteria.
        """
        return self._borg.list_trees(
            first_rec_num, max_num, user_id, after_time, before_time, name,
            meta_string, is_binary, is_ultrametric, has_branch_lengths, atom)

    # ................................
    def get_gridset(self, gridset=None, gridset_id=None, user_id=None,
                    name=None, fill_matrices=False):
        """Get a gridset from the database.

        Note:
            Gridset object values override gridset_id, user_id, name
        """
        if gridset is not None:
            gridset_id = gridset.get_id()
            user_id = gridset.get_user_id()
            name = gridset.name
        return self._borg.get_gridset(gridset_id, user_id, name, fill_matrices)

    # ................................
    def count_gridsets(self, user_id, shapegrid_layer_id=None,
                       meta_string=None, after_time=None, before_time=None,
                       epsg=None):
        """Count the number of gridsets matching the specified criteria.
        """
        return self._borg.count_gridsets(
            user_id, shapegrid_layer_id, meta_string, after_time, before_time,
            epsg)

    # ................................
    def list_gridsets(self, first_rec_num, max_num, user_id=PUBLIC_USER,
                      shapegrid_layer_id=None, meta_string=None,
                      after_time=None, before_time=None, epsg=None, atom=True):
        """List gridsets matching the specified criteria.
        """
        return self._borg.list_gridsets(
            first_rec_num, max_num, user_id, shapegrid_layer_id, meta_string,
            after_time, before_time, epsg, atom)

    # ................................
    def find_taxon_source(self, taxon_source_name):
        """Get a taxon source from the database with the specified name.
        """
        return self._borg.find_taxon_source(taxon_source_name)

    # ................................
    def get_taxon_source(self, tax_source_id=None, tax_source_name=None,
                         tax_source_url=None):
        """Get a taxon source
        """
        return self._borg.get_taxon_source(
            tax_source_id, tax_source_name, tax_source_url)

    # ................................
    def find_or_insert_taxon(self, taxon_source_id=None, taxon_key=None,
                             sci_name=None):
        """Find or insert a ScientificName object into the database."""
        return self._borg.find_or_insert_taxon(
            taxon_source_id, taxon_key, sci_name)

    # ................................
    def get_taxon(self, squid=None, taxon_source_id=None, taxon_key=None,
                  user_id=None, taxon_name=None):
        """Get a ScientificName object from the database.
        """
        return self._borg.get_taxon(
            squid, taxon_source_id, taxon_key, user_id, taxon_name)

    # ................................
    def get_scenario(self, id_or_code, user_id=None, fill_layers=False):
        """Get a scenario from the database.
        """
        sid = code = None
        try:
            sid = int(id_or_code)
        except ValueError:
            code = id_or_code
        return self._borg.get_scenario(
            scen_id=sid, code=code, user_id=user_id,
            fill_layers=fill_layers)

    # ................................
    def count_scenarios(self, user_id=PUBLIC_USER, after_time=None,
                        before_time=None, epsg=None, gcm_code=None,
                        alt_pred_code=None, date_code=None,
                        scen_package_id=None):
        """Count scenarios matching the specified criteria.
        """
        return self._borg.count_scenarios(
            user_id, after_time, before_time, epsg, gcm_code, alt_pred_code,
            date_code, scen_package_id)

    # ................................
    def list_scenarios(self, first_rec_num, max_num, user_id=PUBLIC_USER,
                       after_time=None, before_time=None, epsg=None,
                       gcm_code=None, alt_pred_code=None, date_code=None,
                       scen_package_id=None, atom=True):
        """Return a list of scenarios that match the specified criteria.
        """
        return self._borg.list_scenarios(
            first_rec_num, max_num, user_id, after_time, before_time, epsg,
            gcm_code, alt_pred_code, date_code, scen_package_id, atom)

    # ................................
    def get_occurrence_set(self, occ_id=None, squid=None, user_id=None,
                           epsg=None):
        """Get an occurrence set from the database.
        """
        return self._borg.get_occurrence_set(occ_id, squid, user_id, epsg)

    # ................................
    def get_occurrence_sets_for_name(self, sci_name_str, user_id):
        """Get a list of occurrencesets for the given squid and user

        Args:
            sci_name_str: a string associated with a ScientificName
            user_id: the database primary key of the LMUser
        """
        sci_name = ScientificName(sci_name_str, user_id=user_id)
        updated_sci_name = self.find_or_insert_taxon(sci_name=sci_name)
        occ_sets = self._borg.get_occurrence_sets_for_squid(
            updated_sci_name.squid, user_id)
        return occ_sets

    # ................................
    def find_or_insert_occurrence_set(self, occ):
        """Find or insert an occurrence set into the database.

        Args:
            occ: The occurrence set to look for and / or save.

        Note:
            Updates db with count, the actual count on the object (likely zero
                on initial insertion)
        """
        return self._borg.find_or_insert_occurrence_set(occ)

    # ................................
    def count_occurrence_sets(self, user_id=None, squid=None,
                              min_occurrence_count=None, display_name=None,
                              after_time=None, before_time=None, epsg=None,
                              after_status=None, before_status=None,
                              gridset_id=None):
        """Count occurrence sets that match the specified criteria.
        """
        return self._borg.count_occurrence_sets(
            user_id, squid, min_occurrence_count, display_name, after_time,
            before_time, epsg, after_status, before_status, gridset_id)

    # ................................
    def list_occurrence_sets(self, first_rec_num, max_num, user_id=None,
                             squid=None, min_occurrence_count=None,
                             display_name=None, after_time=None,
                             before_time=None, epsg=None, after_status=None,
                             before_status=None, gridset_id=None, atom=True):
        """List occurrence sets matching the specified criteria.
        """
        return self._borg.list_occurrence_sets(
            first_rec_num, max_num, user_id, squid, min_occurrence_count,
            display_name, after_time, before_time, epsg, after_status,
            before_status, gridset_id, atom)

    # ................................
    def summarize_occurrence_sets_for_gridset(self, gridset_id):
        """Summarize occurrence sets within a gridset.
        """
        return self._borg.summarize_occurrence_sets_for_gridset(gridset_id)

    # ................................
    def get_sdm_project(self, layer_id):
        """Get an SDM Projection.
        """
        return self._borg.get_sdm_project(layer_id)

    # ................................
    def find_or_insert_sdm_project(self, proj):
        """Find or insert an SDM projection.
        """
        return self._borg.find_or_insert_sdm_project(proj)

    # ................................
    def count_sdm_projects(self, user_id=None, squid=None, display_name=None,
                           after_time=None, before_time=None, epsg=None,
                           after_status=None, before_status=None,
                           occ_set_id=None, alg_code=None, mdl_scen_code=None,
                           prj_scen_code=None, gridset_id=None):
        """Count SDM Projections matching the specified criteria.
        """
        if user_id is None and gridset_id is None:
            raise LMError('Must provide either user_id or gridset_id')
        return self._borg.count_sdm_projects(
            user_id, squid, display_name, after_time, before_time, epsg,
            after_status, before_status, occ_set_id, alg_code, mdl_scen_code,
            prj_scen_code, gridset_id)

    # ................................
    def summarize_sdm_projects_for_gridset(self, gridset_id):
        """Summarize SDM projections included in a gridset.
        """
        return self._borg.summarize_sdm_projects_for_gridset(gridset_id)

    # ................................
    def summarize_matrices_for_gridset(self, gridset_id, mtx_type=None):
        """Summarize matrices included in a gridset.
        """
        return self._borg.summarize_matrices_for_gridset(gridset_id, mtx_type)

    # ................................
    def summarize_mtx_columns_for_gridset(self, gridset_id, mtx_type=None):
        """Summarize matrix columns included within a gridset.
        """
        return self._borg.summarize_mtx_columns_for_gridset(
            gridset_id, mtx_type)

    # ................................
    def list_sdm_projects(self, first_rec_num, max_num, user_id=None,
                          squid=None, display_name=None, after_time=None,
                          before_time=None, epsg=None, after_status=None,
                          before_status=None, occ_set_id=None, alg_code=None,
                          mdl_scen_code=None, prj_scen_code=None,
                          gridset_id=None, atom=True):
        """List SDM projections matching the provided criteria.
        """
        if user_id is None and gridset_id is None:
            raise LMError('Must provide either user_id or gridset_id')
        return self._borg.list_sdm_projects(
            first_rec_num, max_num, user_id, squid, display_name, after_time,
            before_time, epsg, after_status, before_status, occ_set_id,
            alg_code, mdl_scen_code, prj_scen_code, gridset_id, atom)

    # ................................
    def find_or_insert_matrix_column(self, mtx_col):
        """Find or insert a matrix column into the database.

        Args:
            mtx_col: The MatrixColumn object to get or insert.
        """
        return self._borg.find_or_insert_matrix_column(mtx_col)

    # ................................
    def init_or_rollback_intersect(self, lyr, mtx, intersect_params, mod_time):
        """Initialize model, projections for inputs/algorithm.
        """
        new_or_existing_mtx_col = None
        if mtx is not None and mtx.get_id() is not None:
            # TODO: Save this into the DB??
            if lyr.data_format in LMFormat.gdal_drivers():
                if mtx.matrix_type in (MatrixType.PAM, MatrixType.ROLLING_PAM):
                    p_type = ProcessType.INTERSECT_RASTER
                else:
                    p_type = ProcessType.INTERSECT_RASTER_GRIM
            else:
                p_type = ProcessType.INTERSECT_VECTOR

            mtx_col = MatrixColumn(
                None, mtx.get_id(), mtx.get_user_id(), layer=lyr,
                shapegrid=None, intersect_params=intersect_params,
                squid=lyr.squid, ident=lyr.ident, process_type=p_type,
                metadata={}, matrix_column_id=None, status=JobStatus.GENERAL,
                status_mod_time=mod_time)
            new_or_existing_mtx_col = self._borg.find_or_insert_matrix_column(
                mtx_col)
            # Reset process_type (not in db)
            new_or_existing_mtx_col.process_type = p_type

            if JobStatus.finished(new_or_existing_mtx_col.status):
                new_or_existing_mtx_col.update_status(
                    JobStatus.GENERAL, mod_time=mod_time)
                _success = self._borg.update_matrix_column(
                    new_or_existing_mtx_col)
        return new_or_existing_mtx_col

    # ................................
    def init_or_rollback_sdm_projects(self, occ, mdl_scen, proj_scens, alg,
                                      mod_time=gmt().mjd, email=None):
        """Initialize or rollback SDM projections.

        Args:
            occ: OccurrenceSet for which to initialize or rollback all
                dependent objects
            mdl_scen: Scenario for SDM model computations
            prj_scens: Scenarios for SDM project computations
            alg: List of algorithm objects for SDM computations on this
                             OccurrenceSet
            mod_time: timestamp of modification, in MJD format
            email: email address for notifications
        """
        prjs = []
        for proj_scen in proj_scens:
            prj = SDMProjection(
                occ, alg, mdl_scen, proj_scen,
                data_format=LMFormat.get_default_gdal().driver,
                status=JobStatus.GENERAL, status_mod_time=mod_time)
            new_or_existing_prj = self._borg.find_or_insert_sdm_project(prj)
            # Instead of re-pulling unchanged scenario layers, update
            # with input arguments
            new_or_existing_prj._model_scenario = mdl_scen
            new_or_existing_prj._proj_scenario = proj_scens
            # Rollback if finished
            if JobStatus.finished(new_or_existing_prj.status):
                new_or_existing_prj.update_status(
                    JobStatus.GENERAL, mod_time=mod_time)
                new_or_existing_prj = self.update_sdm_project(
                    new_or_existing_prj)

            prjs.append(new_or_existing_prj)
        return prjs

    # ................................
    def init_or_rollback_sdm_chain(self, occ, algorithms, mdl_scen, prj_scens,
                                   gridset=None, intersect_params=None,
                                   min_point_count=None):
        """Initialize or rollback existing SDM chain.

        Args:
            occ: OccurrenceSet for which to initialize or rollback all
                dependent objects
            algorithms: List of algorithm objects for SDM computations on this
                OccurrenceSet
            mdl_scen: Scenario for SDM model computations
            prj_scens: Scenarios for SDM project computations
            gridset: Gridset containing Global PAM for output of intersections
            min_point_count: Minimum number of points required for SDM
        """
        objs = [occ]
        curr_time = gmt().mjd
        # ........................
        if (min_point_count is None or occ.query_count is None or
                occ.query_count >= min_point_count):
            for alg in algorithms:
                prjs = self.init_or_rollback_sdm_projects(
                    occ, mdl_scen, prj_scens, alg, mod_time=curr_time)
                objs.extend(prjs)
                # Intersect if intersectGrid is provided
                if gridset is not None and gridset.pam is not None:
                    mtx_cols = []
                    for prj in prjs:
                        mtx_col = self.init_or_rollback_intersect(
                            prj, gridset.pam, intersect_params, curr_time)
                        mtx_cols.append(mtx_col)
                    objs.extend(mtx_cols)
        return objs

    # ................................
    def insert_mf_chain(self, mf_chain, gridset_id):
        """Insert a makeflow process chain into the database
        """
        return self._borg.insert_mf_chain(mf_chain, gridset_id)

    # ................................
    def get_mf_chain(self, mf_process_id):
        """Get a specified makeflow process chain
        """
        return self._borg.get_mf_chain(mf_process_id)

    # ................................
    def find_mf_chains(self, count, user_id=None):
        """Find makeflow chains to run
        """
        return self._borg.find_mf_chains(
            count, user_id, JobStatus.INITIALIZE, JobStatus.PULL_REQUESTED)

    # ................................
    def delete_mf_chains_return_filenames(self, gridset_id):
        """Delete makeflows and return filenames
        """
        return self._borg.delete_mf_chains_return_filenames(gridset_id)

    # ................................
    def count_priority_mf_chains(self, gridset_id):
        """Count makeflows with higher priority than those for the gridset
        """
        count = self._borg.count_priority_mf_chains(gridset_id)
        return count

    # ................................
    def count_mf_chains(self, user_id=None, gridset_id=None, meta_string=None,
                        after_stat=None, before_stat=None, after_time=None,
                        before_time=None):
        """Count makeflow process chains matching the specified criteria
        """
        return self._borg.count_mf_chains(
            user_id, gridset_id, meta_string, after_stat, before_stat,
            after_time, before_time)

    # ................................
    def list_mf_chains(self, first_rec_num, max_num, user_id=None,
                       gridset_id=None, meta_string=None, after_stat=None,
                       before_stat=None, after_time=None, before_time=None,
                       atom=True):
        """List makeflow chains
        """
        return self._borg.list_mf_chains(
            first_rec_num, max_num, user_id, gridset_id, meta_string,
            after_stat, before_stat, after_time, before_time, atom)

    # ................................
    def summarize_mf_chains_for_gridset(self, gridset_id):
        """Return summary information about makeflows for a gridset
        """
        return self._borg.summarize_mf_chains_for_gridset(gridset_id)

    # ................................
    def update_object(self, obj):
        """Update the object in the database
        """
        return self._borg.update_object(obj)

    # ................................
    def delete_object(self, obj):
        """Delete an object from the databse
        """
        return self._borg.delete_object(obj)

    # ................................
    def delete_gridset_return_filenames_mtx_col_ids(self, gridset_id):
        """Delete a gridset and return filenames and matrix column ids.
        """
        mtx_col_ids = self._borg.delete_gridset_return_mtx_col_ids(gridset_id)
        f_names = self._borg.delete_gridset_return_filenames(gridset_id)

        return f_names, mtx_col_ids

    # ................................
    def delete_obsolete_user_gridsets_return_filenames_mtx_col_ids(
            self, user_id, obsolete_time):
        """Delete obsolete user gridsets and return matrix column filenames.
        """
        all_filenames = []
        all_mtx_col_ids = []
        gridset_ids = self._borg.find_user_gridsets(
            user_id, obsolete_time=obsolete_time)
        for gridset_id in gridset_ids:
            f_names, mtx_col_ids = \
                self.delete_gridset_return_filenames_mtx_col_ids(gridset_id)
            all_filenames.extend(f_names)
            all_mtx_col_ids.extend(mtx_col_ids)
        return all_filenames, all_mtx_col_ids

    # ................................
    def delete_obsolete_sdm_data_return_ids(self, user_id, before_time,
                                            max_num=100):
        """Delete obsolete SDM data and return deleted ids.
        """
        occ_ids = self._borg.delete_obsolete_sdm_data_return_ids(
            user_id, before_time, max_num)
        mtx_col_ids = self._borg.delete_obsolete_sdm_mtx_cols_return_ids(
            user_id, before_time, max_num)
        return occ_ids, mtx_col_ids

    # ................................
    def get_map_service_for_sdm_occurrence(self, occ_lyr_or_id):
        """Get the map service for an occurrence set
        @param map_filename: absolute path of mapfile
        @return: LmServer.base.layerset.MapLayerSet containing objects for this
                    a map service
        """
        try:
            int(occ_lyr_or_id)
        except ValueError:
            occ_lyr_or_id = occ_lyr_or_id.get_id()
        occ = self.get_occurrence_set(occ_id=occ_lyr_or_id)
        lyrs = self.list_sdm_projects(
            0, 500, user_id=occ.get_user_id(), occ_set_id=occ_lyr_or_id,
            atom=False)
        lyrs.append(occ)
        map_name = EarlJr().create_basename(
            LMFileType.SDM_MAP, obj_code=occ.get_id(), usr=occ.get_user_id())
        map_svc = MapLayerSet(
            map_name, layers=lyrs, db_id=occ.get_id(),
            user_id=occ.get_user_id(), epsg_code=occ.epsg_code, bbox=occ.bbox,
            map_units=occ.map_units, map_type=LMFileType.SDM_MAP)
        return map_svc

    # ................................
    def get_map_service_from_map_filename(self, map_filename):
        """Get a map service from a map file name.

        Args:
            map_filename: absolute path of mapfile
        """
        earl = EarlJr()
        (mapname, _ancillary, usr, _epsg, occ_set_id, _gridset_id, scen_code
         ) = earl.parse_map_filename(map_filename)
        prefix = mapname.split(NAME_SEPARATOR)[0]
        file_type = FileFix.get_map_type_from_name(prefix=prefix)
        if file_type == LMFileType.SDM_MAP:
            map_svc = self.get_map_service_for_sdm_occurrence(occ_set_id)
        elif file_type == LMFileType.RAD_MAP:
            self.log.error('Mapping is not yet implemented for RAD_MAP')
        elif file_type == LMFileType.SCENARIO_MAP:
            map_svc = self.get_scenario(
                scen_code, user_id=usr, fill_layers=True)
        else:
            self.log.error(
                'Mapping is available for SDM_MAP, SCENARIO_MAP, RAD_MAP')
        return map_svc

    # ................................
    def get_occ_layers_for_gridset(self, gridset_id):
        """Get occurrence layers for a gridset
        """
        occs = []
        pams = self.get_matrices_for_gridset(
            gridset_id, mtx_type=MatrixType.PAM)
        for pam in pams:
            occs.extend(self._borg.get_occ_layers_for_matrix(pam.get_id()))
        return occs

    # ................................
    def get_sdm_columns_for_gridset(self, gridset_id, return_columns=True,
                                    return_projections=True):
        """Get SDM columns in a gridset.
        """
        all_pairs = []
        pams = self.get_matrices_for_gridset(
            gridset_id, mtx_type=MatrixType.PAM)
        for pam in pams:
            all_pairs.extend(
                self.get_sdm_columns_for_matrix(
                    pam.get_id(), return_columns, return_projections))

        return all_pairs

    # ................................
    def get_matrices_for_gridset(self, gridset_id, mtx_type=None):
        """Return matrices for a gridset.
        """
        return self._borg.get_matrices_for_gridset(gridset_id, mtx_type)
