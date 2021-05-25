"""Catalog a scenario package for a user
"""
import argparse
import imp
import logging
import os
import sys
import time

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.time import gmt
from LmServer.base.layer import Vector, Raster
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import (ENV_DATA_PATH, DEFAULT_EMAIL_POSTFIX)
from LmServer.common.lmuser import LMUser
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.env_layer import EnvLayer
from LmServer.legion.scenario import Scenario, ScenPackage

CURRDATE = (gmt().year, gmt().month, gmt().day)


# .............................................................................
class SPFiller(LMObject):
    """Populate database with scenario metadata for a BOOM archive."""
    version = '2.0'

# .............................................................................
# Constructor
# .............................................................................
    def __init__(self, sp_meta_fname, user_id, email=None, logname=None,
                 scribe=None):
        """Constructor."""
        super(SPFiller, self).__init__()
        self.name = self.__class__.__name__.lower()
        if not os.path.exists(sp_meta_fname):
            raise LMError(
                'Climate metadata {} does not exist'.format(sp_meta_fname))
        layer_base_path, fname = os.path.split(sp_meta_fname)
        sp_basename, _ = os.path.splitext(fname)
        self.layer_base_path = layer_base_path

        # TODO: change to importlib on python 2.7 --> 3.3+
        try:
            self.sp_meta = imp.load_source('currentmetadata', sp_meta_fname)
        except Exception as e:
            raise LMError(
                'Climate metadata {} cannot be imported; ({})'.format(
                    sp_meta_fname, e))

        scen_package_names = ','.join(
            list(self.sp_meta.CLIMATE_PACKAGES.keys()))

        # version is a string
        try:
            if self.sp_meta.VERSION != self.version:
                raise LMError(
                    ('SPFiller version {} cannot parse {} metadata '
                     'version {}').format(
                         self.version, scen_package_names,
                         self.sp_meta.VERSION))
        except Exception:
            raise LMError(
                ('SPFiller version {} cannot parse {} non-versioned metadata'
                 ).format(scen_package_names, self.version))

        self.sp_meta_fname = sp_meta_fname
        self.user_id = user_id
        self.user_email = email
        if self.user_email is None:
            self.user_email = '{}{}'.format(
                self.user_id, DEFAULT_EMAIL_POSTFIX)

        # Logfile
        if logname is None:
            logname = '{}.{}.{}'.format(self.name, sp_basename, user_id)
        self.logname = logname

        self.scribe = scribe

    # ...............................................
    def initialize_me(self):
        """Initialize this object with the database connection."""
        if not (self.scribe and self.scribe.is_open):
            # Get database
            self.scribe = self._get_db(self.logname)
            self.open()

    # ...............................................
    def open(self):
        """Open database connections."""
        success = self.scribe.open_connections()
        if not success:
            raise LMError('Failed to open database')

    # ...............................................
    def close(self):
        """Close database connection."""
        self.scribe.close_connections()

    # ...............................................
    @property
    def log_filename(self):
        """Return the absolute logfile name."""
        try:
            fname = self.scribe.log.base_filename
        except Exception:
            fname = None
        return fname

    # ...............................................
    @staticmethod
    def _get_db(logname):
        logger = ScriptLogger(logname, level=logging.INFO)
        # DB connection
        scribe = BorgScribe(logger)
        return scribe

    # ...............................................
    def _create_mask_layer(self, package_meta, mask_meta):
        """Create a mask layer for the package.

        Assembles layer metadata for input to optional pre-processing SDM Mask
        step identified in scenario package metadata.

        Note:
            Only the 'hull_region_intersect' method is currently available.
        """
        # Required keys in SDM_MASK_INPUT: name, bbox, gdaltype, gdalformat,
        #    file
        lyr_meta = {
            Vector.META_IS_CATEGORICAL: self._get_optional_metadata(
                mask_meta, 'iscategorical'),
            ServiceObject.META_TITLE: self._get_optional_metadata(
                mask_meta, 'title'),
            ServiceObject.META_AUTHOR: self._get_optional_metadata(
                mask_meta, 'author'),
            ServiceObject.META_DESCRIPTION: self._get_optional_metadata(
                mask_meta, 'description'),
            ServiceObject.META_KEYWORDS: self._get_optional_metadata(
                mask_meta, 'keywords'),
            ServiceObject.META_CITATION: self._get_optional_metadata(
                mask_meta, 'citation')}
        # required
        try:
            dloc = os.path.join(self.layer_base_path, mask_meta['file'])
        except KeyError:
            raise LMError(
                'Missing `file` key in SDM_MASK_META in scen_pkg metadata')
        else:
            if not os.path.exists(dloc):
                raise LMError('Missing local data {}'.format(dloc))

        try:
            mask_lyr = Raster(
                mask_meta['name'], self.user_id, package_meta['epsg'],
                map_units=package_meta['mapunits'],
                resolution=mask_meta['res'][1], dlocation=dloc,
                metadata=lyr_meta, data_format=mask_meta['gdalformat'],
                gdal_type=mask_meta['gdaltype'], bbox=mask_meta['region'],
                mod_time=gmt().mjd)
        except KeyError:
            raise LMError(
                ('Missing one of: name, res, region, gdal type, gdal format '
                 'in SDM_MASK_META in scen package metadata'))

        return mask_lyr

    # ...............................................
    def add_user(self):
        """Add a user to the database.

        Adds or finds PUBLIC_USER, DEFAULT_POST_USER and USER arguments in the
        database
        """
        curr_time = gmt().mjd
        # Nothing changes if these are already present
        user = LMUser(
            self.user_id, self.user_email, self.user_email, mod_time=curr_time)
        self.scribe.log.info(
            '  Find or insert user {} ...'.format(self.user_id))
        this_user = self.scribe.find_or_insert_user(user)
        # If exists, found by unique Id or Email, update values
        return this_user.user_id

    # ...............................................
    def create_scen_package(self, sp_name):
        """Create a scenario package from a metadata document and insert into
            database.
        """
        package_meta = self.sp_meta.CLIMATE_PACKAGES[sp_name]
        layer_meta = self.sp_meta.LAYERTYPE_META
        # TODO: Put optional masklayer into every Scenario
        try:
            mask_meta = self.sp_meta.SDM_MASK_META
        except AttributeError:
            mask_lyr = None
        else:
            mask_lyr = self._create_mask_layer(package_meta, mask_meta)

        self.scribe.log.info(
            '  Read ScenPackage {} metadata ...'.format(sp_name))
        scen_pkg = ScenPackage(
            sp_name, self.user_id, epsg_code=package_meta['epsg'],
            map_units=package_meta['mapunits'], mod_time=gmt().mjd)

        # Current
        base_code = package_meta['baseline']
        base_meta = self.sp_meta.SCENARIO_META[base_code]
        bscen = self._create_scenario(
            package_meta, base_code, base_meta, layer_meta)
        self.scribe.log.info(
            '     Assembled base scenario {}'.format(base_code))
        all_scens = {base_code: bscen}

        # Predicted Past and Future
        for pred_code in package_meta['predicted']:
            scenario_meta = self.sp_meta.SCENARIO_META[pred_code]
            pscen = self._create_scenario(
                package_meta, pred_code, scenario_meta, layer_meta)
            all_scens[pred_code] = pscen

        self.scribe.log.info(
            '     Assembled predicted scenarios {}'.format(
                list(all_scens.keys())))
        for scen in list(all_scens.values()):
            scen_pkg.add_scenario(scen)
        scen_pkg.reset_bbox()

        return (scen_pkg, mask_lyr)

    # ...............................................
    @staticmethod
    def _get_bio_name(code, res, gcm=None, tm=None, alt_pred=None,
                      lyr_type=None, suffix=None, is_title=False):
        sep = '-'
        if is_title:
            sep = ', '
        name = code
        if lyr_type is not None:
            name = sep.join((lyr_type, name))
        for descriptor in (gcm, alt_pred, tm, res, suffix):
            if descriptor is not None:
                name = sep.join((name, descriptor))
        return name

    # ...............................................
    @staticmethod
    def _get_optional_metadata(meta_dict, key):
        """Assemble layer metadata for mask."""
        val = None
        try:
            val = meta_dict[key]
        except KeyError:
            pass
        return val

    # ...............................................
    def _get_scen_layers(self, package_meta, scen_code, scenario_meta,
                         layer_meta):
        """Assemble layer metadata for a single layerset."""
        curr_time = gmt().mjd
        layers = []
        try:
            date_code = scenario_meta['date']
        except KeyError:
            date_code = None
        try:
            alt_pred_code = scenario_meta['altpred']
        except KeyError:
            alt_pred_code = None
        try:
            gcm_code = scenario_meta['gcm']
        except KeyError:
            gcm_code = None
        res_val = scenario_meta['res'][1]
        scen_keywords = list(scenario_meta['keywords'])
        region = scenario_meta['region']

        for env_code in package_meta['layertypes']:
            lt_meta = layer_meta[env_code]
            layer_keywords = list(lt_meta['keywords'])
            layer_keywords.extend(scen_keywords)
            relative_fname = lt_meta['files'][scen_code]
            lyr_name = '{}_{}'.format(env_code, scen_code)
            lyr_meta = {
                'title': ' '.join((lt_meta['title'], scen_code)),
                'description': ' '.join((scen_code, lt_meta['description']))}
            env_meta = {
                'title': lt_meta['title'],
                'description': lt_meta['description'],
                'keywords': layer_keywords}
            dloc = os.path.join(self.layer_base_path, relative_fname)
            if not os.path.exists(dloc):
                raise LMError('Missing local data {}'.format(dloc))
            env_lyr = EnvLayer(
                lyr_name, self.user_id, package_meta['epsg'], dlocation=dloc,
                layer_metadata=lyr_meta,
                data_format=package_meta['gdalformat'],
                gdal_type=package_meta['gdaltype'],
                val_units=lt_meta['valunits'],
                map_units=package_meta['mapunits'],
                resolution=res_val, bbox=region, mod_time=curr_time,
                env_code=env_code, gcm_code=gcm_code,
                alt_pred_code=alt_pred_code, date_code=date_code,
                env_metadata=env_meta, env_mod_time=curr_time)
            layers.append(env_lyr)
        return layers

    # ...............................................
    def _create_scenario(self, package_meta, scen_code, scenario_meta,
                         layer_meta):
        res_val = scenario_meta['res'][1]
        lyrs = self._get_scen_layers(
            package_meta, scen_code, scenario_meta, layer_meta)
        try:
            date_code = scenario_meta['date']
        except KeyError:
            date_code = None
        try:
            alt_pred_code = scenario_meta['altpred']
        except KeyError:
            alt_pred_code = None
        try:
            gcm_code = scenario_meta['gcm']
        except KeyError:
            gcm_code = None

        scen_meta = {
            ServiceObject.META_TITLE: scenario_meta['name'],
            ServiceObject.META_AUTHOR: scenario_meta['author'],
            ServiceObject.META_DESCRIPTION: scenario_meta['description'],
            ServiceObject.META_KEYWORDS: scenario_meta['keywords']}
        scen = Scenario(
            scen_code, self.user_id, package_meta['epsg'], metadata=scen_meta,
            units=package_meta['mapunits'], res=res_val, gcm_code=gcm_code,
            alt_pred_code=alt_pred_code, date_code=date_code,
            bbox=scenario_meta['region'], mod_time=gmt().mjd, layers=lyrs)
        return scen

    # ...............................................
    def add_package_scenarios_layers(self, scen_pkg):
        """Add scenario package layers to the database.

        Add scenPackage, scenario and layer metadata to database, and update
        the scen_pkg attribute with newly inserted objects
        """
        # Insert empty ScenPackage
        updated_scen_pkg = self.scribe.find_or_insert_scen_package(scen_pkg)
        updated_scens = []
        for scode, scen in scen_pkg.scenarios.items():
            self.scribe.log.info('Insert scenario {}'.format(scode))
            # Insert Scenario and its Layers
            new_scen = self.scribe.find_or_insert_scenario(
                scen, scen_package_id=updated_scen_pkg.get_id())
            updated_scens.append(new_scen)
        updated_scen_pkg.set_scenarios(updated_scens)
        return updated_scen_pkg

    # ...............................................
    def add_mask_layer(self, mask_lyr):
        """Add a mask layer
        """
        updated_mask = None
        if mask_lyr is not None:
            updated_mask = self.scribe.find_or_insert_layer(mask_lyr)
        return updated_mask

    # ...............................................
    def catalog_scen_packages(self):
        """Initialize an empty Lifemapper database and archive
        """
        updated_scen_pkg = None
        try:
            self.initialize_me()
            # If exists, found by unique Id or Email, update values
            _user_id = self.add_user()

            mask_lyr = None
            for sp_name in list(self.sp_meta.CLIMATE_PACKAGES.keys()):
                self.scribe.log.info(
                    'Creating scenario package {}'.format(sp_name))
                scen_pkg, mask_lyr = self.create_scen_package(sp_name)

                # Only one Mask is included per scenario package
                if mask_lyr is not None:
                    self.scribe.log.info(
                        'Adding mask layer {}'.format(mask_lyr.name))
                    updated_mask = self.add_mask_layer(mask_lyr)
                    if updated_mask.get_dlocation() != \
                            mask_lyr.get_dlocation():
                        raise LMError(
                            ('Returned existing layer name {} for user {} '
                             'with filename {}, not expected filename {}'
                             ).format(
                                 mask_lyr.name, self.user_id,
                                 updated_mask.get_dlocation(),
                                 mask_lyr.get_dlocation()))
                updated_scen_pkg = self.add_package_scenarios_layers(scen_pkg)
                if all([
                        updated_scen_pkg is not None,
                        updated_scen_pkg.get_id() is not None,
                        updated_scen_pkg.name == sp_name,
                        updated_scen_pkg.get_user_id() == self.user_id]):
                    self.scribe.log.info(
                        ('Successfully added scenario package {} for '
                         'user {}').format(sp_name, self.user_id))
        finally:
            self.close()

        return updated_scen_pkg


# ...............................................
def main():
    """Main method for script
    """

    parser = argparse.ArgumentParser(
        description=('Populate a Lifemapper archive with metadata for single- '
                     'or multi-species computations specific to the '
                     'configured input data or the data package named.'))
    # Required
    parser.add_argument(
        'user_id', type=str, help='User authorized for the scenario package')
    parser.add_argument(
        'scen_package_meta', type=str,
        help=('Metadata file for Scenario package to be cataloged in '
              'the database.'))
    # Optional
    parser.add_argument(
        '--user_email', type=str, default=None, help='User email')
    parser.add_argument(
        '--logname', type=str, default=None,
        help='Basename of the logfile, without extension')

    args = parser.parse_args()
    user_id = args.user_id
    scen_package_meta = args.scen_package_meta
    logname = args.logname
    user_email = args.user_email

    if logname is None:
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(
            time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(scriptname, timestamp)

    # scen_package_meta may be full pathname or in ENV_DATA_PATH dir
    if not os.path.exists(scen_package_meta):
        # if using package name, look in default location)
        scen_package_meta = os.path.join(
            ENV_DATA_PATH, scen_package_meta + '.py')
    if not os.path.exists(scen_package_meta):
        print(('Missing Scenario Package metadata file {}'.format(
            scen_package_meta)))
        sys.exit(-1)
    else:
        print((
            'Running script with scen_package_meta: {}, user id: {}, email: {}'
            ', log base name: {}').format(
                scen_package_meta, user_id, user_email, logname))

        filler = SPFiller(
            scen_package_meta, user_id, email=user_email, logname=logname)
        filler.initialize_me()
        updated_scen_pkg = filler.catalog_scen_packages()
        if updated_scen_pkg is not None:
            pkg_id = updated_scen_pkg.get_id()
            if pkg_id is not None and \
                    updated_scen_pkg.get_user_id() == user_id:
                print(
                    'Successfully added scen package {} for user {}'.format(
                        pkg_id, user_id))
            else:
                print('Failed to add scenario package {} for user {}'.format(
                    pkg_id, user_id))
        else:
            print(
                ('Failed to add scenario package.  Returned None for package '
                 '{}, user {}').format(scen_package_meta, user_id))


# .............................................................................
if __name__ == '__main__':
    main()
