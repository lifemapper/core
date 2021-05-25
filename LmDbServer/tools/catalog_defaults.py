"""Catalog default values
"""
import argparse
import logging
import os
import time

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import (LMFormat, DEFAULT_POST_USER,
                                         DEFAULT_EPSG, DEFAULT_MAPUNITS)
from LmCommon.common.time import gmt
from LmDbServer.common.lmconstants import TNCMetadata, TAXONOMIC_SOURCE
from LmServer.base.layer import Vector
from LmServer.base.service_object import ServiceObject
from LmServer.common.lmconstants import (ENV_DATA_PATH, DEFAULT_EMAIL_POSTFIX)
from LmServer.common.lmconstants import Algorithms
from LmServer.common.lmuser import LMUser
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.algorithm import Algorithm


# .............................................................................
class Defcat(LMObject):
    """Default catalog class

    Populate a Lifemapper database with scenario package for a BOOM archive
    """

    # ..........................................
    def __init__(self, logname):
        """Constructor"""
        super(Defcat, self).__init__()
        self.name = self.__class__.__name__.lower()

        # Get database
        self.scribe = self._get_db(logname)
        self.open()

    # ..........................................
    def open(self):
        """Open database connection."""
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
        """Return the log file name."""
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
    def add_users(self):
        """Add or find users in the database.

        Adds or finds PUBLIC_USER, DEFAULT_POST_USER and USER arguments in the
        database.
        """
        curr_time = gmt().mjd
        def_users = [PUBLIC_USER, DEFAULT_POST_USER]
        ids = []

        for usr in def_users:
            email = '{}{}'.format(usr, DEFAULT_EMAIL_POSTFIX)
            # Nothing changes if these are already present
            lmuser = LMUser(usr, email, email, mod_time=curr_time)

            self.scribe.log.info('  Find or insert user {} ...'.format(usr))
            # If exists, found by unique Id or unique Email, update object and
            #    return existing
            this_user = self.scribe.find_or_insert_user(lmuser)
            ids.append(this_user.user_id)
        return ids

    # ...............................................
    def add_taxonomic_sources(self):
        """Adds Taxonomic Sources to the database.

        Sources are added from the TAXONOMIC_SOURCE dictionary
        """
        # Insert all taxonomic sources for now
        for _name, tax_info in TAXONOMIC_SOURCE.items():
            _tax_source_id = self.scribe.find_or_insert_taxon_source(
                tax_info['name'], tax_info['url'])

    # .............................
    def add_tnc_ecoregions(self):
        """Add ecoregion layers."""
        meta = {
            Vector.META_IS_CATEGORICAL: TNCMetadata.is_categorical,
            ServiceObject.META_TITLE: TNCMetadata.title,
            ServiceObject.META_AUTHOR: TNCMetadata.author,
            ServiceObject.META_DESCRIPTION: TNCMetadata.description,
            ServiceObject.META_KEYWORDS: TNCMetadata.keywords,
            ServiceObject.META_CITATION: TNCMetadata.citation,
            }
        dloc = os.path.join(
            ENV_DATA_PATH,
            TNCMetadata.filename + LMFormat.get_default_ogr().ext)
        ecoregions = Vector(
            TNCMetadata.title, PUBLIC_USER, DEFAULT_EPSG, ident=None,
            dlocation=dloc, metadata=meta,
            data_format=LMFormat.get_default_ogr().driver,
            ogr_type=TNCMetadata.ogr_type,
            val_attribute=TNCMetadata.val_attribute,
            map_units=DEFAULT_MAPUNITS, bbox=TNCMetadata.bbox,
            mod_time=gmt().mjd)
        return self.scribe.find_or_insert_layer(ecoregions)

    # ...............................................
    def add_algorithms(self):
        """Add algorithms to the database from the algorithm dictionary."""
        algs = []
        for alginfo in Algorithms.implemented():
            meta = {
                'name': alginfo.name,
                'is_discrete_output': alginfo.is_discrete_output,
                'output_format': alginfo.output_format,
                'accepts_categorical_maps': alginfo.accepts_categorical_maps
                }
            alg = Algorithm(alginfo.code, metadata=meta)
            self.scribe.log.info(
                '  Insert algorithm {} ...'.format(alginfo.code))
            algid = self.scribe.find_or_insert_algorithm(alg)
            algs.append(algid)

    # ...............................................
    def add_defaults(self):
        """Add default users, taxonomy, algorithms, and ecoregions to db."""
        # Insert PUBLIC_USER, DEFAULT_POST_USER
        self.scribe.log.info('  Insert public and default users ...')
        self.add_users()

        # Insert all taxonomic sources used by LM
        self.scribe.log.info('  Insert taxonomic authority metadata ...')
        self.add_taxonomic_sources()

        # Insert all algorithms
        self.scribe.log.info('  Insert Algorithms ...')
        self.add_algorithms()

        # Insert all algorithms
        self.scribe.log.info('  Insert TNC Ecoregions ...')
        self.add_tnc_ecoregions()


# .............................................................................
def main():
    """Main method for script."""
    parser = argparse.ArgumentParser(
        description=('Populate a LM archive with metadata for default data '
                     'and parameters'))
    # Optional
    parser.add_argument(
        '--logname', type=str, default=None,
        help=('Basename of the logfile, without extension'))

    args = parser.parse_args()
    logname = args.logname

    if logname is None:
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        secs = time.time()
        timestamp = "{}".format(
            time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        logname = '{}.{}'.format(scriptname, timestamp)

    print(('Running {} with logbasename: {}'.format(scriptname, logname)))

    defcat = Defcat(logname)
    defcat.add_defaults()


# .............................................................................
if __name__ == '__main__':
    main()

"""
from LmDbServer.tools.catalogDefaults import *

logname = '/state/partition1/lmscratch/log/test_catalogDefaults.log'

import logging
logger = ScriptLogger(logname, level=logging.INFO)

########################
from LmServer.db.borg_scribe import *

########################

# scribe = BorgScribe(logger)
# scribe.openConnections()

########################
# defcat = Defcat(logname)

########################
# defcat.add_defaults()
########################

"""
