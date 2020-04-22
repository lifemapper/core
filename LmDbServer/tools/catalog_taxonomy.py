"""This script catalogs accepted GBIF taxonomy in the database
"""
import argparse
import csv
import os
import time

from LmBackend.command.server import CatalogTaxonomyCommand
from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import GBIF, JobStatus, ENCODING
from LmCommon.common.time import gmt
from LmDbServer.common.lmconstants import (
    GBIF_TAXONOMY_DUMP_FILE, TAXONOMIC_SOURCE)
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import NUM_DOCS_PER_POST, Priority
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
import LmServer.common.solr as lm_solr
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.process_chain import MFChain


# .............................................................................
class TaxonFiller(LMObject):
    """Populates the database with accepted taxonomy.

    Class to populates a Lifemapper database with taxonomy for accepted names
    in the GBIF Backbone Taxonomy as read from a text file provided by GBIF.

    TODO:
        Extend this script to add taxonomy for users
    """

    # ...............................................
    def __init__(self, tax_src_name, taxonomy_fname, tax_success_fname,
                 tax_src_url=None, delimiter='\t', log_name=None):
        """Constructor for TaxonFiller class.

        Args:
            tax_src_name: short name for source of taxonomy in database
            taxonomy_fname: absolute filename for taxonomy data
            tax_success_fname: absolute filename to be written indicating
                success
            tax_src_url: url for source of taxonomy in database
            delimiter: delimiter for taxonomy CSV data
            log_name: basename for logfile

        TODO:
            - Allow tax_src_name to be a user_id, and user taxonomy is allowed
            - Define data format for user-provided taxonomy
        """
        super(TaxonFiller, self).__init__()
        script_name, _ = os.path.splitext(os.path.basename(__file__))
        self.name = script_name
        try:
            self.scribe = self._get_db(log_name)
        except Exception as err:
            raise LMError('Failed to get a scribe object', err)
        self.taxonomy_fname = taxonomy_fname
        self._success_fname = tax_success_fname
        self._taxonomy_source_name = tax_src_name
        self._taxonomy_source_url = tax_src_url
        self._delimiter = delimiter
        self._taxonomy_source_id = None
        self._taxon_file = None
        self._csv_reader = None

    # ...............................................
    def open(self):
        """Open database connection."""
        success = self.scribe.open_connections()
        if not success:
            raise LMError('Failed to open database')

    # ...............................................
    def close(self):
        """Close the object and connections."""
        self._taxon_file.close()
        self.scribe.close_connections()

    # ...............................................
    def initialize_me(self):
        """Initialize the object."""
        self._taxonomy_source_id = self.scribe.find_or_insert_taxon_source(
            self._taxonomy_source_name, self._taxonomy_source_url)
        self._taxon_file = open(self.taxonomy_fname, 'r', encoding=ENCODING)
        self._csv_reader = csv.reader(
            self._taxon_file, delimiter=self._delimiter)

    # ...............................................
    @property
    def log_filename(self):
        """Return the log filename."""
        try:
            fname = self.scribe.log.base_filename
        except AttributeError:
            fname = None
        return fname

    # ...............................................
    @staticmethod
    def _get_db(log_name):
        logger = ScriptLogger(log_name)
        scribe = BorgScribe(logger)
        return scribe

    # ...............................................
    @staticmethod
    def _convert_string(val_str, is_integer=True):
        val = None
        if is_integer:
            try:
                val = int(val_str)
            except ValueError:
                pass
        else:
            try:
                val = float(val_str)
            except ValueError:
                pass
        return val

    # ...............................................
    @staticmethod
    def _get_taxon_values(line):
        # aka LmCommon.common.lmconstants GBIF_TAXON_FIELDS
        (taxon_key, kingdom_str, phylum_str, class_str, order_str, family_str,
         genus_str, sci_name_str, genus_key, species_key, count) = line
        try:
            tx_key = int(taxon_key)
            occ_count = int(count)
        except ValueError:
            print('Invalid taxon_key {} or count {} for {}'.format(
                taxon_key, count, sci_name_str))
        try:
            genkey = int(genus_key)
        except ValueError:
            genkey = None
        try:
            spkey = int(species_key)
        except ValueError:
            spkey = None
        return (tx_key, kingdom_str, phylum_str, class_str, order_str,
                family_str, genus_str, sci_name_str, genkey, spkey, occ_count)

    # ...............................................
    def write_success_file(self, message):
        """Write a file to indicate to makeflow that the process succeeded."""
        self.ready_filename(self._success_fname, overwrite=True)
        try:
            with open(self._success_fname, 'w', encoding=ENCODING) as out_file:
                out_file.write(message)
        except IOError as io_err:
            raise LMError('Failed to write success file', io_err)

    # ...............................................
    def read_and_insert_taxonomy(self):
        """Read taxonomy and insert into the database."""
        total_in = total_out = total_wrong_rank = 0

        sciname_objs = []

        for line in self._csv_reader:
            (taxon_key, kingdom_str, phylum_str, class_str, order_str,
             family_str, genus_str, sci_name_str, genus_key, species_key,
             _count) = self._get_taxon_values(line)

            if taxon_key not in (species_key, genus_key):
                total_wrong_rank += 1
            else:
                if taxon_key == species_key:
                    rank = GBIF.RESPONSE_SPECIES_KEY
                elif taxon_key == genus_key:
                    rank = GBIF.RESPONSE_GENUS_KEY
                sci_name = ScientificName(
                    sci_name_str, rank=rank, canonical_name=None,
                    kingdom=kingdom_str, phylum=phylum_str, class_=class_str,
                    order_=order_str, family=family_str, genus=genus_str,
                    taxonomy_source_id=self._taxonomy_source_id,
                    taxonomy_source_key=taxon_key,
                    taxonomy_source_genus_key=genus_key,
                    taxonomy_source_species_key=species_key)
                up_sci_name = self.scribe.find_or_insert_taxon(
                    taxon_source_id=self._taxonomy_source_id,
                    taxon_key=taxon_key, sci_name=sci_name)
                if up_sci_name:
                    total_in += 1
                    self.scribe.log.info(
                        'Found or inserted {}'.format(sci_name_str))
                    # Add object to list, post to solr if we reach threshold
                    sciname_objs.append(up_sci_name)
                    if len(sciname_objs) >= NUM_DOCS_PER_POST:
                        lm_solr.add_taxa_to_taxonomy_index(sciname_objs)
                        sciname_objs = []
                else:
                    total_out += 1
                    self.scribe.log.info(
                        'Failed to insert or find {}'.format(sci_name_str))
        # Add any leftover taxonomy
        lm_solr.add_taxa_to_taxonomy_index(sciname_objs)

        msg = 'Found or inserted {}; failed {}; wrongRank {}'.format(
            total_in, total_out, total_wrong_rank)
        self.write_success_file(msg)
        self.scribe.log.info(msg)

    # ...............................................
    def create_catalog_taxonomy_mf(self):
        """Create a makeflow to initiate taxonomy reader.

        Note:
            Not currently used, MF is created in initWorkflow
        """
        script_name, _ = os.path.splitext(os.path.basename(__file__))
        meta = {
            MFChain.META_CREATED_BY: script_name,
            MFChain.META_DESCRIPTION:
                'Catalog Taxonomy task for source {}, user {}'.format(
                    self._taxonomy_source_name, PUBLIC_USER)}
        new_mfc = MFChain(
            PUBLIC_USER, priority=Priority.HIGH, metadata=meta,
            status=JobStatus.GENERAL, status_mod_time=gmt().mjd)
        mf_chain = self.scribe.insert_mf_chain(new_mfc, None)

        # Create a rule from the MF and Arf file creation
        cattax_cmd = CatalogTaxonomyCommand(
            self._taxonomy_source_name, self.taxonomy_fname,
            self._success_fname, source_url=self._taxonomy_source_url,
            delimiter=self._delimiter)
        mf_chain.add_commands([cattax_cmd.get_makeflow_rule(local=True)])
        mf_chain.write()
        mf_chain.update_status(JobStatus.INITIALIZE)
        self.scribe.update_object(mf_chain)


# .............................................................................
def main():
    """Main method for script."""
    parser = argparse.ArgumentParser(
        description=('Populate a Lifemapper archive with taxonomic data for '
                     'one or more species, from a CSV file'))
    parser.add_argument(
        '--taxon_source_name', type=str,
        default=TAXONOMIC_SOURCE['GBIF']['name'],
        help=('Identifier of taxonomy source to populate.  This must either '
              'already exist in the database, or it will be added to the '
              'database with the (now required) optional parameter '
              '`taxon_source_url`.'))
    parser.add_argument(
        '--taxon_data_filename', type=str, default=None,
        help='Filename of CSV taxonomy data.')
    parser.add_argument(
        '--success_filename', type=str, default=None,
        help='Filename to be written on successful completion of script.')
    parser.add_argument(
        '--taxon_source_url', type=str, default=None,
        help="Optional URL of taxonomy source, requiredfor a new source")
    parser.add_argument(
        '--delimiter', type=str, default='\t',
        help=("""Delimiter in CSV taxon_data_filename. Defaults to tab."""))
    parser.add_argument(
        '--log_name', type=str, default=None, help=('Base name of logfile '))
    # Taxonomy ingest Makeflows will generally be written as part of a BOOM
    #    job calling this script, so new species boom data may be connected to
    #    taxonomy
    args = parser.parse_args()
    source_name = args.taxon_source_name
    taxon_fname = args.taxon_data_filename
    success_fname = args.success_filename
    log_name = args.log_name
    source_url = args.taxon_source_url
    delimiter = args.delimiter

    if source_name == TAXONOMIC_SOURCE['GBIF']['name']:
        if taxon_fname is None:
            taxon_fname = GBIF_TAXONOMY_DUMP_FILE
        if success_fname is None:
            taxbasename, _ = os.path.splitext(taxon_fname)
            _taxon_success_fname = taxbasename + '.success'

    if log_name is None:
        scriptname, _ = os.path.splitext(os.path.basename(__file__))
        dataname, _ = os.path.splitext(taxon_fname)
        secs = time.time()
        timestamp = "{}".format(
            time.strftime("%Y%m%d-%H%M", time.localtime(secs)))
        log_name = '{}.{}.{}'.format(scriptname, dataname, timestamp)

    filler = TaxonFiller(
        source_name, taxon_fname, success_fname, tax_src_url=source_url,
        delimiter=delimiter, log_name=log_name)
    filler.open()

    filler.initialize_me()
    filler.read_and_insert_taxonomy()

    filler.close()


# .............................................................................
if __name__ == '__main__':
    main()


"""
import csv
import os
import time

from LmBackend.command.server import CatalogTaxonomyCommand
from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import GBIF, JobStatus
from LmCommon.common.time import gmt
from LmDbServer.common.lmconstants import (
    GBIF_TAXONOMY_DUMP_FILE, TAXONOMIC_SOURCE)
from LmServer.base.taxon import ScientificName
from LmServer.common.lmconstants import NUM_DOCS_PER_POST, Priority
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
import LmServer.common.solr as lm_solr
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.process_chain import MFChain

from LmDbServer.tools.catalog_taxonomy import *

source_name = "GBIF Backbone Taxonomy"
taxon_fname = '/share/lmserver/data/species/gbif_taxonomy-2019.04.12.csv'
success_fname = 'catalog_taxonomy.success'
log_name = 'catalog_taxonomy.gbif_taxonomy-2019.04.12.20200418-1542'
source_url = 'http://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c'
delimiter = '\t'


self = TaxonFiller(
    source_name, taxon_fname, success_fname, tax_src_url=source_url,
    delimiter=delimiter, log_name=log_name)
self.open()

self.initialize_me()


line = next(self._csv_reader)
(taxon_key, kingdom_str, phylum_str, class_str, order_str,
 family_str, genus_str, sci_name_str, genus_key, species_key,
 _count) = self._get_taxon_values(line)

if taxon_key not in (species_key, genus_key):
    total_wrong_rank += 1
    
# else:
if taxon_key == species_key:
    rank = GBIF.RESPONSE_SPECIES_KEY
elif taxon_key == genus_key:
    rank = GBIF.RESPONSE_GENUS_KEY
sci_name = ScientificName(
    sci_name_str, rank=rank, canonical_name=None,
    kingdom=kingdom_str, phylum=phylum_str, class_=class_str,
    order_=order_str, family=family_str, genus=genus_str,
    taxonomy_source_id=self._taxonomy_source_id,
    taxonomy_source_key=taxon_key,
    taxonomy_source_genus_key=genus_key,
    taxonomy_source_species_key=species_key)




scientific_name = None
curr_time = gmt().mjd
usr = squid = kingdom = phylum = class_ = order_ = family = None
genus = rank = can_name = sci_name = gen_key = sp_key = None
key_hierarchy = last_count = None

try:
    taxon_source_id = sci_name.taxonomy_source_id
    taxon_key = sci_name.sourceTaxonKey
    usr = sci_name.user_id
    squid = sci_name.squid
    kingdom = sci_name.kingdom
    phylum = sci_name.phylum
    class_ = sci_name.txClass
    order_ = sci_name.txOrder
    family = sci_name.family
    genus = sci_name.genus
    rank = sci_name.rank
    can_name = sci_name.canonicalName
    sci_name = sci_name.scientificName
    gen_key = sci_name.sourceGenusKey
    sp_key = sci_name.sourceSpeciesKey
    key_hierarchy = sci_name.sourceKeyHierarchy
    last_count = sci_name.lastOccurrenceCount
except Exception:
    pass
try:
    row, idxs = self.execute_insert_and_select_one_function(
        'lm_findOrInsertTaxon', taxon_source_id, taxon_key, usr, squid,
        kingdom, phylum, class_, order_, family, genus, rank, can_name,
        sci_name, gen_key, sp_key, key_hierarchy, last_count,
        curr_time)
except Exception as e:
    raise e
else:
    scientific_name = self._create_scientific_name(row, idxs)

# up_sci_name = self.scribe.find_or_insert_taxon(
#     taxon_source_id=self._taxonomy_source_id,
#     taxon_key=taxon_key, sci_name=sci_name)

if up_sci_name:
    total_in += 1
    self.scribe.log.info(
        'Found or inserted {}'.format(sci_name_str))
    # Add object to list, post to solr if we reach threshold
    sciname_objs.append(up_sci_name)
    if len(sciname_objs) >= NUM_DOCS_PER_POST:
        lm_solr.add_taxa_to_taxonomy_index(sciname_objs)
        sciname_objs = []
else:
    total_out += 1
    self.scribe.log.info(
        'Failed to insert or find {}'.format(sci_name_str))

# self.read_and_insert_taxonomy()

filler.close()

"""