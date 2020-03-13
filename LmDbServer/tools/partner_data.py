"""Module containing functions for API Queries
"""
try:
    from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except ImportError:
    OFTInteger = 0
    OFTReal = 2
    OFTString = 4
    OFTBinary = 8

import csv
import os

import ot_service_wrapper.open_tree as open_tree

from LmBackend.common.lmobj import LMError
from LmCommon.common.api_query import GbifAPI
from LmCommon.common.lmconstants import PhyloTreeKeys
from LmCommon.common.time import gmt
from LmDbServer.common.lmconstants import (TAXONOMIC_SOURCE, SpeciesDatasource)
from LmServer.base.taxon import ScientificName
from LmServer.common.log import ScriptLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.tree import Tree

GBIF_MISSING_KEY = GbifAPI.GBIF_MISSING_KEY


# .............................................................................
class Partners:
    """Constants for partner data
    """
    OTT_MISSING_KEY = 'unmatched_ott_ids'
    OTT_TREE_KEY = 'newick'
    OTT_TREE_FORMAT = 'newick'


# .............................................................................
class PartnerQuery:
    """Class for querying partner data providers
    """
    # .................................
    def __init__(self, logger=None):
        """
        @summary Constructor for the PartnerQuery class
        """
        self.name = self.__class__.__name__.lower()
        if logger is None:
            logger = ScriptLogger(self.name)
        self.log = logger
        self.encoding = 'utf-8'
        self.delimiter = '\t'

    # .................................
    @staticmethod
    def _convert_type(ogrtype):
        if ogrtype == OFTInteger:
            return 'int'
        if ogrtype == OFTString:
            return 'str'
        if ogrtype == OFTReal:
            return 'float'

        raise LMError('Unknown field type {}'.format(ogrtype))

    # .................................
    def _get_insert_sci_name_for_gbif_species_key(self, scribe, gbif_src_id,
                                                  taxon_key):
        """Returns an existing or newly inserted ScientificName
        """
        sci_name = scribe.get_taxon(
            taxon_source_id=gbif_src_id, taxon_key=taxon_key)
        if sci_name is None:
            # Use API to get and insert species name
            try:
                (rank_str, sciname_str, canonical_str, _, _, _, tax_status,
                 kingdom_str, phylum_str, class_str, order_str, family_str,
                 genus_str, _, genus_key, species_key, _
                 ) = GbifAPI.get_taxonomy(taxon_key)
            except Exception as err:
                self.log.info(
                    'Failed lookup for key {}, ({})'.format(taxon_key, err))
            else:
                # if no species key, this is not a species
                if (rank_str in ('SPECIES', 'GENUS') and
                        tax_status == 'ACCEPTED'):
                    curr_time = gmt().mjd
                    sname = ScientificName(
                        sciname_str, rank=rank_str,
                        canonical_name=canonical_str, kingdom=kingdom_str,
                        phylum=phylum_str, class_=class_str, order_=order_str,
                        family=family_str, genus=genus_str, mod_time=curr_time,
                        taxonomy_source_id=gbif_src_id,
                        taxonomy_source_key=taxon_key,
                        taxonomy_source_genus_key=genus_key,
                        taxonomy_source_species_key=species_key)
                    try:
                        sci_name = scribe.findOrInsertTaxon(sci_name=sname)
                        self.log.info(
                            'Inserted sci_name for taxonKey {}, {}'.format(
                                taxon_key, sci_name.scientific_name))
                    except LMError as lm_err:
                        raise lm_err
                    except Exception as err:
                        raise LMError(
                            'Failed on taxon key: {}'.format(taxon_key), err)
                else:
                    self.log.info(
                        'taxon_key {} is not accepted'.format(taxon_key))
        return sci_name

    # .................................
    @staticmethod
    def _lookup_gbif_for_ott(gbif_ott, ott_label):
        ott_label_prefix = 'ott'
        ott_id = ott_label[len(ott_label_prefix):]
        matches = []
        for gbif_id, ott_map_id in gbif_ott.items():
            if str(ott_map_id) == ott_id:
                matches.append(gbif_id)
        return matches

    # .................................
    def _relabel_ott_tree(self, scribe, otree, gbif_ott):
        tax_src = scribe.get_taxon_source(
            ts_name=TAXONOMIC_SOURCE[SpeciesDatasource.GBIF]['name'])
        gbif_src_id = tax_src.taxonomy_source_id

        squid_dict = {}
        for ott_label in otree.get_labels():
            gbif_ids = self._lookup_gbif_for_ott(gbif_ott, ott_label)
            if len(gbif_ids) == 0:
                print(('No gbifids for OTT {}'.format(ott_label)))
            else:
                squid_dict[ott_label] = []
                for gid in gbif_ids:
                    sno = self._get_insert_sci_name_for_gbif_species_key(
                        scribe, gbif_src_id, gid)
                    if sno:
                        squid_dict[ott_label].append(sno.squid)
                if len(gbif_ids) == 1:
                    squid_dict[ott_label] = sno.squid
                    self.log.warning(
                        'Multiple matches (gbifids {}) for OTT {}'.format(
                            gbif_ids, ott_label))

        otree.annotate_tree(PhyloTreeKeys.SQUID, squid_dict)
        print("Adding interior node labels to tree")
        otree.add_node_labels()

    # .................................
    def relabel_ott_tree_to_gbif_name(self, otree, gbif_ott, keys_names):
        """Relabel open tree ids to gbif names
        """
        ott_gbif_dict = {}
        for ott_label in otree.get_labels():
            gbif_ids = self._lookup_gbif_for_ott(gbif_ott, ott_label)
            if len(gbif_ids) == 0:
                print(('No gbifids for OTT {}'.format(ott_label)))
            else:
                if len(gbif_ids) == 1:
                    gid = gbif_ids[0]
                    canonical = keys_names[gid]
                    ott_gbif_dict[ott_label] = canonical
                else:
                    ott_gbif_dict[ott_label] = gbif_ids
                    self.log.warning(
                        'Multiple matches (gbifids {}) for OTT {}'.format(
                            gbif_ids, ott_label))

        otree.annotate_tree('label', ott_gbif_dict)
        print("Adding interior node labels to tree")
        otree.add_node_labels()

    # .................................
    @staticmethod
    def _get_opt_val(retdict, fld):
        # Top Match
        try:
            val = retdict[fld]
        except Exception:
            val = ''
        return val

    # .................................
    def _write_name_matches(self, orig_name, good_names, writer):
        # Top Match
        rec = [orig_name]
        gud_name = good_names[0]
        for fld in GbifAPI.NameMatchFieldnames:
            try:
                rec.append(gud_name[fld])
            except Exception:
                rec.append('')
        writer.writerow(rec)

        canonical = self._get_opt_val(gud_name, 'canonicalName')
        species_key_1 = self._get_opt_val(gud_name, 'speciesKey')
        print(('origname {}, canonical {}, speciesKey {}'.format(
            orig_name, canonical, species_key_1)))

        # Alternate matches
        alternatives = good_names[1:]
        for alt_name in alternatives:
            rec = [orig_name]
            for fld in GbifAPI.NameMatchFieldnames:
                try:
                    rec.append(alt_name[fld])
                except Exception:
                    rec.append('')
            writer.writerow(rec)

            canonical = self._get_opt_val(alt_name, 'canonicalName')
            species_key = self._get_opt_val(alt_name, 'speciesKey')
            print(('origname {}, canonical {}, speciesKey {}'.format(
                orig_name, canonical, species_key)))
        # Return only top match
        return species_key_1, canonical

    # .................................
    def read_gbif_taxon_ids(self, gbif_id_f_name):
        """Read GBIF ids
        """
        taxon_ids = []
        name_to_gbif_ids = {}
        try:
            in_file = open(gbif_id_f_name, 'r')
            csv_reader = csv.reader(
                in_file, delimiter=self.delimiter, encoding=self.encoding)
        except Exception as err:
            raise Exception(
                'Failed to read or open {}, ({})'.format(
                    gbif_id_f_name, str(err)))
        header = next(csv_reader)
        line = next(csv_reader)
        curr_name = None
        while line is not None:
            try:
                this_name = line[header.index('providedName')]
                this_taxon_id = line[header.index('speciesKey')]
                this_canonical = line[header.index('canonicalName')]
                this_score = line[header.index('confidence')]
            except KeyError as key_err:
                self.log.error(
                    'Failed on line {} finding key {}'.format(
                        line, str(key_err)))
            except Exception as err:
                self.log.error('Failed on line {}, {}'.format(line, str(err)))
            else:
                # If starting a new set of matches, save last winner and reset
                if curr_name != this_name:
                    # Set default winner values on first line
                    if curr_name is None:
                        curr_name = this_name
                        top_taxon_id = this_taxon_id
                        top_canonical = this_canonical
                        top_score = this_score
                    else:
                        # Save winner from last name
                        taxon_ids.append(top_taxon_id)
                        name_to_gbif_ids[curr_name] = (
                            top_taxon_id, top_canonical)
                        self.log.info(
                            'Found id {} for name {}, score {}'.format(
                                top_taxon_id, curr_name, top_score))
                        # Reset current values
                        curr_name = this_name
                        top_taxon_id = this_taxon_id
                        top_canonical = this_canonical
                        top_score = this_score

                # Test this match score against winner, save if new winner
                if this_score > top_score:
                    top_taxon_id = this_taxon_id
                    top_canonical = this_canonical
                    top_score = this_score
                    self.log.info(
                        '   New winner id {} for name {}, score {}'.format(
                            top_taxon_id, curr_name, top_score))

            # Get next one
            try:
                line = next(csv_reader)
            except OverflowError as over_err:
                self.log.debug(
                    'Overflow on line {}, ({}))'.format(
                        csv_reader.line_num, str(over_err)))
            except StopIteration:
                self.log.debug('EOF after line {}'.format(csv_reader.line_num))
                line = None
            except Exception as e:
                self.log.warning('Bad record {}'.format(e))

        # Save winner from final name
        taxon_ids.append(top_taxon_id)
        name_to_gbif_ids[curr_name] = (top_taxon_id, top_canonical)
        self.log.info(
            'Found final id {} for name {}, score {}'.format(
                top_taxon_id, curr_name, top_score))

        return name_to_gbif_ids

    # .................................
    def assemble_gbif_taxon_ids(self, names, out_f_name):
        """Assemble GBIF taxon ids for a list of names.

        Args:
            names: list of names to be sent to the GBIF species API
            out_f_name: absolute filename for output
        """
        unmatched_names = []
        name_to_gbif_ids = {}
        if not isinstance(names, list):
            names = [names]

        if os.path.exists(out_f_name):
            print(('Deleting existing file {} ...'.format(out_f_name)))
            os.remove(out_f_name)

        with open(out_f_name, 'w') as out_file:
            writer = csv.writer(out_file, delimiter='\t')
            header = ['providedName']
            header.extend(GbifAPI.NameMatchFieldnames)
            writer.writerow(header)

            for orig_name in names:
                good_names = GbifAPI.get_accepted_names(orig_name)
                if len(good_names) == 0:
                    unmatched_names.append(orig_name)
                else:
                    top_id_match, canonical = self._write_name_matches(
                        orig_name, good_names, writer)
                    name_to_gbif_ids[orig_name] = (top_id_match, canonical)

        return unmatched_names, name_to_gbif_ids

    # .................................
    @staticmethod
    def assemble_otol_data(gbif_taxon_ids, data_name):
        """Assemble Open Tree data

        Args:
            gbif_taxon_ids: list of GBIF taxon keys for accepted taxa
            data_name: name for output tree
        """
        tree = None
        gbif_to_ott = open_tree.get_ottids_from_gbifids(gbif_taxon_ids)
        ott_ids = list(gbif_to_ott.values())
        output = open_tree.induced_subtree(ott_ids)

        try:
            ott_unmatched_gbif_ids = output[Partners.OTT_MISSING_KEY]
        except Exception:
            ott_unmatched_gbif_ids = []

        try:
            otree = output[Partners.OTT_TREE_KEY]
        except Exception:
            raise LMError('Failed to retrieve OTT tree')
        else:
            tree = Tree(data_name, data=otree, schema=Partners.OTT_TREE_FORMAT)

        return tree, gbif_to_ott, ott_unmatched_gbif_ids

    # .................................
    def encode_ott_tree_to_gbif(self, otree, gbif_ott, scribe=None):
        """Encode open tree with gbif ids

        Args:
            otree: labeled tree from Open Tree of Life (OTOL)
            gbif_ott: dictionary GBIF taxon keys to matching OTOL
            scribe: BorgScribe object with open database connection
        """
        labeled_tree = otree
        if scribe is None:
            scribe = BorgScribe(self.log)
            try:
                scribe.open_connections()
                self._relabel_ott_tree(scribe, otree, gbif_ott)
            except Exception as err:
                raise LMError(
                    'Failed to relabel or update tree ({})'.format(err))
            finally:
                scribe.close_connections()
        else:
            try:
                self._relabel_ott_tree(scribe, otree, gbif_ott)
            except Exception as err:
                raise LMError(
                    'Failed to relabel or update tree ({})'.format(err), err)

        return labeled_tree
