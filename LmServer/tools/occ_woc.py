"""Occurrence data weapon-of-choice

Todo:
    - Consider using generators
"""
import csv
import datetime
import json
import os
import shutil
import sys

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.api_query import GbifAPI
from LmCommon.common.lmconstants import (
    GBIF, JobStatus, LMFormat, ONE_HOUR, ProcessType, ENCODING)
from LmCommon.common.occ_parse import OccDataParser
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt, LmTime
from LmServer.base.taxon import ScientificName
from LmServer.common.data_locator import EarlJr
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.legion.occ_layer import OccurrenceLayer

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR


# .............................................................................
class _SpeciesWeaponOfChoice(LMObject):
    """Base class for getting species data one species at a time
    """
    # ................................
    def __init__(self, scribe, user, archive_name, epsg, exp_date, input_fname,
                 meta_fname=None, taxon_source_name=None, logger=None):
        """Constructor

        Args:
            scribe: An open BorgScribe object
            user: A user id
            archive_name: Name of gridset to be created for these data
            epsg: EPSG code for map projection
            exp_date: Expiration date in Modified Julian Day format
            input_fname: Input species data filename or directory containing
                multiple files of CSV data
            meta_fname: Input species metadata filename or dictionary
            taxon_source_name: Unique name of entity providing taxonomy data
            logger: a logger from LmServer.common.log
        """
        super(_SpeciesWeaponOfChoice, self).__init__()
        self.finished_input = False
        # Set name for this WoC
        self.name = '{}_{}'.format(user, archive_name)
        # Optionally use parent process logger
        if logger is None:
            logger = ScriptLogger(self.name)
        self.log = logger
        self._scribe = scribe
        self.user_id = user
        self.epsg = epsg
        self._obsolete_time = exp_date
        # either a file or directory
        self.input_filename = input_fname
        # Common metadata description for csv points
        # May be installed with species data in location unavailable to
        #    Makeflow
        self.meta_filename = meta_fname
        # Taxon Source for known taxonomy
        self._taxon_source_id = None
        if taxon_source_name is not None:
            tax_source_id, _, _ = self._scribe.find_taxon_source(
                taxon_source_name)
            self._taxon_source_id = tax_source_id
        # Beginning of iteration
        earl = EarlJr()
        self.start_file = earl.create_start_walken_filename(user, archive_name)
        self._line_num = 0

    # ................................
    @staticmethod
    def initialize_me():
        """Base class initialize me
        """

    # ................................
    @property
    def expiration_date(self):
        """Get the expiration date to be used
        """
        return self._obsolete_time

    # ................................
    def reset_expiration_date(self, new_date_mjd):
        """Change the expiration date to the provided value
        """
        curr_time = gmt().mjd
        if new_date_mjd < curr_time:
            self._obsolete_time = new_date_mjd
        else:
            raise LMError('New expiration date {} is in the future (now {})')

    # ................................
    @property
    def occ_delimiter(self):
        """Get the delimiter to use for occurrence data
        """
        try:
            return self._delimiter
        except AttributeError:
            return None

    # ................................
    def _find_start(self):
        line_num = 0
        complete = False
        if os.path.exists(self.start_file):
            with open(self.start_file, 'r', encoding=ENCODING) as in_file:
                for line in in_file:
                    if not complete:
                        self.log.info(
                            'Start on line {}, read from {}'.format(
                                line, self.start_file))
                        try:
                            line_num = int(line)
                            complete = True
                        except ValueError:
                            # Ignore comment lines
                            pass
            os.remove(self.start_file)
        return line_num

    # ................................
    def _get_next_line(self, in_file, csv_reader=None):
        success = False
        line = None
        while not in_file.closed and not success:
            try:
                if csv_reader is not None:
                    line = next(csv_reader)
                else:
                    line = next(in_file)
            except StopIteration:
                self.finished_input = True
                self.log.debug(
                    'Finished file {} on line {}'.format(
                        in_file.name, self._line_num))
                in_file.close()
                self._line_num = -9999
                success = True
            except OverflowError as overflow_e:
                self._line_num += 1
                self.log.debug(
                    'OverflowError on {} ({}), moving on'.format(
                        self._line_num, overflow_e))
            except Exception as err:
                self._line_num += 1
                self.log.debug(
                    'Exception reading line {} ({}), moving on'.format(
                        self._line_num, err))
            else:
                self._line_num += 1
                success = True
                if line == '':
                    line = None
        return line

    # ................................
    def save_next_start(self, fail=False):
        """Save the starting position for the next run
        """
        if fail:
            line_num = self.this_start
        else:
            line_num = self.next_start
        if line_num is not None:
            try:
                with open(self.start_file, 'w', encoding=ENCODING) as out_f:
                    out_f.write(
                        ('# Next start line for {} '
                         'using species data {}\n{}\n').format(
                             self.name, self.input_filename, line_num))
            except Exception:
                self.log.error(
                    'Failed to write next starting line {} to file {}'.format(
                        line_num, self.start_file))

    # ................................
    def _read_provider_keys(self, provider_key_file, provider_key_col_name):
        """
        Return a dictionary of provider uuids and names, and the column index in the occurrence data
        containing the provider uuid to be replaced.
        """
        providers = {}
        prov_key_col = None
        for col_idx, desc in self.occ_parser.column_meta.items():
            if desc['name'] == provider_key_col_name:
                prov_key_col = col_idx
                break
        if prov_key_col is None:
            self.log.error(
                'Unable to find {} in fieldnames'.format(
                    provider_key_col_name))

        if provider_key_file is not None and provider_key_col_name is not None:
            if not os.path.exists(provider_key_file):
                self.log.error(
                    'Missing provider file {}'.format(provider_key_file))
            else:
                with open(provider_key_file, 'r', encoding=ENCODING) as dump_file:
                    csv.field_size_limit(sys.maxsize)
                    csv_reader = csv.reader(dump_file, delimiter=';')
                    for line in csv_reader:
                        try:
                            key, name = line
                            if key != 'key':
                                providers[key] = name
                        except Exception:
                            pass
        return providers, prov_key_col

    # ................................
    def _read_replacements(self, replace_fname, replace_fldname):
        """
        Return a dictionary of keys and values, and the column index in the occurrence data
        containing the field for which to replace a key with a value.
        """
        replacements = {}
        replace_col = None
        for col_idx, desc in self.occ_parser.column_meta.items():
            if desc['name'] == replace_fldname:
                replace_col = col_idx
                break
        if replace_col is None:
            self.log.error(
                'Unable to find {} in fieldnames'.format(
                    replace_fldname))

        if replace_fname is not None and replace_fldname is not None:
            if not os.path.exists(replace_fname):
                self.log.error(
                    'Missing lookup file {}'.format(replace_fname))
            else:
                with open(replace_fname, 'r', encoding=ENCODING) as dump_file:
                    csv.field_size_limit(sys.maxsize)
                    csv_reader = csv.reader(dump_file, delimiter='\t')
                    for line in csv_reader:
                        try:
                            key, val = line
                            if key != 'key':
                                replacements[key] = val
                        except Exception:
                            pass
        return replacements, replace_col

    # ................................
    def _will_compute(self, status, status_mod_time, dlocation, raw_dlocation):
        no_raw_data = not any([raw_dlocation, os.path.exists(raw_dlocation)])
        no_complete_data = not any([dlocation, os.path.exists(dlocation)])
        obsolete_data = 0 < status_mod_time < self._obsolete_time

        return any([
            obsolete_data, no_complete_data, JobStatus.incomplete(status),
            JobStatus.failed(status),
            (JobStatus.waiting(status) and no_raw_data)])

    # ................................
    def _find_or_insert_occurrence_set(self, sci_name, data_count, data=None,
                                       metadata=None):
        """Find or insert an occurrence set
        """
        if metadata is None:
            metadata = {}
        curr_time = gmt().mjd
        occ = None
        # Find existing
        # TODO: CJ, change this if we want canonical name displayed for GBIF
        #    data instead of scientific_name
        tmp_occ = OccurrenceLayer(
            sci_name.scientific_name, self.user_id, self.epsg, data_count,
            squid=sci_name.squid, process_type=self.process_type,
            status=JobStatus.INITIALIZE, status_mod_time=curr_time,
            sci_name=sci_name, raw_meta_dlocation=self.meta_filename)
        try:
            occ = self._scribe.find_or_insert_occurrence_set(tmp_occ)
            self.log.info(
                '    Found/inserted OccLayer {}'.format(occ.get_id()))
        except LMError as lme:
            raise lme
        except Exception as err:
            raise LMError(err, line_num=self.get_line_num())

        if occ is not None:
            # Write raw data regardless
            raw_dloc, _ = self._write_raw_data(
                occ, data=data, metadata=metadata)
            if not raw_dloc:
                raise LMError('    Failed to find raw data location')
            occ.set_raw_dlocation(raw_dloc, curr_time)
            # Set process_type and metadata location (from config, not saved
            #    in DB)
            occ.process_type = self.process_type
            occ.raw_meta_dlocation = self.meta_filename

            # Do reset existing or new Occ?
            will_compute = self._will_compute(
                occ.status, occ.status_mod_time, occ.get_dlocation(),
                occ.get_raw_dlocation())
            if will_compute:
                self.log.info(
                    '    Init new or existing OccLayer status, count')
                occ.update_status(
                    JobStatus.INITIALIZE, mod_time=curr_time,
                    query_count=data_count)
                _ = self._scribe.update_object(occ)
            else:
                # Return existing, completed, unchanged
                self.log.info('    Returning up-to-date OccLayer')
        return occ

    # ................................
    def _get_insert_sci_name_for_gbif_species_key(self, taxon_key,
                                                  taxon_count):
        """Returns an existing or newly inserted ScientificName
        """
        sci_name = self._scribe.find_or_insert_taxon(
            taxon_source_id=self._taxon_source_id, taxon_key=taxon_key)
        if sci_name is not None:
            self.log.info(
                'Found sci_name for taxon_key {}, {}, with {} points'.format(
                    taxon_key, sci_name.scientific_name, taxon_count))
        else:
            # Use API to get and insert species name
            try:
                (rank_str, sciname_str, canonical_str, accepted_key,
                 accepted_str, nub_key, tax_status, kingdom_str, phylum_str,
                 class_str, order_str, family_str, genus_str, species_str,
                 genus_key, species_key, log_lines
                 ) = GbifAPI.get_taxonomy(taxon_key)
            except Exception as err:
                self.log.info(
                    'Failed lookup for key {}, ({})'.format(taxon_key, err))
            else:
                # if no species key, this is not a species
                if rank_str in ('SPECIES', 'GENUS'):
                    if accepted_key is not None:
                        # Update to accepted values
                        taxon_key = accepted_key
                        sciname_str = accepted_str
                    else:
                        self.log.warning(
                            'No accepted key for taxon_key {}'.format(
                                taxon_key))
                        return None

                    curr_time = gmt().mjd
                    # Do not tie GBIF taxonomy to one userid
                    s_name = ScientificName(
                        sciname_str, rank=rank_str,
                        canonical_name=canonical_str, squid=None,
                        last_occurrence_count=taxon_count, kingdom=kingdom_str,
                        phylum=phylum_str, class_=class_str, order_=order_str,
                        family=family_str, genus=genus_str, mod_time=curr_time,
                        taxonomy_source_id=self._taxon_source_id,
                        taxonomy_source_key=taxon_key,
                        taxonomy_source_genus_key=genus_key,
                        taxonomy_source_species_key=species_key)
                    try:
                        sci_name = self._scribe.find_or_insert_taxon(
                            sci_name=s_name)
                        self.log.info(
                            'Inserted sci_name for taxon_key {}, {}'.format(
                                taxon_key, sci_name.scientific_name))
                    except LMError as lme:
                        raise lme
                    except Exception as err:
                        raise LMError(
                            'Failed on taxon_key {}, linenum {}'.format(
                                taxon_key, self._line_num),
                            err, line_num=self.get_line_num())
                else:
                    self.log.info(
                        'Taxon key ({}) is not accepted'.format(taxon_key))
        return sci_name

    # ................................
    @staticmethod
    def _raise_subclass_error():
        raise LMError('Function must be implemented in subclass')

    # ................................
    def _write_raw_data(self, occ, data=None, metadata=None):
        self._raise_subclass_error()

    # ................................
    def close(self):
        """Must be implemented in subclass
        """
        self._raise_subclass_error()

    # ................................
    @property
    def complete(self):
        """Must be implemented in subclass
        """
        self._raise_subclass_error()

    # ................................
    @property
    def next_start(self):
        """Must be implemented in subclass
        """
        self._raise_subclass_error()

    # ................................
    @property
    def this_start(self):
        """Must be implemented in subclass
        """
        self._raise_subclass_error()

    # ................................
    def move_to_start(self):
        """Must be implemented in subclass
        """
        self._raise_subclass_error()

    # ................................
    @property
    def curr_rec_num(self):
        """Get the current record number
        """
        if self.complete:
            return 0

        return self._line_num


# ..............................................................................
class UserWoC(_SpeciesWeaponOfChoice):
    """User weapon of choice.

    Parses a CSV file (with headers) of Occurrences using a metadata file.  A
    template for the metadata, with instructions, is at
    LmDbServer/tools/occurrence.meta.example.  The parser writes each new text
    chunk to a file, inserts or updates the Occurrence record and inserts any
    dependent objects.

    Note:
        If use_gbif_taxonomy is true, the 'GroupBy' field in the metadata
            should name the field containing the GBIF TaxonID for the accepted
            Taxon of each record in the group.
    """

    # ................................
    def __init__(self, scribe, user, archive_name, epsg, exp_date,
                 user_occ_csv, user_occ_meta, user_occ_delimiter,
                 logger=None, 
                 replace_fname=None, replace_fldname=None,
                 use_gbif_taxonomy=False,
                 taxon_source_name=None):
        super(UserWoC, self).__init__(
            scribe, user, archive_name, epsg, exp_date, user_occ_csv,
            meta_fname=user_occ_meta, taxon_source_name=taxon_source_name,
            logger=logger)
        # Save key/value replacements for generic lookup
        self._replacements = {}
        self._replace_col = None
        if (
            replace_fname is not None 
            and replace_fldname is not None 
            and os.path.exists(replace_fname)
            ):
            try:
                self._replacements, self._replace_col = self._read_replacements(
                    replace_fname, replace_fldname)
            except Exception:
                pass

        # User-specific attributes
        self.process_type = ProcessType.USER_TAXA_OCCURRENCE
        self.use_gbif_taxonomy = use_gbif_taxonomy
        self._user_occ_csv = user_occ_csv
        self._user_occ_meta = user_occ_meta
        self._delimiter = user_occ_delimiter
        self.occ_parser = None
        self._field_names = None

    # ................................
    def initialize_me(self):
        """Creates objects for walking species and computation requests.
        """
        try:
            self.occ_parser = OccDataParser(
                self.log, self._user_occ_csv, self._user_occ_meta,
                delimiter=self._delimiter, pull_chunks=True)
        except Exception as e:
            raise LMError('Failed to construct OccDataParser, {}'.format(e))

        self._field_names = self.occ_parser.header
        self.occ_parser.initialize_me()

    # ................................
    def close(self):
        """Close the WoC
        """
        try:
            self.occ_parser.close()
        except Exception:
            try:
                data_name = self.occ_parser.data_fname
            except Exception:
                data_name = None
            self.log.error(
                'Unable to close OccDataParser with file/data {}'.format(
                    data_name))

    # ................................
    @property
    def complete(self):
        """Return boolean indication if the WoC is complete
        """
        try:
            return self.occ_parser.closed
        except Exception:
            return True

    # ................................
    @property
    def this_start(self):
        """Get this start line
        """
        if self.complete:
            return 0

        try:
            return self.occ_parser.key_first_rec
        except Exception:
            return 0

    # ................................
    @property
    def next_start(self):
        """Get the next start line
        """
        if self.complete:
            return 0

        try:
            return self.occ_parser.curr_rec_num
        except Exception:
            return 0

    # ................................
    @property
    def curr_rec_num(self):
        """Get the current record number
        """
        if self.complete:
            return 0

        try:
            return self.occ_parser.curr_rec_num
        except Exception:
            return 0

    # ................................
    def move_to_start(self):
        """Move to the starting line
        """
        start_line = self._find_start()
        # Assumes first line is header
        if start_line > 2:
            self.occ_parser.skip_to_record(start_line)
        elif start_line < 0:
            self._curr_rec = None
    
    # ................................
    def _replace_lookup_keys(self, data_chunk):
        chunk = []
        for line in data_chunk:
            try:
                replace_key = line[self._replace_col]
            except KeyError:
                self.log.debug(
                    'Failed to find replacement key on record {} ({})'.format(
                        self._line_num, line))
            else:
                replace_val = replace_key
                try:
                    replace_val = self._replacements[replace_key]
                except KeyError:
                    pass
                    # TODO: Use API to query for, then save, value
                    # try:
                    #     replace_val = GbifAPI.get_name_from_key(replace_key)
                    # except:
                    #     self.log.debug(
                    #         'Failed to find key {}'.format(replace_key))
                else:
                    self._replacements[replace_key] = replace_val
    
                line[self._replace_col] = replace_val
                chunk.append(line)
        return chunk
    

    # ................................
    def get_one(self):
        """Get one occurrence layer

        Create and return an OccurrenceLayer from a chunk of CSV records
        grouped by a GroupBy value indicating species, possibly a GBIF
        `taxon_key`

        Note:
            - If use_gbif_taxonomy is true:
                - the `taxon_key` will contain the GBIF TaxonID for the
                    accepted Taxon of each record in the chunk, and a taxon
                    record will be retrieved (if already present) or queried
                    from GBIF and inserted
                - the OccurrenceLayer.displayname will use the resolved GBIF
                    canonical name
            - If taxon_name is missing, and use_gbif_taxonomy is False,
                the OccurrenceLayer.displayname will use the GroupBy value
        """
        occ = None
        (data_chunk, taxon_key, taxon_name
         ) = self.occ_parser.pull_current_chunk()
        if data_chunk:
            # TODO: enable generic replacement lookup
            # if self._replacements and self._replace_col:
            #     data_chunk = self._replace_lookup_keys(data_chunk)
            # Get or insert ScientificName (squid)
            if self.use_gbif_taxonomy:
                # returns None if GBIF API does NOT return this or another key
                #    as ACCEPTED
                sci_name = self._get_insert_sci_name_for_gbif_species_key(
                    taxon_key, len(data_chunk))
            else:
                if not taxon_name:
                    taxon_name = taxon_key
                bbsci_name = ScientificName(taxon_name, user_id=self.user_id)
                sci_name = self._scribe.find_or_insert_taxon(
                    sci_name=bbsci_name)

            if sci_name is not None:
                occ = self._find_or_insert_occurrence_set(
                    sci_name, len(data_chunk), data=data_chunk,
                    metadata=self.occ_parser.column_meta)
                if occ is not None:
                    self.log.info(
                        'WoC processed occ set {}, {}; next start {}'.format(
                            occ.get_id(),
                            'name: {}, num records: {}'.format(
                                sci_name.scientific_name, len(data_chunk)),
                            self.next_start))
        return occ

    # ................................
    def _write_raw_data(self, occ, data=None, metadata=None):
        raw_dloc = occ.create_local_dlocation(raw=True)
        ready_filename(raw_dloc)

        with open(raw_dloc, 'w', encoding=ENCODING) as out_file:
            writer = csv.writer(out_file, delimiter=self._delimiter)

            try:
                for rec in data:
                    writer.writerow(rec)
            except Exception as err:
                raw_dloc = None
                self.log.debug(
                    'Unable to write CSV file {} ({})'.format(raw_dloc, err))
            else:
                # Write interpreted metadata along with raw CSV
                raw_meta_dloc = raw_dloc + LMFormat.JSON.ext
                ready_filename(raw_meta_dloc, overwrite=True)
                with open(raw_meta_dloc, 'w', encoding=ENCODING) as meta_f:
                    json.dump(metadata, meta_f)
        return raw_dloc, raw_meta_dloc


# ..............................................................................
class TinyBubblesWoC(_SpeciesWeaponOfChoice):
    """Moves multipe csv files

    Moves multiple csv occurrence files (pre-parsed by taxa, with or without
    headers).  A template for the metadata, with instructions, is at
    LmDbServer/tools/occurrence.meta.example.  The WOC renames and moves each
    csv file to the correct location, inserts or updates the Occurrence record
    and inserts any dependent objects.

    Note:
        If use_gbif_taxonomy is true, the 'GroupBy' field in the metadata
            should name the field containing the GBIF TaxonID for the accepted
            Taxon of each record in the group.
    """

    # ................................
    def __init__(self, scribe, user, archive_name, epsg, exp_date, occ_csv_dir,
                 occ_meta, occ_delimiter, dir_contents_fname, logger=None,
                 process_type=ProcessType.USER_TAXA_OCCURRENCE,
                 use_gbif_taxonomy=False, taxon_source_name=None):
        """Constructor
        """
        super(TinyBubblesWoC, self).__init__(
            scribe, user, archive_name, epsg, exp_date, occ_csv_dir,
            meta_fname=occ_meta, taxon_source_name=taxon_source_name,
            logger=logger)
        # specific attributes
        self.process_type = ProcessType.USER_TAXA_OCCURRENCE
        self._occ_csv_dir = occ_csv_dir
        self._occ_meta = occ_meta
        self._delimiter = occ_delimiter
        self._dir_contents_file = None
        self._update_file(dir_contents_fname, exp_date)
        try:
            self._dir_contents_file = open(dir_contents_fname, 'r', 
                                           encoding=ENCODING)
        except IOError:
            raise LMError('Unable to open {}'.format(dir_contents_fname))
        self.use_gbif_taxonomy = use_gbif_taxonomy

    # ................................
    def _parse_bubble(self, bubble_fname):
        """Parse a bubble

        Todo:
            This method should either get OpenTree ID from filename or some
                taxon ID (GBIF) from record/s.

        Returns:
            (specie_name, open_tree_id, record_count)
        """
        binomial = open_tree_id = None
        if bubble_fname is not None:
            _, fname = os.path.split(bubble_fname)
            basename, _ = os.path.splitext(fname)
            parts = basename.split('_')
            if len(parts) >= 2:
                genus = parts[0]
                species = parts[1]
                try:
                    idstr = parts[2]
                    open_tree_id = int(idstr)
                except (ValueError, IndexError):
                    self.log.error(
                        'Unable to get int open tree id from file {}'.format(
                            basename))
                binomial = ' '.join((genus, species))
            else:
                self.log.error(
                    'Unable to parse {} into binomial and ottid'.format(
                        basename))

        idx = 0
        with open(bubble_fname, 'r', encoding=ENCODING) as in_file:
            for idx, _ in enumerate(in_file):
                pass
        record_count = idx

        return binomial, open_tree_id, record_count

    # ................................
    def _get_insert_sci_name_for_tiny_bubble(self, binomial, open_tree_id,
                                             record_count):
        if binomial is not None:
            if open_tree_id is not None:
                sci_name = ScientificName(
                    binomial, last_occurrence_count=record_count,
                    taxonomy_source_id=self._taxon_source_id,
                    taxonomy_source_key=open_tree_id,
                    taxonomy_source_species_key=open_tree_id)
            else:
                sci_name = ScientificName(
                    binomial, user_id=self.user_id,
                    last_occurrence_count=record_count)
            self._scribe.findOrInsertTaxon(sci_name=sci_name)
            self.log.info(
                'Inserted sci_name for OpenTree UID {}, {}'.format(
                    open_tree_id, binomial))

        return sci_name

    # ................................
    def close(self):
        """Close the WoC
        """
        try:
            self._dir_contents_file.close()
        except Exception:
            self.log.error(
                'Unable to close dirContentsFile {}'.format(
                    self._dir_contents_file))

    # ................................
    @property
    def next_start(self):
        """Get the next starting location
        """
        if self.complete:
            return 0

        return self._line_num + 1

    # ................................
    @property
    def this_start(self):
        """Get this starting location
        """
        if self.complete:
            return 0

        return self._line_num

    # ................................
    @property
    def complete(self):
        """Return indication if WoC is complete
        """
        try:
            return self._dir_contents_file.closed
        except Exception:
            return True

    # ................................
    def _update_file(self, filename, exp_date):
        """If file does not exist or is older than exp_date, create a new file.
        """
        if filename is None or not os.path.exists(filename):
            self._recreate_file(filename)
        elif exp_date is not None:
            ticktime = os.path.getmtime(filename)
            mod_time = LmTime(
                dtime=datetime.datetime.fromtimestamp(ticktime)).mjd
            if mod_time < exp_date:
                self._recreate_file(filename)

    # ................................
    def _recreate_file(self, dir_contents_fname):
        """Create a new file from BISON query for matches with > 20 points.
        """
        self.ready_filename(dir_contents_fname, overwrite=True)
        with open(dir_contents_fname, 'w', encoding=ENCODING) as out_f:
            for root, _, files in os.walk(self._occ_csv_dir):
                for fname in files:
                    if fname.endswith(LMFormat.CSV.ext):
                        full_fname = os.path.join(root, fname)
                        out_f.write('{}\n'.format(full_fname))

    # ................................
    def _get_next_filename(self):
        full_occ_fname = None
        line = self._get_next_line(self._dir_contents_file)
        if line is not None:
            try:
                full_occ_fname = line.strip()
            except Exception as e:
                self.log.debug(
                    'Exception reading line {} ({})'.format(
                        self._line_num, e))
        return full_occ_fname

    # ................................
    def get_one(self):
        """Get one occurrence set
        """
        occ = None
        bubble_fname = self._get_next_filename()
        binomial, open_tree_id, record_count = self._parse_bubble(bubble_fname)
        if binomial is not None and open_tree_id is not None:
            sci_name = self._get_insert_sci_name_for_tiny_bubble(
                binomial, open_tree_id, record_count)
            if sci_name is not None:
                occ = self._find_or_insert_occurrence_set(
                    sci_name, record_count, data=bubble_fname)
            if occ:
                self.log.info(
                    'WoC processed occ set {}, open tree id {}; {}'.format(
                        occ.get_id(), open_tree_id,
                        'with {} points, next start {}'.format(
                            record_count, self.next_start)
                        ))
        return occ

    # ................................
    def _write_raw_data(self, occ, data=None, metadata=None):
        if data is None:
            raise LMError('Missing data file for occ_layer')
        raw_dloc = occ.create_local_dlocation(raw=True)
        occ.ready_filename(raw_dloc, overwrite=True)
        shutil.copyfile(data, raw_dloc)

        if metadata is not None:
            rawmeta_dloc = raw_dloc + LMFormat.JSON.ext
            ready_filename(rawmeta_dloc, overwrite=True)
            with open(rawmeta_dloc, 'w', encoding=ENCODING) as out_f:
                json.dump(metadata, out_f)
        return raw_dloc, rawmeta_dloc

    # ................................
    def move_to_start(self):
        """Move to the starting line
        """
        start_line = self._find_start()
        if start_line < 1:
            self._line_num = 0
            self._curr_rec = None
        else:
            full_occ_fname = self._get_next_filename()
            while full_occ_fname is not None and \
                    self._line_num < start_line - 1:
                full_occ_fname = self._get_next_filename()


# ..............................................................................
class ExistingWoC(_SpeciesWeaponOfChoice):
    """Parse GBIF download for occurrence data

    Parses a GBIF download of Occurrences by GBIF Taxon ID, writes the text
    chunk to a file, then creates an OccurrenceJob for it and updates the
    Occurrence record and inserts a job.
    """

    # ................................
    def __init__(self, scribe, user, archive_name, epsg, exp_date,
                 occ_id_fname, logger=None):
        super(ExistingWoC, self).__init__(
            scribe, user, archive_name, epsg, exp_date, occ_id_fname,
            logger=logger)
        # Copy the occurrencesets
        self.process_type = None
        try:
            self._id_file = open(occ_id_fname, 'r', encoding=ENCODING)
        except IOError:
            raise LMError('Failed to open {}'.format(occ_id_fname))

    # ................................
    def close(self):
        """Close the WoC
        """
        try:
            self._id_file.close()
        except Exception:
            self.log.error('Unable to close id file')

    # ................................
    @property
    def complete(self):
        """Return indication if WoC is complete
        """
        try:
            return self._id_file.closed
        except Exception:
            return True

    # ................................
    @property
    def next_start(self):
        """Get the next start position
        """
        if self.complete:
            return 0

        return self._line_num

    # ................................
    @property
    def this_start(self):
        """Get this start location
        """
        if self.complete:
            return 0

        return self._curr_key_first_rec_num

    # ................................
    def move_to_start(self):
        """Move to the starting location
        """
        start_line = self._find_start()
        if start_line > 1:
            while self._line_num < start_line - 1:
                _ = self._get_next_line(self._id_file)

    # ................................
    def _get_occ(self):
        occ = None
        line = self._get_next_line(self._id_file)
        while line is not None and not self.complete:
            try:
                tmp = line.strip()
            except Exception as e:
                self._scribe.log.info(
                    'Error reading line {} ({}), skipping'.format(
                        self._line_num, str(e)))
            else:
                try:
                    occ_id = int(tmp)
                except Exception:
                    self._scribe.log.info(
                        'Unable to get Id from data {} on line {}'.format(
                            tmp, self._line_num))
                else:
                    occ = self._scribe.get_occurrence_set(occ_id=occ_id)
                    if occ is None:
                        self._scribe.log.info(
                            'Unable to get Occset for Id {} on line {}'.format(
                                tmp, self._line_num))
                    else:
                        if occ.status != JobStatus.COMPLETE:
                            self._scribe.log.info(
                                'Incomplete or failure for occ {}; {}'.format(
                                    occ_id, 'on line {}'.format(
                                        self._line_num)))
            line = None
            if occ is None and not self.complete:
                line = self._get_next_line(self._id_file)
        return occ

    # ................................
    def get_one(self):
        """Get data for one species
        """
        user_occ = None
        occ = self._get_occ()
        if occ is not None:
            if occ.get_user_id() == self.user_id:
                user_occ = occ
                self.log.info(
                    'Found user occset {}, with {} points; {}'.format(
                        occ.get_id(), occ.query_count,
                        'next start {}'.format(self.next_start)))
            elif occ.get_user_id() == PUBLIC_USER:
                tmp_occ = occ.copy_for_user(self.user_id)
                sci_name = self._scribe.get_taxon(squid=occ.squid)
                if sci_name is not None:
                    tmp_occ.set_scientific_name(sci_name)
                tmp_occ.read_data(
                    dlocation=occ.get_dlocation(), data_format=occ.data_format)
                user_occ = self._scribe.find_or_insert_occurrence_set(tmp_occ)
                # Read the data from the original occurrence set
                user_occ.read_data(
                    dlocation=occ.get_dlocation(), data_format=occ.data_format,
                    do_read_data=True)
                user_occ.write_layer()

                # Copy metadata file
                shutil.copyfile(
                    '{}{}'.format(
                        os.path.splitext(occ.get_dlocation())[0],
                        LMFormat.METADATA.ext),
                    '{}{}'.format(
                        os.path.splitext(
                            user_occ.get_dlocation())[0],
                        LMFormat.METADATA.ext))

                self._scribe.update_object(user_occ)
                self.log.info(
                    'Copy/insert occset {} to {}, with {} points; {}'.format(
                        occ.get_id(), user_occ.get_id(), user_occ.query_count,
                        'next start {}'.format(self.next_start)))
            else:
                self._scribe.log.info(
                    'Unauthorized user {} for ID {}'.format(
                        occ.get_user_id(), occ.get_id()))
        return user_occ

"""
import csv
import datetime
import json
import os
import shutil
import sys

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.api_query import GbifAPI
from LmCommon.common.lmconstants import (
    GBIF, JobStatus, LMFormat, ONE_HOUR, ProcessType, ENCODING)
from LmCommon.common.occ_parse import OccDataParser
from LmCommon.common.ready_file import ready_filename
from LmCommon.common.time import gmt, LmTime
from LmServer.base.taxon import ScientificName
from LmServer.common.data_locator import EarlJr
from LmServer.common.localconstants import PUBLIC_USER
from LmServer.common.log import ScriptLogger
from LmServer.legion.occ_layer import OccurrenceLayer

TROUBLESHOOT_UPDATE_INTERVAL = ONE_HOUR

occ = None
(data_chunk, taxon_key, taxon_name
 ) = self.occ_parser.pull_current_chunk()

sci_name = self._get_insert_sci_name_for_gbif_species_key(
    taxon_key, len(data_chunk))

occ = self._find_or_insert_occurrence_set(
    sci_name, len(data_chunk), data=data_chunk,
    metadata=self.occ_parser.column_meta)
if occ is not None:
    self.log.info(
        'WoC processed occ set {}, {}; next start {}'.format(
            occ.get_id(),
            'name: {}, num records: {}'.format(
                sci_name.scientific_name, len(data_chunk)),
            self.next_start))


"""