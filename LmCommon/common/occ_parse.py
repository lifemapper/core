"""Occurrence data parser
"""
import csv
import json
import os
import sys

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import (
    LMFormat, OFTInteger, OFTReal, OFTString, ENCODING)

from LmCommon.common.log import TestLogger

# TODO: Move these to a testing module
try:
    from LmServer.common.localconstants import APP_PATH
except ImportError:
    try:
        from LmCompute.common.localconstants import LM_PATH as APP_PATH
    except ImportError:
        raise Exception('Testing must be done on a Lifemapper instance')


# .............................................................................
class OccDataParser(LMObject):
    """Object with metadata and open file.

    OccDataParser maintains file position and most recently read data chunk
    """
    FIELD_NAME_KEY = 'name'
    FIELD_TYPE_KEY = 'type'
    FIELD_ROLE_KEY = 'role'
    FIELD_VALS_KEY = 'acceptedvals'

    FIELD_ROLE_IDENTIFIER = 'uniqueid'
    FIELD_ROLE_LONGITUDE = 'longitude'
    FIELD_ROLE_LATITUDE = 'latitude'
    FIELD_ROLE_GEOPOINT = 'geopoint'
    FIELD_ROLE_GROUPBY = 'groupby'
    FIELD_ROLE_TAXANAME = 'taxaname'
    FIELD_ROLES = [
        FIELD_ROLE_LONGITUDE, FIELD_ROLE_LATITUDE, FIELD_ROLE_GEOPOINT,
        FIELD_ROLE_GROUPBY, FIELD_ROLE_TAXANAME, FIELD_ROLE_IDENTIFIER]

    # ......................................
    def __init__(self, logger, csv_data_or_fname, metadata, delimiter='\t',
                 pull_chunks=False):
        """Reader for arbitrary user CSV data

        Args:
            logger: Logger to use for the main thread
            data: raw data or filename for CSV data
            metadata: dictionary or filename containing metadata
            delimiter: delimiter of values in csv records
            pull_chunks: use the object to pull chunks of data based on the
                groupBy column.  This results in 'pre-fetching' a line of data
                at the start of a chunk to establish the group, and identifying
                the end of a chunk when the current line does not match the
                existing chunk.


        Note:
            - header record and metadata with column **names** first, OR
            - no header and and metadata with column **positions** first
        """
        self._raw_metadata = metadata
        self._field_meta = None
        self._metadata_f_name = None
        self._do_prefetch = pull_chunks

        self.delimiter = delimiter
        self.csv_fname = csv_data_or_fname

        self._file = open(csv_data_or_fname, 'r', encoding=ENCODING)
        self._csv_reader = csv.reader(self._file, delimiter=delimiter)
        if self._file is None:
            self.csv_fname = None

        self.log = logger
        self.field_count = 0

        # Record values to check
        self.filters = {}
        self._id_idx = None
        self._x_idx = None
        self._y_idx = None
        self._geo_idx = None
        self._group_by_idx = None
        self._name_idx = None

        # Overall stats
        self.rec_total = 0
        self.rec_total_good = 0
        self.group_total = 0
        self.group_vals = set()
        self.bad_ids = 0
        self.bad_geos = 0
        self.bad_groups = 0
        self.bad_names = 0
        self.bad_filters = 0
        self.bad_filter_vals = set()

        self.chunk = []
        self.group_val = None
        self.group_first_rec = 0
        self.curr_line = None

        self.header = None
        self.field_count = None
        self.group_first_rec = None
        self.curr_is_good_enough = None
        self.column_meta = None

    # ......................................
    def initialize_me(self):
        """Initializes CSV Reader and interprets metadata
        """
        # fieldmeta, self._metadataFname, doMatchHeader = self.readMetadata(
        #    self._rawMetadata)
        field_meta, do_match_header = self.read_metadata(self._raw_metadata)
        if do_match_header:
            # Read CSV header
            tmp_list = next(self._csv_reader)
            print(('Header = {}'.format(tmp_list)))
            self.header = [fld_name.strip() for fld_name in tmp_list]

        (self.column_meta, self.filters, self._id_idx, self._x_idx,
         self._y_idx, self._geo_idx, self._group_by_idx, self._name_idx
         ) = self.get_check_indexed_metadata(field_meta, self.header)
        self.field_count = len(self.column_meta)

        # Start by pulling line 1; populates groupVal, currLine and currRecnum
        if self._do_prefetch:
            self.pull_next_valid_rec()
        # record number of the chunk of current key
        self.group_first_rec = self.curr_rec_num
        self.curr_is_good_enough = True

    # .............................................................................
    @property
    def curr_rec_num(self):
        """Get the current record number
        """
        if self._csv_reader:
            return self._csv_reader.line_num
        if self.closed:
            return -9999
        return None

    # ......................................
    @property
    def group_by_idx(self):
        """The group by index of the csv file
        """
        return self._group_by_idx

    # ......................................
    @property
    def id_value(self):
        """The id value of the current record
        """
        if self.curr_line and self._id_idx:
            return self.curr_line[self._id_idx]
        return None

    # ......................................
    @property
    def group_by_value(self):
        """The value of the group by field for the current record
        """
        if self.curr_line:
            tmp = self.curr_line[self._group_by_idx]
            try:
                return int(tmp)
            except ValueError:
                return str(tmp)
        return None

    # ......................................
    @property
    def name_value(self):
        """Return the name value for the current record
        """
        if self.curr_line:
            return self.curr_line[self._name_idx]
        return None

    # ......................................
    @property
    def name_idx(self):
        """The index of the name field
        """
        return self._name_idx

    # ......................................
    @property
    def id_field_name(self):
        """Return the id field name
        """
        if self._id_idx:
            return self.column_meta[self._id_idx][self.FIELD_NAME_KEY]
        return None

    # ......................................
    @property
    def id_idx(self):
        """Return the id field index
        """
        return self._id_idx

    # ......................................
    @property
    def x_field_name(self):
        """Return the name of the x field
        """
        try:
            return self.column_meta[self._x_idx][self.FIELD_NAME_KEY]
        except Exception:
            return None

    # ......................................
    @property
    def x_idx(self):
        """Return the x field index
        """
        return self._x_idx

    # ......................................
    @property
    def y_field_name(self):
        """Return the name of the y field
        """
        try:
            return self.column_meta[self._y_idx][self.FIELD_NAME_KEY]
        except Exception:
            return None

    # ......................................
    @property
    def y_idx(self):
        """Return the y field index
        """
        return self._y_idx

    # ......................................
    @property
    def pt_field_name(self):
        """Return the name of the point field
        """
        try:
            return self.column_meta[self._geo_idx][self.FIELD_NAME_KEY]
        except Exception:
            return None

    # ......................................
    @property
    def pt_idx(self):
        """Return the pt field index
        """
        return self._geo_idx

    # ......................................
    @staticmethod
    def read_metadata(metadata):
        """Reads metadata describing the CSV file of species data.

        Returns:
            dict - a dictionary with
                Key = column name or column index
               Value = dictionary of keys 'name', 'type', 'role', and
                   'acceptedVals' values

        Note:
            A full description of the input data is at
               LmDbServer/boom/occurrence.meta.example
        """
        meta = None
        # Read as JSON
        try:
            # from file
            with open(metadata, 'r', encoding=ENCODING) as in_file:
                meta = json.load(in_file)
        except IOError as io_err:
            print(('Failed to open {} err: {}'.format(metadata, str(io_err))))
            raise
        except Exception:
            # or string/stream
            try:
                meta = json.loads(metadata)
            # or parse oldstyle CSV
            except Exception:
                with open(metadata, 'r', encoding=ENCODING) as in_file:
                    meta_lines = in_file.readlines()
                meta = OccDataParser.read_old_metadata(meta_lines)

        # Convert fieldtype string to OGR constant
        for col_idx in list(meta.keys()):
            f_type = meta[col_idx][OccDataParser.FIELD_TYPE_KEY]
            ogr_type = OccDataParser.get_ogr_field_type(f_type)
            meta[col_idx][OccDataParser.FIELD_TYPE_KEY] = ogr_type

        # If keys are column indices, change to ints
        do_match_header = False
        column_meta = {}
        for k, val in meta.items():
            try:
                column_meta[int(k)] = val
            except ValueError:
                do_match_header = True
                break
        if not do_match_header:
            meta = column_meta

        return meta, do_match_header

    # ......................................
    @staticmethod
    def read_old_metadata(meta_lines):
        """Reads a stream/string of metadata describing a CSV file

        Returns:
            dict - a dictionary with
                Key = column name or column index
                Value = dictionary of keys 'name', 'type', 'role', and
                    'acceptedVals' values

        Note:
            A full description of the input data is at
               LmDbServer/boom/occurrence.meta.example
        """
        field_meta = {}
        try:
            for line in meta_lines:
                if not line.startswith('#'):
                    tmp = line.split(',')
                    parts = [p.strip() for p in tmp]
                    # First value is original fieldname or column index
                    key = parts[0]
                    try:
                        key = int(parts[0])
                    except Exception:
                        if len(key) == 0:
                            key = None
                    if key is not None:
                        if len(tmp) < 3:
                            print((
                                'Skip field {} without name or type'.format(
                                    key)))
                            field_meta[key] = None
                        else:
                            # Required second value is fieldname, must
                            # be 10 chars or less to write to a shapefile
                            # Required third value is string/real/integer or
                            #    None to ignore
                            field_meta[key] = {
                                OccDataParser.FIELD_NAME_KEY: parts[1],
                                OccDataParser.FIELD_TYPE_KEY: parts[2]}
                            # Optional remaining values are role and/or
                            #    allowable values
                            if len(parts) >= 4:
                                # Convert to lowercase
                                rest = []
                                for val in parts[3:]:
                                    try:
                                        rest.append(val.lower())
                                    except Exception:
                                        rest.append(val)
                                # If there are 4+ values, fourth may be role of
                                #    this field:
                                #   longitude, latitude, geopoint, groupby,
                                #        taxaname, uniqueid
                                # Convert to lowercase
                                if rest[0] in OccDataParser.FIELD_ROLES:
                                    field_meta[key][
                                        OccDataParser.FIELD_ROLE_KEY] = rest[0]
                                    rest = rest[1:]
                                # Remaining values are acceptable values for
                                #     this field
                                if len(rest) >= 1:
                                    field_meta[key][
                                        OccDataParser.FIELD_VALS_KEY] = rest
        except Exception as err:
            raise LMError('Failed to parse metadata, ({})'.format(err), err)

        return field_meta

    # ......................................
    @staticmethod
    def get_check_indexed_metadata(field_meta, header):
        """Identify data columns from metadata dictionary and optional header

        Args:
            field_meta: Dictionary of field names, types, roles, and accepted
                values.  If the first level 'Value' is None, this field will be
                ignored:
                    - Key = name in header or column index
                    - Value = None or Dictionary of
                        key = ['name', 'type', optional 'role', and optional
                        'accepted_vals'] values for those items
                    Keywords identify roles for x, y, id, grouping, taxa name.
            header: First row of data file containing field names for values
                        in subsequent rows. Field names match those in
                        field_meta dictionary

        Returns:
            List of: field_name list (order of data columns) field_type list
                (order of data columns) dictionary of filters for accepted
                values for zero or more fields, keys are the new field indexes
                column indexes for id, x, y, geopoint, groupBy, and name fields
        """
        filters = {}
        id_idx = x_idx = y_idx = pt_idx = group_by_idx = name_idx = None
        # If necessary, build new metadata dict with column indexes as keys
        if header is None:
            # keys are column indexs
            field_index_meta = field_meta
        else:
            # keys are fieldnames
            field_index_meta = {}
            for i, hdr in enumerate(header):
                try:
                    field_index_meta[i] = field_meta[hdr]
                except AttributeError:
                    field_index_meta[i] = None

        for idx, vals in field_index_meta.items():
            # add placeholders in the fieldnames and fieldTypes lists for
            # columns we will not process
            ogr_type = role = accepted_vals = None
            if vals is not None:
                # Get required vals for columns to save
                _name = field_index_meta[idx][OccDataParser.FIELD_NAME_KEY]
                ogr_type = field_index_meta[idx][OccDataParser.FIELD_TYPE_KEY]
                # Check for optional filter AcceptedValues.
                try:
                    accepted_vals = field_index_meta[idx][
                        OccDataParser.FIELD_VALS_KEY]
                except Exception:
                    pass
                else:
                    # Convert acceptedVals to lowercase
                    if ogr_type == OFTString:
                        field_index_meta[idx][
                            OccDataParser.FIELD_VALS_KEY] = [
                                val.lower() for val in accepted_vals]
                # Find column index of important fields
                try:
                    role = field_index_meta[idx][
                        OccDataParser.FIELD_ROLE_KEY].lower()
                except Exception:
                    pass
                else:
                    # If role exists, convert to lowercase
                    field_index_meta[idx]['role'] = role
                    if role == OccDataParser.FIELD_ROLE_IDENTIFIER:
                        id_idx = idx
                        print(('Found id index {}').format(idx))
                    elif role == OccDataParser.FIELD_ROLE_LONGITUDE:
                        x_idx = idx
                        print(('Found X index {}').format(idx))
                    elif role == OccDataParser.FIELD_ROLE_LATITUDE:
                        y_idx = idx
                        print(('Found Y index {}').format(idx))
                    elif role == OccDataParser.FIELD_ROLE_GEOPOINT:
                        pt_idx = idx
                        print(('Found point index {}').format(idx))
                    elif role == OccDataParser.FIELD_ROLE_TAXANAME:
                        name_idx = idx
                        print(('Found name index {}').format(idx))
                    elif role == OccDataParser.FIELD_ROLE_GROUPBY:
                        group_by_idx = idx
                        print(('Found group index {}').format(idx))
            filters[idx] = accepted_vals

        if name_idx is None:
            raise Exception('Missing `TAXANAME` required role in metadata')
        if (x_idx is None or y_idx is None) and pt_idx is None:
            print(('Found x {}, y {}, point {}').format(x_idx, y_idx, pt_idx))
            raise LMError(
                'Missing `{}`-`{}` pair or `{}` roles in metadata'.format(
                    'LATITUDE', 'LONGITUDE', 'GEOPOINT'))
        if group_by_idx is None:
            group_by_idx = name_idx
        return (
            field_index_meta, filters, id_idx, x_idx, y_idx, pt_idx,
            group_by_idx, name_idx)

    # ......................................
    @staticmethod
    def get_ogr_field_type(type_val):
        """Get the type of the OGR field
        """
        if type_val is None:
            return None
        try:
            type_int = int(type_val)
            if type_int in (OFTInteger, OFTString, OFTReal):
                return type_int

            raise LMError(
                'Field type must be OFTInteger, OFTString, ' +
                'OFTReal ({}, {}, {})'.format(OFTInteger, OFTString, OFTReal))
        except Exception:
            try:
                type_str = type_val.lower()
            except Exception:
                raise LMError(
                    'Field type must be coded as a string or integer')

            if type_str == 'none':
                return None
            if type_str in ('int', 'integer'):
                return OFTInteger
            if type_str in ('str', 'string'):
                return OFTString
            if type_str in ('float', 'real'):
                return OFTReal

            print(('Unsupported field type: {}, must be in {}'.format(
                type_str, '(None, int, string, real)')))
        return None

    # ......................................
    @staticmethod
    def get_xy(line, x_idx, y_idx, geo_idx):
        """Returns Longitude/X Latitude/Y from x, y fields or geopoint
        """
        x_val = y_val = None
        try:
            x_val = line[x_idx]
            y_val = line[y_idx]
        except (AttributeError, TypeError):
            point = line[geo_idx]
            new_pt = point.strip('{').strip('}')
            new_coords = new_pt.split(',')
            for coord in new_coords:
                try:
                    lat_idx = coord.index('lat')
                except Exception:
                    # Longitude
                    try:
                        lon_idx = coord.index('lon')
                    except Exception:
                        pass
                    else:
                        if lon_idx >= 0:
                            tmp = coord[lon_idx + 3:].strip()
                            x_val = tmp.replace(
                                '"', '').replace(
                                    ':', '').replace(',', '').strip()
                # Latitude
                else:
                    if lat_idx >= 0:
                        tmp = coord[lat_idx + 3:].strip()
                        y_val = tmp.replace(
                            '"', '').replace(':', '').replace(',', '').strip()
        return x_val, y_val

    # ......................................
    def _test_line(self, line):
        good_enough = True

        if len(line) == 1:
            self.log.info(
                'Line has only one element - is delimiter set correctly?')
        if len(line) < len(list(self.column_meta.keys())):
            raise LMError(
                'Line has {} elements; expecting {} fields'.format(
                    len(line), len(list(self.column_meta.keys()))))
        self.rec_total += 1

        # Field filters
        for filter_idx, accepted_vals in self.filters.items():
            val = line[filter_idx]
            try:
                val = val.lower()
            except Exception:
                pass
            if accepted_vals is not None and val not in accepted_vals:
                self.bad_filter_vals.add(val)
                self.bad_filters += 1
                good_enough = False

        # Sort/Group value; may be a string or integer
        try:
            grp_val = self._get_group_by_value(line)
        except Exception:
            self.bad_groups += 1
            good_enough = False
        else:
            self.group_vals.add(grp_val)

        # If present, unique ID value
        if self._id_idx is not None:
            try:
                int(line[self._id_idx])
            except Exception:
                if line[self._id_idx] == '':
                    self.bad_ids += 1
                    good_enough = False

        # Lat/long values
        x_val, y_val = self.get_xy(
            line, self._x_idx, self._y_idx, self._geo_idx)
        try:
            float(x_val)
            float(y_val)
        except Exception:
            self.bad_geos += 1
            good_enough = False
        else:
            if x_val == 0 and y_val == 0:
                self.bad_geos += 1
                good_enough = False

        if good_enough:
            self.rec_total_good += 1

        return good_enough

    # ......................................
    def _get_line(self):
        """
        """
        success = good_enough = False
        line = None
        while not success and self._csv_reader is not None:
            try:
                line = next(self._csv_reader)
                if len(line) > 0:
                    good_enough = self._test_line(line)
                    success = True
            except OverflowError as err:
                self.log.debug(
                    'Overflow on {}; {}'.format(self.curr_rec_num, err))
            except StopIteration:
                self.log.debug('EOF after rec {}'.format(self.curr_rec_num))
                self.close()
                self.curr_line = None
                success = True
            except Exception as err:
                self.log.warning('Bad record {}'.format(err))

        return line, good_enough

    # ......................................
    def skip_to_record(self, target_num):
        """Reads up to, not including, targetnum line.
        """
        while self.curr_line and self.curr_rec_num < target_num - 1:
            _ = self._get_line()

    # ......................................
    def read_all_recs(self):
        """Read all records

        Note:
            Does not check for goodEnough line
        """
        while self.curr_line is not None:
            _ = self._get_line()

    # ......................................
    # TODO: get rid of this, use property groupByValue
    def _get_group_by_value(self, line):
        try:
            value = int(line[self._group_by_idx])
        except Exception:
            try:
                value = str(line[self._group_by_idx])
            except Exception:
                value = None
        return value

    # ......................................
    def pull_next_valid_rec(self):
        """Fills in self.group_val and self.curr_line

        Todo:
            Get rid of self.groupVal, use property groupByValue
        """
        complete = False
        self.group_val = None
        line, good_enough = self._get_line()
        if self.closed:
            self.curr_line = self.group_val = None
        try:
            while self._csv_reader and not self.closed and not complete:
                if line and good_enough:
                    self.curr_line = line
                    # TODO: Remove groupBy from required fi
                    self.group_val = self._get_group_by_value(line)
                    complete = True
                # Keep pulling records until goodEnough
                if not complete:
                    line, good_enough = self._get_line()
                    if line is None:
                        complete = True
                        self.curr_line = None
                        self.group_val = None
                        self.log.info(
                            'Unable to pull_next_valid_rec; completed')

        except Exception as err:
            self.log.error(
                'Failed in pull_next_valid_rec, currRecnum={}, {}'.format(
                    self.curr_rec_num, err))
            self.curr_line = self.group_val = None

    # ......................................
    def print_stats(self):
        """Print stats
        """
        if not self.closed:
            self.log.error('{}; {}'.format(
                'File is on line {}'.format(self._csv_reader.line_num),
                'print_stats must be run after reading complete file'))
        else:
            report = """
            Totals for {}
            -------------------------------------------------------------
            Total records read: {}
            Total good records: {}
            Total groupings: {}
            Breakdown
            ----------
            Records with missing or invalid ID value: {}
            Records with missing or invalid Longitude/Latitude values: {}
            Records with missing or invalid GroupBy value: {}
            Records with missing or invalid Dataname value: {}
            Records with unacceptable value for filter fields: {}
               Indexes: {}
               Accepted vals: {}
               Bad filter values: {}
            """.format(
                self.csv_fname, self.rec_total, self.rec_total_good,
                len(self.group_vals), self.bad_ids, self.bad_geos,
                self.bad_groups, self.bad_names, self.bad_filters,
                list(self.filters.keys()), list(self.filters.values()),
                self.bad_filter_vals)
            self.log.info(report)

    # ......................................
    def pull_current_chunk(self):
        """Returns chunk for self.group_val
        """
        complete = False
        curr_count = 0
        chunk_group = self.group_by_value
        chunk_name = self.name_value
        chunk = []

        if self.curr_line is not None:
            # first line of chunk is currLine
            good_enough = self._test_line(self.curr_line)
            if good_enough:
                chunk.append(self.curr_line)

            try:
                while not self.closed and not complete:
                    # get next line
                    self.pull_next_valid_rec()

                    # Add to or complete chunk
                    if self.group_by_value == chunk_group:
                        curr_count += 1
                        chunk.append(self.curr_line)
                    else:
                        complete = True
                        self.group_first_rec = self.curr_rec_num

                    if self.curr_line is None:
                        complete = True

            except Exception as err:
                self.log.error(
                    'Failed in "pull_current_chunk", at {}, err: {}'.format(
                        self.curr_rec_num, err))
                self.curr_line = self.group_val = None
        return chunk, chunk_group, chunk_name

    # ......................................
    def read_all_chunks(self):
        """Read all chunks

        Note:
            Does not check for good_enough line
        """
        summary = {}
        while self.curr_line is not None:
            chunk, chunk_group, chunk_name = self.pull_current_chunk()
            summary[chunk_group] = (chunk_name, len(chunk))
            self.log.info(
                'Pulled chunk {} for name {} with {} records'.format(
                    chunk_group, chunk_name, len(chunk)))
        count = len(summary.keys())
        self.log.info('Pulled {} total chunks'.format(count))
        return summary

    # ......................................
    def get_size_chunk(self, max_size):
        """Get a chunk of maximum size
        """
        complete = False
        chunk = []
        try:
            while self._csv_reader is not None and not complete:
                chunk.append(self.curr_line)
                if self.curr_line is None or sys.getsizeof(chunk) >= max_size:
                    complete = True
                else:
                    self.pull_next_valid_rec()
        except Exception as err:
            self.log.error(
                'Failed in "get_size_chunk", currRecnum={}, e={}'.format(
                    self.curr_rec_num, err))
            self.curr_line = self.group_val = None
        return chunk

    # ......................................
    @property
    def closed(self):
        """Return boolean indicating if file is closed
        """
        return self._file.closed

    # ......................................
    def close(self):
        """Close the file
        """
        try:
            self._file.close()
        except Exception:
            pass
        self._csv_reader = None


# .............................................................................
def test_run():
    """Test the module

    Todo:
        Move this to a testing module
    """
    relpath = 'LmTest/data/sdm'

    #    dataname = 'gbif_borneo_simple'
    dataname = 'user_heuchera_all'

    pth_and_basename = os.path.join(APP_PATH, relpath, dataname)
    log = TestLogger('occparse_checkInput')
    occ_parser = OccDataParser(
        log, pth_and_basename + LMFormat.CSV.ext,
        pth_and_basename + LMFormat.METADATA.ext, pull_chunks=True)
    occ_parser.read_all_recs()
    occ_parser.print_stats()
    occ_parser.close()


# .............................................................................
if __name__ == '__main__':
    test_run()
