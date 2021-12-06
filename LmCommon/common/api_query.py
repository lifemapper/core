"""Module containing functions for API Queries
"""
from copy import copy
import csv
from http import HTTPStatus
import json
import os
import urllib

import requests

import idigbio

from LmCommon.common.lm_xml import fromstring, deserialize
from LmCommon.common.lmconstants import (
    BISON, BisonQuery, DwcNames, GBIF, Idigbio, IdigbioQuery, Itis, URL_ESCAPES, ENCODING)
from LmCommon.common.occ_parse import OccDataParser
from LmCommon.common.ready_file import ready_filename


# .............................................................................
class APIQuery:
    """Class to query APIs and return results.

    Note:
        CSV files are created with tab delimiter
    """
    DELIMITER = GBIF.DATA_DUMP_DELIMITER
    GBIF_MISSING_KEY = 'unmatched_gbif_ids'

    def __init__(self, base_url, q_key=None, q_filters=None,
                 other_filters=None, filter_string=None, headers=None):
        """
        @summary Constructor for the APIQuery class
        """
        self._q_key = q_key
        self.headers = {} if headers is None else headers
        # No added filters are on url (unless initialized with filters in url)
        self.base_url = base_url
        self._q_filters = {} if q_filters is None else q_filters
        self._other_filters = {} if other_filters is None else other_filters
        self.filter_string = self._assemble_filter_string(
            filter_string=filter_string)
        self.output = None
        self.debug = False

    # .....................................
    @classmethod
    def init_from_url(cls, url, headers=None):
        """Initialize APIQuery from a url

        Args:
            url (str): The url to use as the base
            headers (dict): Headers to use for query
        """
        if headers is None:
            headers = {}
        base, filters = url.split('?')
        qry = APIQuery(base, filter_string=filters, headers=headers)
        return qry

    # .........................................
    @property
    def url(self):
        """Retrieve a url for the query
        """
        # All filters added to url
        if self.filter_string and len(self.filter_string) > 1:
            return '{}?{}'.format(self.base_url, self.filter_string)

        return self.base_url

    # ...............................................
    def add_filters(self, q_filters=None, other_filters=None):
        """Add or replace filters.

        Note:
            This does not remove existing filters unless they are replaced
        """
        self.output = None
        q_filters = {} if q_filters is None else q_filters
        other_filters = {} if other_filters is None else other_filters

        for k, val in q_filters.items():
            self._q_filters[k] = val
        for k, val in other_filters.items():
            self._other_filters[k] = val
        self.filter_string = self._assemble_filter_string()

    # ...............................................
    def clear_all(self, q_filters=True, other_filters=True):
        """Clear existing q_filters, other_filters, and output
        """
        self.output = None
        if q_filters:
            self._q_filters = {}
        if other_filters:
            self._other_filters = {}
        self.filter_string = self._assemble_filter_string()

    # ...............................................
    def clear_other_filters(self):
        """Clear existing otherFilters and output
        """
        self.clear_all(other_filters=True, q_filters=False)

    # ...............................................
    def clear_q_filters(self):
        """Clear existing qFilters and output
        """
        self.clear_all(other_filters=False, q_filters=True)

    # ...............................................
    def _assemble_filter_string(self, filter_string=None):
        if filter_string is not None:
            for replace_str, with_str in URL_ESCAPES:
                filter_string = filter_string.replace(replace_str, with_str)
        else:
            all_filters = self._other_filters.copy()
            if self._q_filters:
                q_val = self._assemble_q_val(self._q_filters)
                all_filters[self._q_key] = q_val
            filter_string = self._assemble_key_val_filters(all_filters)
        return filter_string

    # ...............................................
    @staticmethod
    def _assemble_key_val_filters(of_dict):
        for k, val in of_dict.items():
            if isinstance(val, bool):
                val = str(val).lower()
            of_dict[k] = str(val).encode(ENCODING)
        filter_string = urllib.parse.urlencode(of_dict)
        return filter_string

    # ...............................................
    @staticmethod
    def _interpret_q_clause(key, val):
        cls = None
        if isinstance(val, (float, int, str)):
            cls = '{}:{}'.format(key, str(val))
        # Tuple for negated or range value
        elif isinstance(val, tuple):
            # negated filter
            if isinstance(val[0], bool) and val[0] is False:
                cls = 'NOT ' + key + ':' + str(val[1])
            # range filter (better be numbers)
            elif isinstance(
                    val[0], (float, int)) and isinstance(val[1], (float, int)):
                cls = '{}:[{} TO {}]'.format(key, str(val[0]), str(val[1]))
            else:
                print('Unexpected value type {}'.format(val))
        else:
            print('Unexpected value type {}'.format(val))
        return cls

    # ...............................................
    def _assemble_q_item(self, key, val):
        itm_clauses = []
        # List for multiple values of same key
        if isinstance(val, list):
            for list_val in val:
                itm_clauses.append(self._interpret_q_clause(key, list_val))
        else:
            itm_clauses.append(self._interpret_q_clause(key, val))
        return itm_clauses

    # ...............................................
    def _assemble_q_val(self, q_dict):
        clauses = []
        q_val = ''
        # interpret dictionary
        for key, val in q_dict.items():
            clauses.extend(self._assemble_q_item(key, val))
        # convert to string
        first_clause = ''
        for cls in clauses:
            if not first_clause and not cls.startswith('NOT'):
                first_clause = cls
            elif cls.startswith('NOT'):
                q_val = ' '.join((q_val, cls))
            else:
                q_val = ' AND '.join((q_val, cls))
        q_val = first_clause + q_val
        return q_val

    # ...............................................
    def query_by_get(self, output_type='json'):
        """Queries the API and sets 'output' attribute to a JSON object
        """
        self.output = None
        ret_code = None
        try:
            response = requests.get(self.url, headers=self.headers)
        except Exception as e:
            try:
                ret_code = response.status_code
                reason = response.reason
            except AttributeError:
                reason = 'Unknown Error'
            print(('Failed on URL {}, code = {}, reason = {} ({})'.format(
                self.url, ret_code, reason, str(e))))

        if response.status_code == HTTPStatus.OK:
            if output_type == 'json':
                try:
                    self.output = response.json()
                except Exception as e:
                    output = response.content
                    self.output = deserialize(fromstring(output))
            elif output_type == 'xml':
                output = response.text
                self.output = deserialize(fromstring(output))
            else:
                print(('Unrecognized output type {}'.format(output_type)))
        else:
            print(('Failed on URL {}, code = {}, reason = {}'.format(
                self.url, response.status_code, response.reason)))

    # ...........    ....................................
    def query_by_post(self, output_type='json', file=None):
        """Perform a POST request.
        """
        self.output = None
        # Post a file
        if file is not None:
            # TODO: send as bytes here?
            files = {'files': open(file, 'rb')}
            try:
                response = requests.post(self.base_url, files=files)
            except Exception as e:
                try:
                    ret_code = response.status_code
                    reason = response.reason
                except Exception:
                    ret_code = HTTPStatus.INTERNAL_SERVER_ERROR
                    reason = 'Unknown Error'
                print((
                    """Failed on URL {}, posting uploaded file {}, code = {},
                        reason = {} ({})""".format(
                            self.url, file, ret_code, reason, str(e))))
        # Post parameters
        else:
            all_params = self._other_filters.copy()
            all_params[self._q_key] = self._q_filters
            query_as_string = json.dumps(all_params)
            try:
                response = requests.post(
                    self.base_url, data=query_as_string, headers=self.headers)
            except Exception as e:
                try:
                    ret_code = response.status_code
                    reason = response.reason
                except Exception:
                    ret_code = HTTPStatus.INTERNAL_SERVER_ERROR
                    reason = 'Unknown Error'
                print(('Failed on URL {}, code = {}, reason = {} ({})'.format(
                    self.url, ret_code, reason, str(e))))

        if response.ok:
            try:
                if output_type == 'json':
                    try:
                        self.output = response.json()
                    except Exception as e:
                        output = response.content
                        self.output = deserialize(fromstring(output))
                elif output_type == 'xml':
                    output = response.text
                    self.output = deserialize(fromstring(output))
                else:
                    print(('Unrecognized output type {}'.format(output_type)))
            except Exception as e:
                print(('{} {}, content={}, ({})'.format(
                    'Failed to interpret output of URL', self.base_url,
                    response.content, str(e))))
        else:
            try:
                ret_code = response.status_code
                reason = response.reason
            except Exception:
                ret_code = HTTPStatus.INTERNAL_SERVER_ERROR
                reason = 'Unknown Error'
            print(('Failed ({}: {}) for baseurl {}'.format(
                ret_code, reason, self.base_url)))


# .............................................................................
class BisonAPI(APIQuery):
    """Class to query BISON APIs and return results
    """

    # ...............................................
    def __init__(self, q_filters=None, other_filters=None, filter_string=None,
                 headers=None):
        """Constructor for BisonAPI class
        """
        if headers is None:
            headers = {'Content-Type': 'application/json'}
        all_q_filters = copy(BisonQuery.QFILTERS)
        if q_filters:
            all_q_filters.update(q_filters)

        # Add/replace other filters to defaults for this instance
        all_other_filters = copy(BisonQuery.FILTERS)
        if other_filters:
            all_other_filters.update(other_filters)

        APIQuery.__init__(
            self, BISON.OCCURRENCE_URL, q_key='q', q_filters=all_q_filters,
            other_filters=all_other_filters, filter_string=filter_string,
            headers=headers)

    # ...............................................
    @classmethod
    def init_from_url(cls, url, headers=None):
        """Instiate from url
        """
        if headers is None:
            headers = {'Content-Type': 'application/json'}
        base, filters = url.split('?')
        if base.strip().startswith(BISON.OCCURRENCE_URL):
            qry = BisonAPI(filter_string=filters)
        else:
            raise Exception(
                'Bison occurrence API must start with {}'.format(
                    BISON.OCCURRENCE_URL))
        return qry

    # ...............................................
    def query(self):
        """Queries the API and sets 'output' attribute to a JSON object
        """
        APIQuery.query_by_get(self, output_type='json')

    # ...............................................
    def _burrow(self, key_list):
        this_dict = self.output
        if isinstance(this_dict, dict):
            for key in key_list:
                try:
                    this_dict = this_dict[key]
                except KeyError:
                    raise Exception('Missing key {} in output'.format(key))
        else:
            raise Exception('Invalid output type ({})'.format(type(this_dict)))
        return this_dict

    # ...............................................
    @staticmethod
    def get_tsn_list_for_binomials():
        """Returns a list of sequences containing tsn and tsnCount
        """
        bison_qry = BisonAPI(
            q_filters={BISON.NAME_KEY: BISON.BINOMIAL_REGEX},
            other_filters=BisonQuery.TSN_FILTERS)
        tsn_list = bison_qry._get_binomial_tsns()
        return tsn_list

    # ...............................................
    def _get_binomial_tsns(self):
        data_list = None
        self.query()
        if self.output is not None:
            data_count = self._burrow(BisonQuery.COUNT_KEYS)
            data_list = self._burrow(BisonQuery.TSN_LIST_KEYS)
            print('Reported count = {}, actual count = {}'.format(
                data_count, len(data_list)))
        return data_list

    # ...............................................
    @staticmethod
    def get_itis_tsn_values(itis_tsn):
        """Return ItisScientificName, kingdom, and TSN info for occ record
        """
        itis_name = king = tsn_hier = None
        try:
            occ_api = BisonAPI(
                q_filters={BISON.HIERARCHY_KEY: '*-{}-'.format(itis_tsn)},
                other_filters={'rows': 1})
            tsn_hier = occ_api.get_first_value_for(BISON.HIERARCHY_KEY)
            itis_name = occ_api.get_first_value_for(BISON.NAME_KEY)
            king = occ_api.get_first_value_for(BISON.KINGDOM_KEY)
        except Exception as e:
            print(str(e))
            raise
        return (itis_name, king, tsn_hier)

    # ...............................................
    def get_tsn_occurrences(self):
        """Returns a list of occurrence record dictionaries
        """
        data_list = []
        if self.output is None:
            self.query()
        if self.output is not None:
            data_list = self._burrow(BisonQuery.RECORD_KEYS)
        return data_list

    # ...............................................
    def get_first_value_for(self, field_name):
        """Returns first value for given field name
        """
        val = None
        records = self.get_tsn_occurrences()
        for rec in records:
            try:
                val = rec[field_name]
                break
            except KeyError:
                print(('Missing {} for {}'.format(field_name, self.url)))

        return val


# .............................................................................
class ItisAPI(APIQuery):
    """Class to query BISON APIs and return results
    """

    # ...............................................
    def __init__(self, other_filters=None):
        """Constructor for ItisAPI class
        """
        APIQuery.__init__(
            self, Itis.TAXONOMY_HIERARCHY_URL, other_filters=other_filters)

    # # ...............................................
    # @staticmethod
    # def _find_taxon_by_rank(root, rank_key):
    #     for tax in root.iter(
    #             '{{}}{}'.format(Itis.DATA_NAMESPACE, Itis.HIERARCHY_TAG)):
    #         rank = tax.find(
    #             '{{}}{}'.format(Itis.DATA_NAMESPACE, Itis.RANK_TAG)).text
    #         if rank == rank_key:
    #             name = tax.find(
    #                '{{}}{}'.format(Itis.DATA_NAMESPACE, Itis.TAXON_TAG)).text
    #             tsn = tax.find(
    #                 '{{}}{}'.format(
    #                     Itis.DATA_NAMESPACE, Itis.TAXONOMY_KEY)).text
    #             return (tsn, name)
    #     return None

    # ...............................................
    @staticmethod
    def _get_rank_from_path(tax_path, rank_key):
        for rank, tsn, name in tax_path:
            if rank == rank_key:
                return (int(tsn), name)
        return (None, None)

    # ...............................................
    def _return_hierarchy(self):
        """
        Todo:
            Look at formatted strings, I don't know if this is working
        """
        tax_path = []
        for tax in self.output.iter(
                # '{{}}{}'.format(Itis.DATA_NAMESPACE, Itis.HIERARCHY_TAG)):
                '{}{}'.format(Itis.DATA_NAMESPACE, Itis.HIERARCHY_TAG)):
            rank = tax.find(
                # '{{}}{}'.format(Itis.DATA_NAMESPACE, Itis.RANK_TAG)).text
                '{}{}'.format(Itis.DATA_NAMESPACE, Itis.RANK_TAG)).text
            name = tax.find(
                # '{{}}{}'.format(Itis.DATA_NAMESPACE, Itis.TAXON_TAG)).text
                '{}{}'.format(Itis.DATA_NAMESPACE, Itis.TAXON_TAG)).text
            tsn = tax.find(
                # '{{}}{}'.format(Itis.DATA_NAMESPACE, Itis.TAXONOMY_KEY)).text
                '{}{}'.format(Itis.DATA_NAMESPACE, Itis.TAXONOMY_KEY)).text
            tax_path.append((rank, tsn, name))
        return tax_path

    # ...............................................
    def get_taxon_tsn_hierarchy(self):
        """Retrieve taxon hierarchy
        """
        if self.output is None:
            APIQuery.query_by_get(self, output_type='xml')
        tax_path = self._return_hierarchy()
        hierarchy = {}
        for rank in (
                Itis.KINGDOM_KEY, Itis.PHYLUM_DIVISION_KEY, Itis.CLASS_KEY,
                Itis.ORDER_KEY, Itis.FAMILY_KEY, Itis.GENUS_KEY,
                Itis.SPECIES_KEY):
            hierarchy[rank] = self._get_rank_from_path(tax_path, rank)
        return hierarchy

    # ...............................................
    def query(self):
        """Queries the API and sets 'output' attribute to a ElementTree object
        """
        APIQuery.query_by_get(self, output_type='xml')


# .............................................................................
class GbifAPI(APIQuery):
    """Class to query GBIF APIs and return results
    """
    NameMatchFieldnames = [
        'scientificName', 'kingdom', 'phylum', 'class', 'order', 'family',
        'genus', 'species', 'rank', 'genusKey', 'speciesKey', 'usageKey',
        'canonicalName', 'confidence']
    ACCEPTED_NAME_KEY = 'accepted_name'
    SEARCH_NAME_KEY = 'search_name'
    SPECIES_KEY_KEY = 'speciesKey'
    SPECIES_NAME_KEY = 'species'
    TAXON_ID_KEY = 'taxon_id'

    # ...............................................
    def __init__(self, service=GBIF.SPECIES_SERVICE, key=None,
                 other_filters=None):
        """Constructor for GbifAPI class
        """
        url = '/'.join((GBIF.REST_URL, service))
        if key is not None:
            url = '/'.join((url, str(key)))
        APIQuery.__init__(self, url, other_filters=other_filters)

    # ...............................................
    @staticmethod
    def _get_output_val(out_dict, name):
        try:
            val = out_dict[name]
        except Exception:
            return None
        return val

    # ...............................................
    @staticmethod
    def get_taxonomy(taxon_key):
        """Return GBIF backbone taxonomy for this GBIF Taxon ID"""
        accepted_key = accepted_str = nub_key = None
        log_lines = []
        tax_api = GbifAPI(service=GBIF.SPECIES_SERVICE, key=taxon_key)

        try:
            tax_api.query()
            out = tax_api.output
            sciname_str = tax_api._get_output_val(out, 'scientificName')
            kingdom_str = tax_api._get_output_val(out, 'kingdom')
            phylum_str = tax_api._get_output_val(out, 'phylum')
            class_str = tax_api._get_output_val(out, 'class')
            order_str = tax_api._get_output_val(out, 'order')
            family_str = tax_api._get_output_val(out, 'family')
            genus_str = tax_api._get_output_val(out, 'genus')
            species_str = tax_api._get_output_val(out, 'species')
            rank_str = tax_api._get_output_val(out, 'rank')
            genus_key = tax_api._get_output_val(out, 'genusKey')
            species_key = tax_api._get_output_val(out, 'speciesKey')
            tax_status = tax_api._get_output_val(out, 'taxonomicStatus')
            canonical_str = tax_api._get_output_val(out, 'canonicalName')
            nub_key = tax_api._get_output_val(out, 'nubKey')
            
            # Return accepted key and name if available
            if tax_status != 'ACCEPTED':
                try:
                    # Not present if results are taxonomicStatus=ACCEPTED
                    accepted_key = tax_api._get_output_val(
                        out, 'acceptedKey')
                    accepted_str = tax_api._get_output_val(
                        out, 'accepted')

                except Exception:
                    log_lines.append(
                        'Failed to format data from {}'.format(taxon_key))
            else:
                if rank_str == 'SPECIES':
                    accepted_key = species_key
                    accepted_str = sciname_str
                elif rank_str == 'GENUS':
                    accepted_key = genus_key
                    accepted_str = genus_str
                else:
                    log_lines.append(
                        'Rank {} is not species or genus '.format(rank_str))
                    
            # Log results
            log_lines.append(tax_api.url)
            log_lines.append(
                '   taxonomicStatus = {}'.format(tax_status))
            log_lines.append(
                '   acceptedKey = {}'.format(accepted_key))
            log_lines.append(
                '   acceptedStr = {}'.format(accepted_str))
        except Exception as e:
            print(str(e))
            raise
        return (
            rank_str, sciname_str, canonical_str, accepted_key, accepted_str,
            nub_key, tax_status, kingdom_str, phylum_str, class_str, order_str,
            family_str, genus_str, species_str, genus_key, species_key,
            log_lines)

    # ...............................................
    @staticmethod
    def _get_taiwan_row(occ_api, taxon_key, canonical_name, rec):
        row = None
        occ_key = occ_api._get_output_val(rec, 'gbifID')
        lon_str = occ_api._get_output_val(rec, 'decimalLongitude')
        lat_str = occ_api._get_output_val(rec, 'decimalLatitude')
        try:
            float(lon_str)
        except ValueError:
            return row

        try:
            float(lat_str)
        except ValueError:
            return row

        if (occ_key is not None
                and not lat_str.startswith('0.0')
                and not lon_str.startswith('0.0')):
            row = [taxon_key, canonical_name, occ_key, lon_str, lat_str]
        return row

    # ...............................................
    @staticmethod
    def get_occurrences(taxon_key, canonical_name, out_f_name,
                        other_filters=None, max_points=None):
        """Return GBIF occurrences for this GBIF Taxon ID
        """
        gbif_api = GbifAPI(
            service=GBIF.OCCURRENCE_SERVICE, key=GBIF.SEARCH_COMMAND,
            other_filters={'taxonKey': taxon_key,
                           'limit': GBIF.LIMIT,
                           'hasCoordinate': True,
                           'has_geospatial_issue': False})

        gbif_api.add_filters(q_filters=other_filters)

        offset = 0
        curr_count = 0
        lm_total = 0
        gbif_total = 0
        complete = False

        ready_filename(out_f_name, overwrite=True)
        with open(out_f_name, 'w', encoding=ENCODING, newline='') as csv_f:
            writer = csv.writer(csv_f, delimiter=GbifAPI.DELIMITER)

            while not complete and offset <= gbif_total:
                gbif_api.add_filters(other_filters={'offset': offset})
                try:
                    gbif_api.query()
                except Exception:
                    print('Failed on {}'.format(taxon_key))
                    curr_count = 0
                else:
                    # First query, report count
                    if offset == 0:
                        gbif_total = gbif_api.output['count']
                        print(('Found {} recs for key {}'.format(
                            gbif_total, taxon_key)))

                    recs = gbif_api.output['results']
                    curr_count = len(recs)
                    lm_total += curr_count
                    # Write header
                    if offset == 0 and curr_count > 0:
                        writer.writerow(
                            ['taxonKey', 'canonicalName', 'gbifID',
                             'decimalLongitude', 'decimalLatitude'])
                    # Write recs
                    for rec in recs:
                        row = gbif_api._get_taiwan_row(
                            gbif_api, taxon_key, canonical_name, rec)
                        if row:
                            writer.writerow(row)
                    print(('  Retrieved {} records, starting at {}'.format(
                        curr_count, offset)))
                    offset += GBIF.LIMIT
                    if max_points is not None and lm_total >= max_points:
                        complete = True

    # ...............................................
    @staticmethod
    def _get_fld_vals(big_rec):
        rec = {}
        for fld_name in GbifAPI.NameMatchFieldnames:
            try:
                rec[fld_name] = big_rec[fld_name]
            except KeyError:
                pass
        return rec

    # ...............................................
    @staticmethod
    def get_accepted_names(name_str, kingdom=None):
        """Return closest accepted species in GBIF backbone taxonomy

        Note:
            This function uses the name search API

        Todo:
            Rename function to match_accepted_name
        """
        good_names = []
        name_clean = name_str.strip()

        other_filters = {'name': name_clean, 'verbose': 'true'}
        if kingdom:
            other_filters['kingdom'] = kingdom
        name_api = GbifAPI(
            service=GBIF.SPECIES_SERVICE, key='match',
            other_filters=other_filters)
        try:
            name_api.query()
            output = name_api.output
        except Exception as e:
            print(('Failed to get a response for species match on {}, ({})'
                   .format(name_clean, str(e))))
            raise

        try:
            status = output['status'].lower()
        except AttributeError:
            status = None

        if status in ('accepted', 'synonym'):
            small_rec = name_api._get_fld_vals(output)
            good_names.append(small_rec)
        else:
            try:
                alternatives = output['alternatives']
                print('No exact match on {}, returning top alt. {}'.format(
                    name_clean, len(alternatives)))
                # get first/best synonym
                for alt in alternatives:
                    try:
                        alt_status = alt['status'].lower()
                    except AttributeError:
                        alt_status = None
                    if alt_status in ('accepted', 'synonym'):
                        small_rec = name_api._get_fld_vals(alt)
                        good_names.append(small_rec)
                        break
            except Exception:
                print(('No match or alternatives to return for {}'.format(
                    name_clean)))

        return good_names

    # ......................................
    @staticmethod
    def _post_json_to_parser(url, data):
        response = output = None
        try:
            response = requests.post(url, json=data)
        except Exception as e:
            if response is not None:
                ret_code = response.status_code
            else:
                print(('Failed on URL {} ({})'.format(url, str(e))))
        else:
            if response.ok:
                try:
                    output = response.json()
                except Exception as e:
                    try:
                        output = response.content
                    except Exception:
                        output = response.text
                    else:
                        print((
                            'Failed to interpret output of URL {} ({})'.format(
                                url, str(e))))
            else:
                try:
                    ret_code = response.status_code
                    reason = response.reason
                except AttributeError:
                    print((
                        'Failed to find failure reason for URL {} ({})'.format(
                            url, str(e))))
                else:
                    print(('Failed on URL {} ({}: {})'.format(
                        url, ret_code, reason)))
        return output

    # ...............................................
    @staticmethod
    def parse_names(filename=None):
        """Return dictionary of given, and clean taxon name for namestrings
        """
        if os.path.exists(filename):
            names = []
            with open(filename, 'r', encoding=ENCODING) as in_file:
                for line in in_file:
                    names.append(line.strip())

        clean_names = {}
        name_api = GbifAPI(service=GBIF.PARSER_SERVICE)
        try:
            output = name_api._post_json_to_parser(names)
        except Exception as err:
            print((
                'Failed to get response from GBIF for data {}, {}'.format(
                    filename, str(err))))
            raise err

        if output:
            for rec in output:
                if rec['parsed'] is True:
                    try:
                        sci_name = rec['scientificName']
                        can_name = rec['canonicalName']
                    except KeyError as key_err:
                        print('Missing scientific or canonicalName in record')
                    except Exception as err:
                        print(('Failed, err: {}'.format(str(err))))
                    clean_names[sci_name] = can_name

        return clean_names

    # ...............................................
    @staticmethod
    def get_publishing_org(pub_org_key):
        """Return title from one organization record with this key

        Args:
            pub_org_key: GBIF identifier for this publishing organization
        """
        org_api = GbifAPI(service=GBIF.ORGANIZATION_SERVICE, key=pub_org_key)
        try:
            org_api.query()
            pub_org_name = org_api._get_output_val(org_api.output, 'title')
        except Exception as e:
            print(str(e))
            raise
        return pub_org_name

    # ...............................................
    @staticmethod
    def get_dataset_meta(dataset_key):
        """Return title of dataset and provider from dataset key

        Args:
            dataset_key: GBIF identifier for this dataset
        """
        api = GbifAPI(service=GBIF.DATASET_SERVICE, key=dataset_key)
        try:
            api.query()
            org_key = api._get_output_val(api.output, 'publishingOrganizationKey')
            dataset_name = api._get_output_val(api.output, 'title')
        except Exception as e:
            print(str(e))
            raise
        org_name = GbifAPI.get_publishing_org(org_key)
        return dataset_name, org_name

    # ...............................................
    def query(self):
        """ Queries the API and sets 'output' attribute to a ElementTree object
        """
        APIQuery.query_by_get(self, output_type='json')


# .............................................................................
class IdigbioAPI(APIQuery):
    """Class to query iDigBio APIs and return results
    """
    OCCURRENCE_COUNT_KEY = 'count'

    # ...............................................
    def __init__(self, q_filters=None, other_filters=None, filter_string=None,
                 headers=None):
        """Constructor for IdigbioAPI class
        """
        idig_search_url = '/'.join((
            Idigbio.SEARCH_PREFIX, Idigbio.SEARCH_POSTFIX,
            Idigbio.OCCURRENCE_POSTFIX))

        # Add/replace Q filters to defaults for this instance
        all_q_filters = copy(IdigbioQuery.QFILTERS)
        if q_filters:
            all_q_filters.update(q_filters)

        # Add/replace other filters to defaults for this instance
        all_other_filters = copy(IdigbioQuery.FILTERS)
        if other_filters:
            all_other_filters.update(other_filters)

        APIQuery.__init__(
            self, idig_search_url, q_key='rq', q_filters=all_q_filters,
            other_filters=all_other_filters, filter_string=filter_string,
            headers=headers)

    # ...............................................
    @classmethod
    def init_from_url(cls, url, headers=None):
        """Initialize from url
        """
        base, filters = url.split('?')
        if base.strip().startswith(Idigbio.SEARCH_PREFIX):
            qry = IdigbioAPI(filter_string=filters, headers=headers)
        else:
            raise Exception(
                'iDigBio occurrence API must start with {}' .format(
                    Idigbio.SEARCH_PREFIX))
        return qry

    # ...............................................
    def query(self):
        """Queries the API and sets 'output' attribute to a JSON object
        """
        APIQuery.query_by_post(self, output_type='json')

    # ...............................................
    def query_by_gbif_taxon_id(self, taxon_key):
        """Return a list of occurrence record dictionaries.
        """
        self._q_filters[Idigbio.GBIFID_FIELD] = taxon_key
        self.query()
        specimen_list = []
        if self.output is not None:
            # full_count = self.output['itemCount']
            for item in self.output[Idigbio.OCCURRENCE_ITEMS_KEY]:
                new_item = item[Idigbio.RECORD_CONTENT_KEY].copy()

                for idx_fld, idx_val in item[Idigbio.RECORD_INDEX_KEY].items():
                    if idx_fld == 'geopoint':
                        new_item[DwcNames.DECIMAL_LONGITUDE['SHORT']
                                 ] = idx_val['lon']
                        new_item[DwcNames.DECIMAL_LATITUDE['SHORT']
                                 ] = idx_val['lat']
                    else:
                        new_item[idx_fld] = idx_val
                specimen_list.append(new_item)
        return specimen_list

    # ...............................................
    @staticmethod
    def _write_idigbio_metadata(orig_fld_names, meta_f_name):
        new_meta = {}
        for col_idx, fld_name in enumerate(orig_fld_names):
            val_dict = {'name': fld_name, 'type': 'str'}

            if fld_name == 'uuid':
                val_dict['role'] = OccDataParser.FIELD_ROLE_IDENTIFIER
            elif fld_name == 'taxonid':
                val_dict['role'] = OccDataParser.FIELD_ROLE_GROUPBY
            elif fld_name == 'geopoint':
                val_dict['role'] = OccDataParser.FIELD_ROLE_GEOPOINT
            elif fld_name == 'canonicalname':
                val_dict['role'] = OccDataParser.FIELD_ROLE_TAXANAME
            elif fld_name == 'dec_long':
                val_dict['role'] = OccDataParser.FIELD_ROLE_LONGITUDE
            elif fld_name == 'dec_lat':
                val_dict['role'] = OccDataParser.FIELD_ROLE_LATITUDE
            new_meta[str(col_idx)] = val_dict

        ready_filename(meta_f_name, overwrite=True)
        with open(meta_f_name, 'w', encoding=ENCODING) as out_f:
            json.dump(new_meta, out_f)
        return new_meta

    # ...............................................
    @staticmethod
    def _get_idigbio_fields(rec):
        """Get iDigBio fields
        """
        fld_names = list(rec['indexTerms'].keys())
        # add dec_long and dec_lat to records
        fld_names.extend(['dec_lat', 'dec_long'])
        fld_names.sort()
        return fld_names

    # ...............................................
    @staticmethod
    def _count_idigbio_records(gbif_taxon_id):
        """Count iDigBio records for a GBIF taxon id.
        """
        api = idigbio.json()
        record_query = {
            'taxonid': str(gbif_taxon_id), 'geopoint': {'type': 'exists'}}

        try:
            output = api.search_records(rq=record_query, limit=1, offset=0)
        except Exception:
            print('Failed on {}'.format(gbif_taxon_id))
            total = 0
        else:
            total = output['itemCount']
        return total

    # ...............................................
    def _get_idigbio_records(self, gbif_taxon_id, fields, writer,
                             meta_output_file):
        """Get records from iDigBio
        """
        api = idigbio.json()
        limit = 100
        offset = 0
        curr_count = 0
        total = 0
        record_query = {'taxonid': str(gbif_taxon_id),
                        'geopoint': {'type': 'exists'}}
        while offset <= total:
            try:
                output = api.search_records(
                    rq=record_query, limit=limit, offset=offset)
            except Exception:
                print('Failed on {}'.format(gbif_taxon_id))
                total = 0
            else:
                total = output['itemCount']

                # First gbifTaxonId where this data retrieval is successful,
                # get and write header and metadata
                if total > 0 and fields is None:
                    print('Found data, writing data and metadata')
                    fields = self._get_idigbio_fields(output['items'][0])
                    # Write header in datafile
                    writer.writerow(fields)
                    # Write metadata file with column indices
                    _meta = self._write_idigbio_metadata(
                        fields, meta_output_file)

                # Write these records
                recs = output['items']
                curr_count += len(recs)
                print(('  Retrieved {} records, {} recs starting at {}'.format(
                    len(recs), limit, offset)))
                for rec in recs:
                    rec_data = rec['indexTerms']
                    vals = []
                    for fld_name in fields:
                        # Pull long, lat from geopoint
                        if fld_name == 'dec_long':
                            try:
                                vals.append(rec_data['geopoint']['lon'])
                            except KeyError:
                                vals.append('')
                        elif fld_name == 'dec_lat':
                            try:
                                vals.append(rec_data['geopoint']['lat'])
                            except KeyError:
                                vals.append('')
                        # or just append verbatim
                        else:
                            try:
                                vals.append(rec_data[fld_name])
                            except KeyError:
                                vals.append('')

                    writer.writerow(vals)
                offset += limit
        print(('Retrieved {} of {} reported records for {}'.format(
            curr_count, total, gbif_taxon_id)))
        return curr_count, fields

    # ...............................................
    def assemble_idigbio_data(self, taxon_ids, point_output_file,
                              meta_output_file, missing_id_file=None):
        """Assemble iDigBio data dictionary
        """
        if not isinstance(taxon_ids, list):
            taxon_ids = [taxon_ids]

        # Delete old files
        for fname in (point_output_file, meta_output_file):
            if os.path.exists(fname):
                print(('Deleting existing file {} ...'.format(fname)))
                os.remove(fname)

        summary = {self.GBIF_MISSING_KEY: []}

        ready_filename(point_output_file, overwrite=True)
        with open(point_output_file, 'w', encoding=ENCODING, newline='') as csv_f:
            writer = csv.writer(csv_f, delimiter=GbifAPI.DELIMITER)
            fld_names = None
            for gid in taxon_ids:
                # Pull / write field names first time
                pt_count, fld_names = self._get_idigbio_records(
                    gid, fld_names, writer, meta_output_file)

                summary[gid] = pt_count
                if pt_count == 0:
                    summary[self.GBIF_MISSING_KEY].append(gid)

        # get/write missing data
        if missing_id_file is not None and len(
                summary[self.GBIF_MISSING_KEY]) > 0:
            with open(missing_id_file, 'w', encoding=ENCODING) as out_f:
                for gid in summary[self.GBIF_MISSING_KEY]:
                    out_f.write('{}\n'.format(gid))

        return summary

    # ...............................................
    def query_idigbio_data(self, taxon_ids):
        """Query iDigBio for data
        """
        if not isinstance(taxon_ids, list):
            taxon_ids = [taxon_ids]

        summary = {self.GBIF_MISSING_KEY: []}

        for gid in taxon_ids:
            # Pull/write fieldnames first time
            pt_count = self._count_idigbio_records(gid)
            if pt_count == 0:
                summary[self.GBIF_MISSING_KEY].append(gid)
            summary[gid] = pt_count

        return summary

    # ...............................................
    def read_idigbio_data(self, pt_file_name, meta_file_name):
        """Read iDigBio data
        """
        gbif_id_counts = {}
        if not os.path.exists(pt_file_name):
            print(('Point data {} does not exist'.format(pt_file_name)))
        elif not os.path.exists(meta_file_name):
            print(('Metadata {} does not exist'.format(meta_file_name)))
        else:
            log = None
            occ_parser = OccDataParser(
                log, pt_file_name, meta_file_name, delimiter=self.DELIMITER,
                pull_chunks=True)
            occ_parser.initialize_me()
            # returns dict with key = taxonid, val = (name, count)
            summary = occ_parser.read_all_chunks()
            for tax_id, (_, count) in summary.items():
                gbif_id_counts[tax_id] = count
        return gbif_id_counts


# .............................................................................
def test_bison():
    """Test Bison
    """
    tsn_list = [['100637', 31], ['100667', 45], ['100674', 24]]

    #       tsn_list = BisonAPI.getTsnListForBinomials()
    for tsn_pair in tsn_list:
        tsn = int(tsn_pair[0])
        count = int(tsn_pair[1])

        new_q = {BISON.HIERARCHY_KEY: '*-{}-*'.format(tsn)}
        occ_api = BisonAPI(
            q_filters=new_q, other_filters=BisonQuery.OCC_FILTERS)
        this_url = occ_api.url
        occ_list = occ_api.get_tsn_occurrences()
        count = None if not occ_list else len(occ_list)
        print('Received {} occurrences for TSN {}'.format(count, tsn))

        occ_api2 = BisonAPI.init_from_url(this_url)
        occ_list2 = occ_api2.get_tsn_occurrences()
        count = None if not occ_list2 else len(occ_list2)
        print('Received {} occurrences from url init'.format(count))

        tsn_api = BisonAPI(
            q_filters={BISON.HIERARCHY_KEY: '*-{}-'.format(tsn)},
            other_filters={'rows': 1})
        hier = tsn_api.get_first_value_for(BISON.HIERARCHY_KEY)
        name = tsn_api.get_first_value_for(BISON.NAME_KEY)
        print(name, hier)


# .............................................................................
def test_gbif():
    """Test GBIF
    """
    taxon_id = 1000225
    output = GbifAPI.get_taxonomy(taxon_id)
    print('GBIF Taxonomy for {} = {}'.format(taxon_id, output))


# .............................................................................
def test_idigbio_taxon_ids():
    """Test iDigBio taxon ids
    """
    in_f_name = '/tank/data/input/idigbio/taxon_ids.txt'
    test_count = 20

    out_list = '/tmp/idigbio_accepted_list.txt'
    if os.path.exists(out_list):
        os.remove(out_list)
    out_f = open(out_list, 'w', encoding=ENCODING)

    idig_list = []
    with open(in_f_name, 'r', encoding=ENCODING) as in_f:
        #          with line in file:
        for _ in range(test_count):
            line = in_f.readline()

            if line is not None:
                temp_vals = line.strip().split()
                if len(temp_vals) < 3:
                    print(('Missing data in line {}'.format(line)))
                else:
                    try:
                        curr_gbif_taxon_id = int(temp_vals[0])
                    except Exception:
                        pass
                    try:
                        curr_reported_count = int(temp_vals[1])
                    except Exception:
                        pass
                    temp_vals = temp_vals[1:]
                    temp_vals = temp_vals[1:]
                    curr_name = ' '.join(temp_vals)

                (_, _, _, _, _, _, tax_status, _, _, _, _, _, _, _, _, _, _
                 ) = GbifAPI.get_taxonomy(curr_gbif_taxon_id)

                if tax_status == 'ACCEPTED':
                    idig_list.append(
                        [curr_gbif_taxon_id, curr_reported_count, curr_name])
                    out_f.write(line)

    out_f.close()
    return idig_list


# .............................................................................
if __name__ == '__main__':
    pass
