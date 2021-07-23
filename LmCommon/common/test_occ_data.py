import csv
import json

ENCODING = 'UTF-8'
OFTInteger = 0 
OFTReal = 2 
OFTString = 4

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



def get_ogr_field_type(type_val):
    if type_val is None:
        return None
    try:
        type_int = int(type_val)
        if type_int in (OFTInteger, OFTString, OFTReal):
            return type_int
        raise Exception(
            'Field type must be OFTInteger, OFTString, ' +
            'OFTReal ({}, {}, {})'.format(OFTInteger, OFTString, OFTReal))
    except Exception:
        try:
            type_str = type_val.lower()
        except Exception:
            raise Exception(
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

def get_check_indexed_metadata(field_meta, header):
    filters = {}
    id_idx = x_idx = y_idx = pt_idx = group_by_idx = name_idx = None
    if header is None:
        field_index_meta = field_meta
    else:
        field_index_meta = {}
        for i, hdr in enumerate(header):
            try:
                field_index_meta[i] = field_meta[hdr]
            except AttributeError:
                field_index_meta[i] = None
    for idx, vals in field_index_meta.items():
        ogr_type = role = accepted_vals = None
        if vals is not None:
            _name = field_index_meta[idx][FIELD_NAME_KEY]
            ogr_type = field_index_meta[idx][FIELD_TYPE_KEY]
            try:
                accepted_vals = field_index_meta[idx][FIELD_VALS_KEY]
            except Exception:
                pass
            else:
                if ogr_type == OFTString:
                    field_index_meta[idx][FIELD_VALS_KEY] = [
                            val.lower() for val in accepted_vals]
            try:
                role = field_index_meta[idx][FIELD_ROLE_KEY].lower()
            except Exception:
                pass
            else:
                field_index_meta[idx]['role'] = role
                if role == FIELD_ROLE_IDENTIFIER:
                    id_idx = idx
                    print(('Found id index {}').format(idx))
                elif role == FIELD_ROLE_LONGITUDE:
                    x_idx = idx
                    print(('Found X index {}').format(idx))
                elif role == FIELD_ROLE_LATITUDE:
                    y_idx = idx
                    print(('Found Y index {}').format(idx))
                elif role == FIELD_ROLE_GEOPOINT:
                    pt_idx = idx
                    print(('Found point index {}').format(idx))
                elif role == FIELD_ROLE_TAXANAME:
                    name_idx = idx
                    print(('Found name index {}').format(idx))
                elif role == FIELD_ROLE_GROUPBY:
                    group_by_idx = idx
                    print(('Found group index {}').format(idx))
        filters[idx] = accepted_vals
    if name_idx is None:
        raise Exception('Missing `TAXANAME` required role in metadata')
    if (x_idx is None or y_idx is None) and pt_idx is None:
        print(('Found x {}, y {}, point {}').format(x_idx, y_idx, pt_idx))
        raise Exception(
            'Missing `{}`-`{}` pair or `{}` roles in metadata'.format(
                'LATITUDE', 'LONGITUDE', 'GEOPOINT'))
    if group_by_idx is None:
        group_by_idx = name_idx
    return (
        field_index_meta, filters, id_idx, x_idx, y_idx, pt_idx,
        group_by_idx, name_idx)

def read_metadata(metadata):
    with open(metadata, 'r', encoding=ENCODING) as in_file:
        meta = json.load(in_file)
    for col_idx in list(meta.keys()):
        f_type = meta[col_idx]['type']
        ogr_type = get_ogr_field_type(f_type)
        meta[col_idx]['type'] = ogr_type
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



metadata = '/tank/lmdata/gbif/2021/07.14/gbif_occ-2021.07.14.json'
fname = '/tank/lmdata/gbif/2021/07.14/gbif_occ-2021.07.14.csv'

f = open(fname, 'r', encoding='UTF-8')
cr = csv.reader(f, delimiter='\t')

meta, do_match_header = read_metadata(metadata)
if do_match_header:
    tmp_list = next(cr)
    print(('Header = {}'.format(tmp_list)))
    header = [fld_name.strip() for fld_name in tmp_list]

(column_meta, filters, id_idx, x_idx, y_idx, geo_idx, group_by_idx, name_idx
 ) = get_check_indexed_metadata(meta, header)
field_count = len(column_meta)

acc_tax_idx = header.index('acceptedTaxonKey')
tax_idx = group_by_idx


line = next(cr)

while line[acc_tax_idx] == line[tax_idx] and cr.line_num < 1000000:
    line = next(cr)








