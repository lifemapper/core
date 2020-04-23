"""Create a package to be returned to a client based on a gridset

Todo:
     * Move this as necessary
     * Probably want to split of EML generating code to separate module(s)
"""
import argparse
from collections import defaultdict
import json
import os
import zipfile

from lmpy import Matrix

from LmCommon.common.lm_xml import tostring
from LmCommon.common.lmconstants import LMFormat, MatrixType, ENCODING
from LmServer.common.lmconstants import TEMP_PATH
from LmServer.common.log import ConsoleLogger
from LmServer.db.borg_scribe import BorgScribe
from LmWebServer.formatters.eml_formatter import make_eml
from LmWebServer.formatters.geo_json_formatter import geo_jsonify_flo


# .............................................................................
def create_header_lookup(headers, squids=False, scribe=None, user_id=None):
    """Generate a header lookup to be included in the package metadata
    """

    def get_header_dict(header, idx):
        return {
            'header': header,
            'index': idx
        }

    def get_squid_header_dict(header, idx, scribe, user_id):
        taxon = scribe.get_taxon(squid=header, user_id=user_id)
        ret = get_header_dict(header, idx)

        for attrib, key in [
                ('scientificName', 'scientific_name'),
                ('canonicalName', 'canonical_name'),
                ('rank', 'taxon_rank'),
                ('kingdom', 'taxon_kingdom'),
                ('phylum', 'taxon_phylum'),
                ('txClass', 'taxon_class'),
                ('txOrder', 'taxon_order'),
                ('family', 'taxon_family'),
                ('genus', 'taxon_genus')]:
            val = getattr(taxon, attrib)
            if val is not None:
                ret[key] = val
        return ret

    if squids and scribe and user_id:
        return [
            get_squid_header_dict(
                headers[i], i, scribe, user_id) for i in range(len(headers))]

    return [get_header_dict(headers[i], i) for i in range(len(headers))]


# .............................................................................
def mung(data):
    """Replace a list of values with a map from non-zero values
    """
    munged = defaultdict(list)
    for i, datum in enumerate(data):
        if datum != 0:
            munged[datum].append(i)
    return munged


# .............................................................................
def assemble_package_for_gridset(gridset, out_file, scribe, user_id):
    """Creates an output zip file from the gridset
    """
    print(('Assembling package: {}'.format(out_file)))
    print('Creating EML')
    gs_eml = tostring(make_eml(gridset))
    with zipfile.ZipFile(
            out_file, mode='w', compression=zipfile.ZIP_DEFLATED,
            allowZip64=True) as out_zip:
        print('Write out EML')
        out_zip.writestr('gridset_{}.eml'.format(gridset.get_id()), gs_eml)
        print('Write tree')
        out_zip.write(
            gridset.tree.get_dlocation(),
            os.path.basename(gridset.tree.get_dlocation()))
        print('Getting shapegrid')
        shapegrid = gridset.getShapegrid()
        matrices = gridset.getMatrices()
        i = 0
        print(('{} matrices'.format(len(matrices))))
        for mtx in matrices:
            i += 1
            print(('Matrix: ({} of {}) {}'.format(
                i, len(matrices), mtx.get_dlocation())))
            print(' - Loading matrix')
            mtx_obj = Matrix.load(mtx.get_dlocation())
            print(' - Loaded')

            # Need to get geojson where we can
            if mtx.matrixType in [MatrixType.PAM, MatrixType.ROLLING_PAM]:
                mtx_file_name = '{}{}'.format(
                    os.path.splitext(
                        os.path.basename(
                            mtx.get_dlocation()))[0], LMFormat.GEO_JSON.ext)

                print(' - Creating SQUID lookup')
                hlfn = 'squidLookup.json'
                out_zip.writestr(
                    hlfn, json.dumps(
                        create_header_lookup(
                            mtx_obj.get_column_headers(), squids=True,
                            scribe=scribe, user_id=user_id), indent=4))

                # Make a temporary file
                temp_file_name = os.path.join(TEMP_PATH, mtx_file_name)
                print((' - Temporary file name: {}'.format(temp_file_name)))
                with open(temp_file_name, 'w', encoding=ENCODING) as temp_f:
                    print(' - Getting GeoJSON')
                    geo_jsonify_flo(
                        temp_f, shapegrid.get_dlocation(), matrix=mtx_obj,
                        mtx_join_attrib=0, ident=0,
                        header_lookup_filename=hlfn, transform=mung)

            elif mtx.matrixType == MatrixType.ANC_PAM:
                mtx_file_name = '{}{}'.format(
                    os.path.splitext(
                        os.path.basename(
                            mtx.get_dlocation()))[0], LMFormat.GEO_JSON.ext)

                print(' - Creating node lookup')
                hlfn = 'nodeLookup.json'
                out_zip.writestr(
                    hlfn, json.dumps(
                        create_header_lookup(mtx_obj.get_column_headers()),
                        indent=4))

                # Make a temporary file
                temp_file_name = os.path.join(TEMP_PATH, mtx_file_name)
                print((' - Temporary file name: {}'.format(temp_file_name)))
                with open(temp_file_name, 'w', encoding=ENCODING) as temp_f:
                    print(' - Getting GeoJSON')
                    geo_jsonify_flo(
                        temp_f, shapegrid.get_dlocation(), matrix=mtx_obj,
                        mtx_join_attrib=0, ident=0,
                        header_lookup_filename=hlfn, transform=mung)

            elif mtx.matrixType in [
                    MatrixType.SITES_COV_OBSERVED, MatrixType.SITES_COV_RANDOM,
                    MatrixType.SITES_OBSERVED, MatrixType.SITES_RANDOM]:
                mtx_file_name = '{}{}'.format(
                    os.path.splitext(
                        os.path.basename(
                            mtx.get_dlocation()))[0], LMFormat.GEO_JSON.ext)

                # Make a temporary file
                temp_file_name = os.path.join(TEMP_PATH, mtx_file_name)
                print((' - Temporary file name: {}'.format(temp_file_name)))
                with open(temp_file_name, 'w', encoding=ENCODING) as temp_f:
                    print(' - Getting GeoJSON')
                    geo_jsonify_flo(
                        temp_f, shapegrid.get_dlocation(), matrix=mtx_obj,
                        mtx_join_attrib=0, ident=0)
            else:
                print(' - Write non Geo-JSON matrix')
                mtx_file_name = '{}{}'.format(
                    os.path.splitext(
                        os.path.basename(
                            mtx.get_dlocation()))[0], LMFormat.CSV.ext)
                # Make a temporary file
                temp_file_name = os.path.join(TEMP_PATH, mtx_file_name)
                print((' - Temporary file name: {}'.format(temp_file_name)))
                with open(temp_file_name, 'w', encoding=ENCODING) as temp_f:
                    print(' - Getting CSV')
                    mtx_obj.write_csv(temp_f)

            print((' - Zipping {}'.format(temp_file_name)))
            out_zip.write(temp_file_name, mtx_file_name)

            print(' - Delete temp file')
            os.remove(temp_file_name)


# ..........................................................................
def main():
    """Main method for script.
    """
    parser = argparse.ArgumentParser(
        description='This script creates a package of gridset outputs')

    parser.add_argument('gridset_id', type=int, help='The gridset id number')
    parser.add_argument(
        'out_file', type=str,
        help='The file location to write the output package')

    args = parser.parse_args()

    scribe = BorgScribe(ConsoleLogger())
    scribe.open_connections()

    gridset = scribe.get_gridset(
        gridset_id=args.gridset_id, fill_matrices=True)

    assemble_package_for_gridset(
        gridset, args.out_file, scribe, gridset.get_user_id())

    scribe.close_connections()


# ..........................................................................
if __name__ == '__main__':
    main()
