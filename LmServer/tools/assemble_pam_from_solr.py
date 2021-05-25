"""Query Solr for matching PAVs and assemble into a PAM

Todo:
    * Consider if we want to expand this
"""
import argparse

import numpy as np
from osgeo import ogr

from LmCommon.common.lmconstants import LMFormat, JobStatus, ENCODING
from LmCommon.common.ready_file import ready_filename
from LmCommon.compression.binary_list import decompress
from LmServer.common.lmconstants import SOLR_FIELDS
from LmServer.common.log import ConsoleLogger
from LmServer.common.solr import query_archive_index
from LmServer.db.borg_scribe import BorgScribe


# .............................................................................
def _get_row_headers_from_shapegrid(shapegrid_filename):
    """Get the row headers from a shapegrid

    Args:
        shapegrid_filename (:obj: `str`): The file location of the shapegrid to
            get headers
    """
    ogr.RegisterAll()
    drv = ogr.GetDriverByName(LMFormat.get_default_ogr().driver)
    dataset = drv.Open(shapegrid_filename)
    lyr = dataset.GetLayer(0)

    row_headers = []

    for j in range(lyr.GetFeatureCount()):
        cur_feat = lyr.GetFeature(j)
        site_idx = cur_feat.GetFID()
        x_coord, y_coord = cur_feat.geometry().Centroid().GetPoint_2D()
        row_headers.append((site_idx, x_coord, y_coord))
    return sorted(row_headers)


# .............................................................................
def assemble_pam(pam_id):
    """Query Solr for PAVs and assemble into a PAM

    Args:
        pam_id (:obj: `int`): The database ID of the PAM to assemble

    Todo:
        * Use Solr parameter for PAM id once that is in full use
    """
    scribe = BorgScribe(ConsoleLogger())
    scribe.open_connections()

    pam = scribe.get_matrix(mtx_id=pam_id)

    matches = query_archive_index(gridset_id=pam.gridset_id, user_id=pam.user)
    mtx_cols = scribe.list_matrix_columns(
        0, 10000, matrix_id=pam.get_id(), user_id=pam.user)
    mtx_col_ids = [int(c.get_id()) for c in mtx_cols]

    # ......................
    def _match_in_column_ids(match):
        """Filter function to determine if a matrix column is in the PAM
        """
        return int(match[SOLR_FIELDS.ID]) in mtx_col_ids

    # Filter matches
    filtered_matches = list(filter(_match_in_column_ids, matches))

    # Create empty PAM
    shapegrid = pam.get_shapegrid()
    rows = shapegrid.feature_count

    row_headers = _get_row_headers_from_shapegrid(shapegrid.get_dlocation())

    pam_data = np.zeros((rows, len(filtered_matches)), dtype=np.int)

    # Decompress
    column_headers = []
    for i, match in enumerate(filtered_matches):
        column_headers.append(match[SOLR_FIELDS.SQUID])
        pam_data[:, i] = decompress(match[SOLR_FIELDS.COMPRESSED_PAV])

    pam.set_data(pam_data, headers={'0': row_headers, '1': column_headers})

    pam.update_status(JobStatus.COMPLETE)
    scribe.update_object(pam)
    pam.write(pam.get_dlocation())
    scribe.close_connections()
    return pam


# .............................................................................
def main():
    """Main method of script
    """
    parser = argparse.ArgumentParser(
        description='Assemble a PAM from Solr matches')
    parser.add_argument(
        'pam_id', type=int, help='The database ID of the PAM to assemble')
    parser.add_argument(
        'success_filename', type=str,
        help='File location to write success status')

    args = parser.parse_args()

    pam_id = args.pam_id
    try:
        _pam = assemble_pam(pam_id)
        success = 1
    except Exception as e:
        print((str(e)))
        success = 0
        raise e

    ready_filename(args.success_filename)
    with open(args.success_filename, 'w', encoding=ENCODING) as out_f:
        out_f.write(str(success))


# .............................................................................
if __name__ == '__main__':
    main()
