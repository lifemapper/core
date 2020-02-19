"""Query Solr for matching PAVs and assemble into a PAM

Todo:
    * Consider if we want to expand this
"""
import argparse

from LmCommon.common.lmconstants import LMFormat, JobStatus
from LmCommon.common.ready_file import ready_filename
from LmCommon.compression.binary_list import decompress
from LmServer.common.lmconstants import SOLR_FIELDS
from LmServer.common.log import ConsoleLogger
from LmServer.common.solr import query_archive_index
from LmServer.db.borgscribe import BorgScribe
import numpy as np
from osgeo import ogr


# .............................................................................
def _get_row_headers_from_shapegrid(shapegrid_filename):
    """Get the row headers from a shapegrid

    Args:
        shapegrid_filename (:obj: `str`): The file location of the shapegrid to
            get headers
    """
    ogr.RegisterAll()
    drv = ogr.GetDriverByName(LMFormat.getDefaultOGR().driver)
    ds = drv.Open(shapegrid_filename)
    lyr = ds.GetLayer(0)

    row_headers = []

    for j in range(lyr.GetFeatureCount()):
        curFeat = lyr.GetFeature(j)
        siteIdx = curFeat.GetFID()
        x, y = curFeat.geometry().Centroid().GetPoint_2D()
        row_headers.append((siteIdx, x, y))
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
    scribe.openConnections()

    pam = scribe.getMatrix(mtxId=pam_id)

    matches = query_archive_index(gridSetId=pam.gridsetId, userId=pam.user)
    mtx_cols = scribe.listMatrixColumns(
        0, 10000, matrixId=pam.get_id(), userId=pam.user)
    mtx_col_ids = [int(c.get_id()) for c in mtx_cols]

    # ......................
    def _match_in_column_ids(match):
        """Filter function to determine if a matrix column is in the PAM
        """
        return int(match[SOLR_FIELDS.ID]) in mtx_col_ids

    # Filter matches
    filtered_matches = list(filter(_match_in_column_ids, matches))

    # Create empty PAM
    shapegrid = pam.getShapegrid()
    rows = shapegrid.featureCount

    row_headers = _get_row_headers_from_shapegrid(shapegrid.get_dlocation())

    pam_data = np.zeros((rows, len(filtered_matches)), dtype=np.int)

    # Decompress
    column_headers = []
    for i in range(len(filtered_matches)):
        match = filtered_matches[i]
        column_headers.append(match[SOLR_FIELDS.SQUID])
        pam_data[:, i] = decompress(match[SOLR_FIELDS.COMPRESSED_PAV])

    pam.data = pam_data
    pam.setHeaders({'0' : row_headers, '1' : column_headers})

    pam.updateStatus(JobStatus.COMPLETE)
    scribe.updateObject(pam)
    with open(pam.get_dlocation(), 'w') as out_f:
        pam.save(out_f)
    scribe.closeConnections()
    return pam


# .............................................................................
if __name__ == '__main__':
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
        pam = assemble_pam(pam_id)
        success = 1
    except Exception as e:
        print((str(e)))
        success = 0

    ready_filename(args.success_filename)
    with open(args.success_filename, 'w') as out_f:
        out_f.write(str(success))

