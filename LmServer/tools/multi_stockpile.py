"""This script stockpiles multiple objects at once

Todo:
    * Expand testing
"""
import argparse
import json
import os

from lmpy import Matrix

from LmBackend.common.lmconstants import RegistryKey
from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import JobStatus, LMFormat, ProcessType
from LmCommon.compression.binary_list import decompress
from LmServer.base.layer import Raster, Vector
from LmServer.common.log import ConsoleLogger
from LmServer.db.borg_scribe import BorgScribe


# .............................................................................
def stockpile_pavs(pav_list):
    """Stockpiles pavs from a list of dictionary entries
    """
    log = ConsoleLogger()
    scribe = BorgScribe(log)
    scribe.open_connections()
    for pav_dict in pav_list:
        pav_id = int(pav_dict[RegistryKey.IDENTIFIER])
        pav = scribe.get_matrix_column(mtx_col_id=pav_id)
        if pav is None:
            raise LMError('Failed to get PAV {}'.format(pav_id))
        try:
            pav_data = decompress(pav_dict[RegistryKey.COMPRESSED_PAV_DATA])
            status = JobStatus.COMPLETE
        except Exception:
            status = JobStatus.IO_MATRIX_READ_ERROR

        pav.update_status(status)
        scribe.update_object(pav)
    scribe.close_connections()


# .............................................................................
def stockpile_objects(stockpile_list):
    """Stockpiles objects from a list of dictionary entries
    """
    log = ConsoleLogger()
    scribe = BorgScribe(log)
    scribe.open_connections()

    for stockpile_dict in stockpile_list:
        obj_id = int(stockpile_dict[RegistryKey.IDENTIFIER])
        test_file = stockpile_dict[RegistryKey.PRIMARY_OUTPUT]
        process_type = int(stockpile_dict[RegistryKey.PROCESS_TYPE])
        status = int(stockpile_dict[RegistryKey.STATUS])

        # Get object
        if ProcessType.is_occurrence(process_type):
            obj = scribe.get_occurrence_set(occ_id=obj_id)
            test_method = test_spatial
        elif ProcessType.is_project(process_type):
            obj = scribe.get_sdm_project(obj_id)
            test_method = test_spatial
        elif ProcessType.is_matrix(process_type):
            obj = scribe.get_matrix(mtx_id=obj_id)
            test_method = test_matrix
        elif ProcessType.is_intersect(process_type):
            obj = scribe.get_matrix_column(mtx_col_id=obj_id)
            test_method = test_matrix
        else:
            raise LMError(
                'Unsupported process type {} for object {}'.format(
                    process_type, obj_id))
        if obj is None:
            raise LMError(
                'Failed to get object {} for process {}'.format(
                    obj_id, process_type))

        log.debug('Test object: ptype {}, object id {}, status {}'.format(
            process_type, obj_id, status))
        if status < JobStatus.GENERAL_ERROR:
            # Test outputs
            status = test_method(test_file)
            # Attempt to update verify
            try:
                obj.set_verify()
            except AttributeError:
                # If the object doesn't have the setVerify method, pass
                pass

            # TODO: Test secondary outputs

        # Update the object in the database
        log.debug(
            'Updating process type {}, object {}, with status {}'.format(
                process_type, obj_id, status))
        obj.update_status(status)
        scribe.update_object(obj)

    scribe.close_connections()


# .............................................................................
def test_matrix(matrix_filename):
    """Tests a matrix file

    Args:
        matrix_filename : The matrix file to test
    """
    test_status = JobStatus.COMPLETE
    if os.path.exists(matrix_filename):
        try:
            Matrix.load(matrix_filename)
        except Exception:
            test_status = JobStatus.IO_GENERAL_ERROR
    else:
        test_status = JobStatus.NOT_FOUND
    return test_status


# .............................................................................
def test_spatial(spatial_filename):
    """Tests a spatial file

    Args:
        spatial_filename : The spatial file to test
    """
    test_status = JobStatus.COMPLETE
    if os.path.exists(spatial_filename):
        try:
            success = False
            _, ext = os.path.splitext(spatial_filename)
            file_format = LMFormat.get_format_by_extension(ext)
            if LMFormat.is_ogr(ext=ext):
                success, _feat_count = Vector.test_vector(
                    spatial_filename, driver=file_format.driver)
            elif LMFormat.is_gdal(ext=ext):
                success = Raster.test_raster(spatial_filename)
            else:
                raise LMError(
                    'File is not a valid spatial file: {}'.format(
                        spatial_filename))
            if not success:
                test_status = JobStatus.GENERAL_ERROR
        except Exception:
            test_status = JobStatus.IO_GENERAL_ERROR
    else:
        test_status = JobStatus.NOT_FOUND
    return test_status


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(description='Stockpile multiple objects')
    parser.add_argument(
        '-p', '--pavs_filename', type=str,
        help='A JSON file containing PAV data for stockpiling')
    parser.add_argument(
        'stockpile_filename', type=str,
        help='A JSON file containing stockpile information')
    parser.add_argument(
        'success_filename', type=str,
        help='A file location to write an indication of success')
    args = parser.parse_args()

    try:
        with open(args.stockpile_filename) as in_stockpile:
            stockpile_list = json.load(in_stockpile)
            stockpile_objects(stockpile_list)
        msg = 'success'
    except Exception as e:
        msg = str(e)

    if args.pavs_filename is not None:
        with open(args.pavs_filename) as in_pavs:
            pav_list = json.load(in_pavs)
            stockpile_pavs(pav_list)

    with open(args.success_filename, 'w') as out_success:
        out_success.write(msg)


# .............................................................................
if __name__ == '__main__':
    main()
