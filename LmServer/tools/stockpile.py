#!/bin/bash
"""This script updates a Lifemapper object in the database
"""
import argparse
import json
import glob
import os
import shutil

from LmBackend.common.lmobj import LMError, LMObject
from LmCommon.common.lmconstants import ProcessType, LMFormat, JobStatus
from LmServer.base.layer2 import Vector, Raster
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe


# .............................................................................
class Stockpile(LMObject):
    """Update database with status of generated object files
    """
    # ..................................
    @classmethod
    def test_and_stash(cls, ptype, obj_id, status, success_f_name,
                       output_f_name_list, meta_filename=None):
        """Test output files and update DB with status.

        Args:
            ptype: ProcessType for the process being examined
            obj_id: Unique database ID for the object to update
            status: Assumed status of object output files
            success_f_name:Filename to be written IFF success=True. Contains
                final status and testing results
            output_f_name_listList of output files returned by the
                computational process
            meta_filename: filename for JSON matrix metadata

        Returns:
            Boolean indicating success of database update
        """
        output_info = []
        success = True

        # Check incoming status
        if status >= JobStatus.GENERAL_ERROR:
            success = False
            output_info.append('Incoming status value was {}'.format(success))
        else:
            # Test each file
            for fname in output_f_name_list:
                curr_success, msgs = cls.test_file(fname)
                if not curr_success:
                    success = False
                    output_info.extend(msgs)

            if not success:
                status = JobStatus.GENERAL_ERROR
            else:
                status = JobStatus.COMPLETE

        # Update database
        scribe = BorgScribe(ConsoleLogger())
        scribe.openConnections()
        try:
            obj = cls._get_object(scribe, ptype, obj_id)
            cls._copy_object(ptype, obj, output_f_name_list, meta_filename)
            cls._update_object(scribe, obj, status)
        except Exception as err:
            msg = 'Exception on Stockpile._updateObject ({})'.format(
                str(err))
            print(msg)

            success = False
            raise LMError(msg, err)
        finally:
            scribe.closeConnections()

        return success

    # ..................................
    @classmethod
    def _get_object(cls, scribe, ptype, obj_id):
        """Get object and update DB with status.
        """
        # Get object
        obj = None
        try:
            if ProcessType.isOccurrence(ptype):
                obj = scribe.getOccurrenceSet(occId=obj_id)
            elif ProcessType.isProject(ptype):
                obj = scribe.getSDMProject(obj_id)
            elif ptype == ProcessType.RAD_BUILDGRID:
                obj = scribe.getShapeGrid(lyrId=obj_id)
            elif ProcessType.isMatrix(ptype):
                obj = scribe.getMatrix(mtxId=obj_id)
            elif ProcessType.isIntersect(ptype):
                obj = scribe.getMatrixColumn(mtxcolId=obj_id)
            else:
                raise LMError(
                    'Unsupported ProcessType {} for object {}'.format(
                        ptype, obj_id))
        except Exception as err:
            raise LMError(
                'Failed to get object {} for process {}, exception {}'.format(
                    obj_id, ptype, str(err)))
        if obj is None:
            raise LMError(
                'Failed to get object {} for process {}'.format(obj_id, ptype))
        return obj

    # ..................................
    @classmethod
    def _copy_object(cls, ptype, obj, file_names, meta_filename):
        """Get object and update DB with status.
        """
        metadata = None
        try:
            with open(meta_filename) as in_meta:
                metadata = json.load(in_meta)
        except Exception:
            pass
        # Copy data
        try:
            if (ProcessType.isOccurrence(ptype) and
                    os.path.getsize(file_names[0]) > 0):
                # Move data file
                base_out_dir = os.path.dirname(obj.getDLocation())
                for filename in glob.glob(
                        '{}.*'.format(os.path.splitext(file_names[0])[0])):
                    shutil.copy(filename, base_out_dir)
                # Try big data file
                big_f_name = file_names[0].replace('/pt', '/bigpt')
                if cls.test_file(big_f_name)[0]:
                    shutil.copy(big_f_name, obj.getDlocation(largeFile=True))
            elif ProcessType.isProject(ptype) and \
                    os.path.getsize(file_names[0]) > 0:
                shutil.copy(file_names[0], obj.getDLocation())
                shutil.copy(file_names[1], obj.getProjPackageFilename())
            elif ProcessType.isMatrix(ptype) and \
                    os.path.getsize(file_names[0]) > 0:
                if metadata is not None:
                    obj.addMtxMetadata(metadata)
                if os.path.exists(obj.getDLocation()):
                    os.remove(obj.getDLocation())
                shutil.copy(file_names[0], obj.getDLocation())
        except Exception as err:
            raise LMError(
                'Exception copying primary {} or an output, ({})'.format(
                    obj.getDLocation(), str(err)), err)

    # ..................................
    @classmethod
    def _update_object(cls, scribe, obj, status):
        """Get object and update DB with status.
        """
        # Update verify hash and modtime for layers
        try:
            obj.updateLayer()
        except Exception:
            pass

        obj.updateStatus(status)

        # Update database
        try:
            scribe.updateObject(obj)
        except Exception as err:
            raise LMError(
                'Exception updating object {} ({})'.format(
                    obj.getId(), str(err)), err)

    # ...............................................
    @classmethod
    def test_file(cls, output_f_name):
        """Test the validity of a file
        """
        success = True
        msgs = []
        _, ext = os.path.splitext(output_f_name)
        if not os.path.exists(output_f_name):
            msgs.append('File {} does not exist'.format(output_f_name))
            success = False
        elif LMFormat.isTestable(ext):
            if LMFormat.isGeo(ext):
                file_format = LMFormat.getFormatByExtension(ext)
                if LMFormat.isOGR(ext=ext):
                    success, feat_count = Vector.testVector(
                        output_f_name, driver=file_format.driver)
                    if not success:
                        try:
                            with open(output_f_name, 'r') as in_f:
                                msg = in_f.read()
                            msgs.append(msg)
                        except Exception:
                            pass
                        msgs.append(
                            'File {} is not a valid {} file'.format(
                                output_f_name, file_format.driver))
                    elif feat_count < 1:
                        msgs.append(
                            'Vector {} has no features'.format(output_f_name))

                elif LMFormat.isGDAL(ext=ext):
                    success = Raster.testRaster(output_f_name)
                    if not success:
                        msgs.append(
                            'File {} is not a valid GDAL file'.format(
                                output_f_name))
            else:
                with open(output_f_name, 'r') as in_f:
                    data = in_f.read()
                if LMFormat.isJSON(ext):
                    try:
                        json.loads(data)
                    except Exception:
                        success = False
                        msgs.append(
                            'File {} does not contain valid JSON'.format(
                                output_f_name))
        return success, msgs


# .............................................................................
def main():
    """Main method of script
    """
    parser = argparse.ArgumentParser(
        description='This script updates a Lifemapper object')
    # Inputs
    parser.add_argument(
        'process_type', type=int,
        help='The process type of the object to update')
    parser.add_argument(
        'object_id', type=int, help='The id of the object to update')
    parser.add_argument(
        'success_filename', type=str,
        help='File to be created only if the job was completed successfully.')
    parser.add_argument(
        'object_output', type=str, nargs='*', help='Files to sanity check. ')
    # Status arguments
    parser.add_argument(
        '-s', dest='status', type=int,
        help='The status to update the object with')
    parser.add_argument(
        '-f', dest='status_file', type=str,
        help='A file containing the new object status')

    # Metadata filename
    parser.add_argument(
        '-m', dest='metadata_filename', type=str,
        help='A JSON file containing metadata about this object')
    args = parser.parse_args()

    # Status comes in as an integer or file
    status = None
    if args.status is not None:
        status = args.status
    elif args.status_file is not None:
        try:
            with open(args.status_file) as status_in:
                status = int(status_in.read())
        except IOError:
            # Need to catch empty status file
            status = JobStatus.GENERAL_ERROR

    success = Stockpile.test_and_stash(
        args.process_type, args.object_id, status, args.success_filename,
        args.object_output, meta_filename=args.metadata_filename)

    with open(args.success_filename, 'w') as success_out:
        success_out.write(int(success))


# .............................................................................
if __name__ == '__main__':
    main()
