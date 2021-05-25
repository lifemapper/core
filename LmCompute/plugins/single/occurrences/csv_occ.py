"""Module containing occurrence set processing functions
"""
import os

from LmBackend.common.lmobj import JobError
from LmCommon.common.lmconstants import JobStatus, LMFormat
from LmCommon.common.ready_file import ready_filename
from LmCommon.shapes.create_shape import ShapeShifter
from LmCompute.common.log import LmComputeLogger


# ...............................................
def _get_line_as_string(csv_reader, delimiter, rec_no):
    """Return line in CSV as a single string
    """
    success = False
    line_str = None
    while not success and csv_reader is not None:
        try:
            line = next(csv_reader)
            if line:
                rec_no += 1
                line_str = delimiter.join(line)
            success = True
        except OverflowError as err:
            rec_no += 1
            print(('Overflow on record {}, line {} ({})'.format(
                rec_no, csv_reader.line, str(err))))
        except StopIteration:
            success = True
        except Exception as err:
            rec_no += 1
            print(('Bad record on record {}, line {} ({})'.format(
                rec_no, csv_reader.line, err)))
    return line_str, rec_no


# .............................................................................
def create_shapefile_from_csv(csv_fname, metadata, out_fname, big_fname,
                              max_points, delimiter='\t', is_gbif=False,
                              log=None):
    """Parses a CSV-format dataset and saves it to a shapefile

    Args:
        csv_fname: Raw occurrence data for processing
        metadata: Metadata that can be used for processing the CSV
        out_fname: The file location to write the modelable occurrence set
        big_fname: The file location to write the full occurrence set
        max_points: The maximum number of points to be included in the regular
            shapefile
        is_gbif: Flag to indicate special processing for GBIF link, lookup keys
        log: If provided, use this logger.  If not, will create new
    """
    # Ready file names
    ready_filename(out_fname, overwrite=True)
    ready_filename(big_fname, overwrite=True)

    delimiter = str(delimiter)

    # Initialize logger if necessary
    if log is None:
        logname, _ = os.path.splitext(os.path.basename(__file__))
        log = LmComputeLogger(logname, add_console=True)

    try:
        shaper = ShapeShifter(
            csv_fname, metadata, logger=log, delimiter=delimiter,
            is_gbif=is_gbif)
        shaper.write_occurrences(
            out_fname, max_points=max_points, big_f_name=big_fname)
        log.debug('Shaper wrote occurrences')

        # Test generated shapefiles, throws exceptions if bad
        status = JobStatus.COMPUTED
        good_data, _ = ShapeShifter.test_shapefile(out_fname)
        if not good_data:
            raise JobError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Shaper tested, failed newly created occset')

        # Test the big shapefile if it exists
        if os.path.exists(big_fname):
            ShapeShifter.test_shapefile(big_fname)

    except JobError as job_err:
        log.debug(job_err.msg)
        status = job_err.code
        log.debug('Failed to write occurrences, return {}'.format(status))
        log.debug('Delete shapefiles if they exist')

        # TODO: Find a better way to delete (existing function maybe?)
        if os.path.exists(out_fname):
            out_base = os.path.splitext(out_fname)[0]
            for ext in LMFormat.SHAPE.get_extensions():
                file_name = '{}{}'.format(out_base, ext)
                if os.path.exists(file_name):
                    os.remove(file_name)

        if os.path.exists(big_fname):
            big_base = os.path.splitext(big_fname)[0]
            for ext in LMFormat.SHAPE.get_extensions():
                file_name = '{}{}'.format(big_base, ext)
                if os.path.exists(file_name):
                    os.remove(file_name)
    except Exception as err:
        log.debug(str(err))
        status = JobStatus.LM_POINT_DATA_ERROR
        log.debug('Failed to write occurrences, return {}'.format(status))

    return status
