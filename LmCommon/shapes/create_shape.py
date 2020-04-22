"""Module containing functions to create a shapefile from occurrence data
"""
import json
import os
from random import shuffle

from LmBackend.common.lmobj import LMError
from LmCommon.common.lmconstants import (
    DEFAULT_EPSG, DwcNames, GBIF, JobStatus, LM_WKT_FIELD, LMFormat,
    PROVIDER_FIELD_COMMON)
from LmCommon.common.occ_parse import OccDataParser
from LmCommon.common.ready_file import ready_filename
from LmCompute.common.log import LmComputeLogger
from osgeo import ogr, osr


# .............................................................................
class ShapeShifter:
    """Class to write a shapefile from GBIF CSV output or BISON JSON output.

    Note:
        Puts all valid records from input csv file into a single shapefile.
    """

    # .......................
    def __init__(self, csv_f_name, metadata, logger=None, delimiter='\t',
                 is_gbif=False):
        """Constructor

        Args:
            csv_f_name: File containing CSV data of species occurrence records
            metadata: dictionary or filename containing JSON format metadata
            logger: logger for debugging output
            delimiter: delimiter of values in csv records
            is_gbif: boolean flag to indicate whether data contains GBIF/DwC
                fields.
        """
        if not os.path.exists(csv_f_name):
            raise LMError(
                JobStatus.LM_RAW_POINT_DATA_ERROR,
                'Raw data file {} does not exist'.format(csv_f_name))
        if not metadata:
            raise LMError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Failed to get metadata')
        if logger is None:
            logname, _ = os.path.splitext(os.path.basename(__file__))
            logger = LmComputeLogger(logname, add_console=True)

        self._reader = None
        # If necessary, map provider dictionary keys to our field names
        self.lookup_fields = None
        self._curr_rec_num = 0
        count = sum(1 for _ in open(csv_f_name))
        self._rec_count = count
        self.link_field = None
        self.link_url = None
        self.provider_key_field = None
        self.computed_provider_field = None
        self.occ_parser = None

        if is_gbif:
            self.link_field = GBIF.LINK_FIELD
            self.link_url = GBIF.LINK_PREFIX
            self.link_id_field = GBIF.ID_FIELD
            self.provider_key_field = GBIF.PROVIDER_FIELD
            self.computed_provider_field = PROVIDER_FIELD_COMMON

        self.occ_parser = OccDataParser(
            logger, csv_f_name, metadata, delimiter=delimiter,
            pull_chunks=False)
        self.occ_parser.initialize_me()
        if self.occ_parser.header is not None:
            self._rec_count = self._rec_count - 1
        self.id_field = self.occ_parser.id_field_name
        if self.occ_parser.x_field_name is not None:
            self.x_field = self.occ_parser.x_field_name
        else:
            self.x_field = DwcNames.DECIMAL_LONGITUDE['SHORT']
        if self.occ_parser.y_field_name is not None:
            self.y_field = self.occ_parser.y_field_name
        else:
            self.y_field = DwcNames.DECIMAL_LATITUDE['SHORT']
        self.pt_field = self.occ_parser.pt_field_name

        self.special_fields = (
            self.id_field, self.link_field, self.provider_key_field,
            self.computed_provider_field)

# .............................................................................
# Private functions
# .............................................................................
    def _create_fill_feat(self, lyr_def, rec_dict, lyr):
        feat = ogr.Feature(lyr_def)
        try:
            self._fill_feature(feat, rec_dict)
        except Exception as err:
            print(('Failed to _create_fill_feat, e = {}'.format(err)))
            raise LMError('Failed to create feature', err)
        else:
            # Create new feature, setting FID, in this layer
            lyr.CreateFeature(feat)
            feat.Destroy()

# .............................................................................
# Public functions
# .............................................................................

# ...............................................
    @staticmethod
    def test_shapefile(dlocation):
        """Test the validity of a shapefile.

        Todo:
            This should go into a LmCommon base layer class
        """
        good_data = True
        feat_count = 0
        if dlocation is not None and os.path.exists(dlocation):
            ogr.RegisterAll()
            drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
            try:
                data_set = drv.Open(dlocation)
            except Exception:
                good_data = False
            else:
                try:
                    s_lyr = data_set.GetLayer(0)
                except Exception:
                    good_data = False
                else:
                    feat_count = s_lyr.GetFeatureCount()
        if not good_data:
            raise LMError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Failed to open dataset or layer {}'.format(dlocation))
        if feat_count == 0:
            raise LMError(
                JobStatus.OCC_NO_POINTS_ERROR,
                'Failed to create shapefile with > 0 points {}'.format(
                    dlocation))
        return good_data, feat_count

    # .............................................................................
    def write_occurrences(self, out_f_name, max_points=None, big_f_name=None,
                          overwrite=True):
        """Write the occurrences to a shapefile.

        Args:
            out_f_name: destination filename
            max_points: maximum number of points to include in shapefile
            big_f_name: destination filename for shapefile with all points
            overwrite: flag indicating whether to overwrite an existing file,
                or throw an Exception

        Raises:
            LMError if write failure
        """
        if not ready_filename(out_f_name, overwrite=overwrite):
            raise LMError(
                '{} is not ready for write (overwrite={})'.format(
                    out_f_name, overwrite))
        discard_indices = self._get_subset(max_points)
        # Create empty datasets with field definitions
        out_dataset = big_dataset = None
        try:
            out_dataset = self._create_dataset(out_f_name)
            out_lyr = self._add_user_field_def(out_dataset)
            lyr_def = out_lyr.GetLayerDefn()

            # Do we need a BIG dataset?
            if len(discard_indices) > 0 and big_f_name is not None:
                if not ready_filename(big_f_name, overwrite=overwrite):
                    raise LMError(
                        '{} is not ready for write (overwrite={})'.format(
                            big_f_name, overwrite))
                big_dataset = self._create_dataset(big_f_name)
                big_lyr = self._add_user_field_def(big_dataset)
        except Exception as e:
            raise LMError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Unable to create field definitions ({})'.format(e))
        # Fill datasets with records
        try:
            # Loop through records
            rec_dict = self._get_record()
            while rec_dict is not None:
                try:
                    # Add non-discarded features to regular layer
                    if self._curr_rec_num not in discard_indices:
                        self._create_fill_feat(lyr_def, rec_dict, out_lyr)
                    # Add all features to optional "Big" layer
                    if big_dataset is not None:
                        self._create_fill_feat(lyr_def, rec_dict, big_lyr)
                except Exception as e:
                    print(('Failed to create record ({})'.format((e))))
                rec_dict = self._get_record()

            # Return metadata
            (min_x, max_x, min_y, max_y) = out_lyr.GetExtent()
            geom_type = lyr_def.GetGeomType()
            f_count = out_lyr.GetFeatureCount()
            # Close dataset and flush to disk
            out_dataset.Destroy()
            self._finish_write(
                out_f_name, min_x, max_x, min_y, max_y, geom_type, f_count)

            # Close Big dataset and flush to disk
            if big_dataset is not None:
                big_count = big_lyr.GetFeatureCount()
                big_dataset.Destroy()
                self._finish_write(
                    big_f_name, min_x, max_x, min_y, max_y, geom_type,
                    big_count)

        except LMError as err:
            raise
        except Exception as err:
            raise LMError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Unable to read or write data ({})'.format(err),
                err)

# .............................................................................
# Private functions
# .............................................................................
    # .............................................................................
    @staticmethod
    def _create_dataset(f_name):
        drv = ogr.GetDriverByName(LMFormat.SHAPE.driver)
        new_dataset = drv.CreateDataSource(f_name)
        if new_dataset is None:
            raise LMError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Dataset creation failed for {}'.format(f_name))
        return new_dataset

    # .............................................................................
    def _get_subset(self, max_points):
        discard_indices = []
        if max_points is not None and self._rec_count > max_points:
            discard_count = self._rec_count - max_points
            all_indices = list(range(self._rec_count))
            shuffle(all_indices)
            discard_indices = all_indices[:discard_count]
        return discard_indices

    # .............................................................................
    def _finish_write(self, out_f_name, min_x, max_x, min_y, max_y, geom_type,
                      f_count):
        print(('Closed/wrote {}-feature dataset {}'.format(
            f_count, out_f_name)))

        # Write shapetree index for faster access
        # TODO: Uncomment this if we restore shapetree on compute nodes
        # try:
        #     shp_tree_cmd = os.path.join(BIN_PATH, 'shptree')
        #     ret_code = subprocess.call([shp_tree_cmd, out_f_name])
        #     if ret_code != 0:
        #         print(
        #             'Unable to create shapetree index on {}'.format(
        #                 out_f_name))
        # except Exception as e:
        #     print(
        #         'Unable to create shapetree index on {}: {}'.format(
        #             out_f_name, str(e)))

        # Test output data
        good_data, feat_count = self.test_shapefile(out_f_name)
        if not good_data:
            raise LMError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Failed to create shapefile {}'.format(out_f_name))
        if feat_count == 0:
            raise LMError(
                JobStatus.OCC_NO_POINTS_ERROR,
                'Failed to create shapefile {}'.format(out_f_name))

        # Write metadata as JSON
        basename, _ = os.path.splitext(out_f_name)
        self._write_metadata(
            basename, geom_type, f_count, min_x, min_y, max_x, max_y)

    # ...............................................
    @staticmethod
    def _write_metadata(basename, geom_type, count, min_x, min_y, max_x,
                        max_y):
        meta_dict = {
            'ogrformat': LMFormat.SHAPE.driver,
            'geomtype': geom_type, 'count': count, 'minx': min_x,
            'miny': min_y, 'maxx': max_x, 'maxy': max_y}
        with open(basename + '.meta', 'w') as out_file:
            json.dump(meta_dict, out_file)

    # ...............................................
    def _lookup(self, name):
        if self.lookup_fields is not None:
            try:
                val = self.lookup_fields[name]
                return val
            except Exception:
                return None
        else:
            return name

    # ...............................................
    def _get_record(self):
        success = False
        tmp_dict = {}
        rec_dict = None
        bad_rec_count = 0
        # skip lines w/o valid coordinates
        while not success and not self.occ_parser.closed:
            try:
                self.occ_parser.pull_next_valid_rec()
                this_rec = self.occ_parser.curr_line
                if this_rec is not None:
                    x_coord, y_coord = OccDataParser.get_xy(
                        this_rec, self.occ_parser.x_idx, self.occ_parser.y_idx,
                        self.occ_parser.pt_idx)
                    # Unique identifier field is not required, default to FID
                    # ignore records without valid lat/long; all occ jobs
                    #    contain these fields
                    tmp_dict[self.x_field] = float(x_coord)
                    tmp_dict[self.y_field] = float(y_coord)
                    success = True
            except StopIteration:
                success = True
            except (OverflowError, ValueError, Exception) as err:
                bad_rec_count += 1
                print(('Exception reading line {} ({})'.format(
                    self.occ_parser.curr_rec_num, str(err))))

        if success:
            for idx, vals in self.occ_parser.column_meta.items():
                if vals is not None and idx not in (
                        self.occ_parser.x_idx, self.occ_parser.y_idx):
                    fld_name = self.occ_parser.column_meta[idx][
                        OccDataParser.FIELD_NAME_KEY]
                    tmp_dict[fld_name] = this_rec[idx]
            rec_dict = tmp_dict

        if bad_rec_count > 0:
            print(('Skipped over {} bad records'.format(bad_rec_count)))

        if rec_dict is not None:
            self._curr_rec_num += 1

        return rec_dict

    # ...............................................
    def _add_user_field_def(self, new_dataset):
        sp_ref = osr.SpatialReference()
        sp_ref.ImportFromEPSG(DEFAULT_EPSG)
        max_str_len = LMFormat.get_str_len_for_default_ogr()

        new_lyr = new_dataset.CreateLayer(
            'points', geom_type=ogr.wkbPoint, srs=sp_ref)
        if new_lyr is None:
            raise LMError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Layer creation failed')

        for _, vals in self.occ_parser.column_meta.items():
            if vals is not None:
                fld_name = str(vals[OccDataParser.FIELD_NAME_KEY])
                fld_type = vals[OccDataParser.FIELD_TYPE_KEY]
                fld_def = ogr.FieldDefn(fld_name, fld_type)
                if fld_type == ogr.OFTString:
                    fld_def.SetWidth(max_str_len)
                return_val = new_lyr.CreateField(fld_def)
                if return_val != 0:
                    raise LMError(
                        JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                        'Failed to create field {}'.format(fld_name))

        # Add wkt field
        fld_def = ogr.FieldDefn(LM_WKT_FIELD, ogr.OFTString)
        fld_def.SetWidth(max_str_len)
        return_val = new_lyr.CreateField(fld_def)
        if return_val != 0:
            raise LMError(
                JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR,
                'Failed to create field {}'.format(fld_name))
        return new_lyr

    # ...............................................
    def _handle_special_fields(self, feat, rec_dict):
        try:
            # Find or assign a (dataset) unique id for each point
            if self.id_field is not None:
                try:
                    pt_id = rec_dict[self.id_field]
                except AttributeError:
                    # Set LM added id field
                    pt_id = self._curr_rec_num
                    feat.SetField(self.id_field, pt_id)

            # If data has a Url link field
            if self.link_field is not None:
                try:
                    search_id = rec_dict[self.link_field]
                except Exception:
                    pass
                else:
                    pt_url = '{}{}'.format(self.link_url, str(search_id))
                    feat.SetField(self.link_field, pt_url)

            # If data has a provider field and value to be resolved
            if self.computed_provider_field is not None:
                prov = ''
                try:
                    prov = rec_dict[self.provider_key_field]
                except Exception:
                    pass
                if not isinstance(prov, str):
                    prov = ''
                feat.SetField(self.computed_provider_field, prov)

        except Exception as err:
            print(('Failed to set optional field in rec {}, e = {}'.format(
                str(rec_dict), err)))
            raise LMError(
                'Failed to set optional field in rec {}'.format(rec_dict), err)

    # ...............................................
    def _fill_feature(self, feat, rec_dict):
        """Fill a feature using the provided dictionary."""
        try:
            x_coord = rec_dict[self.occ_parser.x_idx]
            y_coord = rec_dict[self.occ_parser.y_idx]
        except (AttributeError, KeyError):
            x_coord = rec_dict[self.x_field]
            y_coord = rec_dict[self.y_field]

        try:
            # Set LM added fields, geometry, geomwkt
            wkt = 'POINT ({} {})'.format(x_coord, y_coord)
            feat.SetField(LM_WKT_FIELD, wkt)
            geom = ogr.CreateGeometryFromWkt(wkt)
            feat.SetGeometryDirectly(geom)
        except Exception as err:
            print(('Failed to create/set geometry, e = {}'.format(err)))
            raise LMError('Failed to create / set geometry', err)

        self._handle_special_fields(feat, rec_dict)

        try:
            # Add values out of the line of data
            for name in rec_dict.keys():
                if (name in feat.keys() and name not in self.special_fields):
                    # Handles reverse lookup for BISON metadata
                    # TODO: make this consistent!!!
                    # For User data, name = fldname
                    fld_name = self._lookup(name)
                    if fld_name is not None:
                        fld_idx = feat.GetFieldIndex(str(fld_name))
                        val = rec_dict[name]
                        if val is not None and val != 'None':
                            feat.SetField(fld_idx, val)
        except Exception as err:
            print(('Failed to fill feature with rec dict {}, e = {}'.format(
                str(rec_dict), err)))
            raise LMError('Failed to fill feature with dict {}'.format(
                rec_dict), err)


# ...............................................
if __name__ == '__main__':
    print('__main__ is not implemented')

"""
from osgeo import ogr, osr
import StringIO
import subprocess

from LmBackend.common.occparse import OccDataParser
from LmCommon.shapes.create_shape import ShapeShifter
from LmCommon.common.lmconstants import (ENCODING, BISON, BISON_QUERY,
                    GBIF, GBIF_QUERY, IDIGBIO, IDIGBIO_QUERY,
                    PROVIDER_FIELD_COMMON,
                    LM_ID_FIELD, LM_WKT_FIELD, ProcessType, JobStatus,
                    DWCNames, LMFormat)
from LmServer.common.log import ScriptLogger
import ast

log = LmComputeLogger('csvocc_testing', addConsole=True)

# ......................................................
# User test
csvfname = '/share/lm/data/archive/taffy/000/000/396/487/pt_396487.csv'
metafname = '/share/lm/data/archive/taffy/heuchera.json'
outfname = '/state/partition1/lmscratch/temp/testpoints.shp'
bigfname = '/state/partition1/lmscratch/temp/testpoints_big.shp'

with open(csvfname, 'r') as f:
    blob = f.read()

with open(csvfname, 'r') as f:
    blob2 = f.readlines()

# with open(metafname, 'r') as f:
#     metad = ast.literal_eval(f.read())

shaper = ShapeShifter(blob, metafname, logger=logger)
shaper.writeOccurrences(outfname, maxPoints=50, bigfname=bigfname)


# ......................................................
# GBIF test
csv_fn = '/share/lm/data/archive/kubi/000/000/398/505/pt_398505.csv'
out_fn = '/state/partition1/lmscratch/temp/test_points'
big_out_fn = '/state/partition1/lmscratch/temp/big_test_points'
metadata = '/share/lmserver/data/species/gbif_occ_subset-2019.01.10.json'
delimiter = '\t'
maxPoints = 500
ready_filename(out_fn, overwrite=True)
ready_filename(big_out_fn, overwrite=True)


with open(csv_fn) as inF:
    csvInputBlob = inF.readlines()

rawdata = ''.join(csvInputBlob)
count = len(csvInputBlob)

log = LmComputeLogger('csvocc_testing', addConsole=True)

shaper = ShapeShifter(
    rawData, metadata, count, logger=log, delimiter='\t', isGbif=True)
shaper.writeOccurrences(out_fn, maxPoints=maxPoints, bigfname=big_out_fn)

status = JobStatus.COMPUTED
goodData, featCount = ShapeShifter.test_shapefile(outFile)

# ......................................................
"""