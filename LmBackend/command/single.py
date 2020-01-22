"""Module containing command objects for operations on single species
"""
import os

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import SINGLE_SPECIES_SCRIPTS_DIR


# .............................................................................
class GrimRasterCommand(_LmCommand):
    """This command intersects a raster layer and a shapegrid for a GRIM
    """
    relative_directory = SINGLE_SPECIES_SCRIPTS_DIR
    script_name = 'grim_raster.py'

    # ................................
    def __init__(self, shapegrid_file_name, raster_file_name,
                 grim_col_file_name, minPercent=None, ident=None):
        """Construct the command object

        Args:
            shapegrid_file_name: The file location of the shapegrid to
                intersect
            raster_file_name: The file location of the raster file to intersect
                with the shapegrid
            grim_col_file_name: The file location to write the GRIM column
            resolution: The resolution of the raster
            minPercent: If provided, use largest class method, otherwise use
                weighted mean
            ident: If included, use this for a label on the GRIM column
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegrid_file_name, raster_file_name])
        self.outputs.append(grim_col_file_name)

        self.args = '{} {} {}'.format(
            shapegrid_file_name, raster_file_name, grim_col_file_name)
        if minPercent:
            self.opt_args += ' -m largest_class'
        if ident is not None:
            self.opt_args += '-i {}'.format(ident)


# .............................................................................
class IntersectRasterCommand(_LmCommand):
    """This command intersects a raster layer and a shapegrid
    """
    relative_directory = SINGLE_SPECIES_SCRIPTS_DIR
    script_name = 'intersect_raster.py'

    # ................................
    def __init__(self, shapegrid_file_name, raster_file_name, pav_file_name,
                 min_presence, max_presence, percent_presence, squid=None,
                 layer_status_file_name=None, status_file_name=None):
        """Construct the command object

        Args:
            shapegrid_file_name: The file location of the shapegrid to
                intersect
            raster_file_name: The file location of the raster file to intersect
                with the shapegrid
            pav_file_name: The file location to write the resulting PAV
            resolution: The resolution of the raster
            min_presence: The minimum value to be considered present
            max_presence: The maximum value to be considered present
            percent_presence: The percent of a shapegrid feature that must be
                present to be considered present
            squid: If included, use this for a label on the PAV
            layer_status_file_name: If provided, check this for the status of
                the input layer object
            status_file_name: If provided, write out status to this location
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegrid_file_name, raster_file_name])
        self.outputs.append(pav_file_name)

        self.args = '{} {} {} {} {} {}'.format(
            shapegrid_file_name, raster_file_name, pav_file_name, min_presence,
            max_presence, percent_presence)

        if squid is not None:
            self.opt_args += ' --squid={}'.format(squid)

        if layer_status_file_name is not None:
            self.opt_args += '  --layer_status_file={}'.format(
                layer_status_file_name)
            self.inputs.append(layer_status_file_name)

        if status_file_name is not None:
            self.opt_args += ' -s {}'.format(status_file_name)
            self.outputs.append(status_file_name)


# .............................................................................
class IntersectVectorCommand(_LmCommand):
    """This command intersects a vector layer and a shapegrid
    """
    relative_directory = SINGLE_SPECIES_SCRIPTS_DIR
    script_name = 'intersect_vector.py'

    # ................................
    def __init__(self, shapegrid_file_name, vector_file_name, pav_file_name,
                 presence_attrib, min_presence, max_presence, percent_presence,
                 squid=None):
        """Construct the command object

        Args
            shapegrid_file_name: The file location of the shapegrid to
                intersect
            vector_file_name: The file location of the vector file to intersect
                with the shapegrid
            pav_file_name: The file location to write the resulting PAV
            presence_attrib: The shapefile attribute to use for determining
                presence
            min_presence: The minimum value to be considered present
            max_presence: The maximum value to be considered present
            percent_presence: The percent of a shapegrid feature that must be
                present to be considered present
            squid: If included, use this for a label on the PAV
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegrid_file_name, vector_file_name])
        self.outputs.append(pav_file_name)

        self.args = '{} {} {} {} {} {} {}'.format(
            shapegrid_file_name, vector_file_name, pav_file_name, min_presence,
            max_presence, percent_presence, presence_attrib)
        if squid is not None:
            self.opt_args += ' --squid={}'.format(squid)


# .............................................................................
class SpeciesParameterSweepCommand(_LmCommand):
    """This command will perform a parameter sweep for a single species
    """
    relative_directory = SINGLE_SPECIES_SCRIPTS_DIR
    script_name = 'species_controller.py'

    # ...............................
    def __init__(self, config_filename, inputs, outputs, base_work_dir,
                 pedantic_mode=False):
        """Construct a command object for a single species parameter sweep

        Args:
            config_filename : A file containing configuration information that
                was written for the ParameterSweepConfiguration class
            inputs : A list of input files required for this parameter sweep
            outputs : A list of output files that will be generated by this
                parameter sweep.

        Note:
            * Use the ParameterSweepConfiguration class to determine inputs and
                outputs
        """
        _LmCommand.__init__(self)
        if not os.path.isabs(config_filename):
            self.inputs.append(config_filename)
        self.inputs.extend(inputs)
        self.outputs.extend(outputs)

        self.opt_args += ' -b {}'.format(base_work_dir)
        if pedantic_mode:
            self.opt_args += ' -p'
        self.args = config_filename
