"""Module containing command objects for operations on single species
"""
import os

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import SINGLE_SPECIES_SCRIPTS_DIR

# .............................................................................
class GrimRasterCommand(_LmCommand):
    """This command intersects a raster layer and a shapegrid for a GRIM
    """
    relDir = SINGLE_SPECIES_SCRIPTS_DIR
    scriptName = 'grim_raster.py'

    # ................................
    def __init__(self, shapegridFilename, rasterFilename, grimColFilename, 
                 minPercent=None, ident=None):
        """Construct the command object

        Args:
            shapegridFilename: The file location of the shapegrid to intersect
            rasterFilename: The file location of the raster file to intersect
                with the shapegrid
            grimColFilename: The file location to write the GRIM column
            resolution: The resolution of the raster
            minPercent: If provided, use largest class method, otherwise use
                weighted mean
            ident: If included, use this for a label on the GRIM column 
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegridFilename, rasterFilename])
        self.outputs.append(grimColFilename)
        
        self.args = '{} {} {}'.format(
            shapegridFilename, rasterFilename, grimColFilename)
        if minPercent:
            self.opt_args += ' -m largest_class'
        if ident is not None:
            self.opt_args += '-i {}'.format(ident)

# .............................................................................
class IntersectRasterCommand(_LmCommand):
    """This command intersects a raster layer and a shapegrid
    """
    relDir = SINGLE_SPECIES_SCRIPTS_DIR
    scriptName = 'intersect_raster.py'

    # ................................
    def __init__(self, shapegridFilename, rasterFilename, pavFilename, 
                 minPresence, maxPresence, percentPresence, squid=None,
                 layerStatusFilename=None, statusFilename=None):
        """Construct the command object

        Args:
            shapegridFilename: The file location of the shapegrid to intersect
            rasterFilename: The file location of the raster file to intersect
                with the shapegrid
            pavFilename: The file location to write the resulting PAV
            resolution: The resolution of the raster
            minPresence: The minimum value to be considered present
            maxPresence: The maximum value to be considered present
            percentPresence: The percent of a shapegrid feature that must be
                present to be considered present
            squid: If included, use this for a label on the PAV 
            layerStatusFilename: If provided, check this for the status of the
                input layer object
            statusFilename: If provided, write out status to this location
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegridFilename, rasterFilename])
        self.outputs.append(pavFilename)
        
        self.args = '{} {} {} {} {} {}'.format(
            shapegridFilename, rasterFilename, pavFilename, minPresence,
            maxPresence, percentPresence)
        
        if squid is not None:
            self.opt_args += ' --squid={}'.format(squid)
        
        if layerStatusFilename is not None:
            self.opt_args += '  --layer_status_file={}'.format(
                layerStatusFilename)
            self.inputs.append(layerStatusFilename)
            
        if statusFilename is not None:
            self.opt_args += ' -s {}'.format(statusFilename)
            self.outputs.append(statusFilename)
        
# .............................................................................
class IntersectVectorCommand(_LmCommand):
    """This command intersects a vector layer and a shapegrid
    """
    relDir = SINGLE_SPECIES_SCRIPTS_DIR
    scriptName = 'intersect_vector.py'

    # ................................
    def __init__(self, shapegridFilename, vectorFilename, pavFilename, 
                 presenceAttrib, minPresence, maxPresence, percentPresence, 
                 squid=None):
        """Construct the command object
            shapegridFilename: The file location of the shapegrid to intersect
            vectorFilename: The file location of the vector file to intersect
                with the shapegrid
            pavFilename: The file location to write the resulting PAV
            presenceAttrib: The shapefile attribute to use for determining
                presence
            minPresence: The minimum value to be considered present
            maxPresence: The maximum value to be considered present
            percentPresence: The percent of a shapegrid feature that must be
                present to be considered present
            squid: If included, use this for a label on the PAV 
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegridFilename, vectorFilename])
        self.outputs.append(pavFilename)
        
        self.args = '{} {} {} {} {} {} {}'.format(
            shapegridFilename, vectorFilename, pavFilename, minPresence,
            maxPresence, percentPresence, presenceAttrib)
        if squid is not None:
            self.opt_args += ' --squid={}'.format(squid)

# .............................................................................
class SpeciesParameterSweepCommand(_LmCommand):
    """This command will perform a parameter sweep for a single species
    """
    relDir = SINGLE_SPECIES_SCRIPTS_DIR
    scriptName = 'species_controller.py'

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
