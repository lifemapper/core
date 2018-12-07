"""Module containing command objects for operations on single species
"""
import os

from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import CMD_PYBIN, SINGLE_SPECIES_SCRIPTS_DIR

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
        @param shapegridFilename: The file location of the shapegrid to
            intersect
        @param rasterFilename: The file location of the raster file to
            intersect with the shapegrid
        @param grimColFilename: The file location to write the GRIM column
        @param resolution: The resolution of the raster
        @param minPercent: If provided, use largest class method, otherwise use
            weighted mean
        @param ident: If included, use this for a label on the GRIM column 
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegridFilename, rasterFilename])
        self.outputs.append(grimColFilename)
        
        self.args = [shapegridFilename, rasterFilename, grimColFilename]
        self.optArgs = ''
        if minPercent:
            self.optArgs += ' -m largest_class'
        if ident is not None:
            self.optArgs += '-i {}'.format(ident)

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {} {}'.format(
            CMD_PYBIN, self.getScript(), self.optArgs, ' '.join(self.args))

# .............................................................................
class IntersectRasterCommand(_LmCommand):
    """
    @summary: This command intersects a raster layer and a shapegrid
    """
    relDir = SINGLE_SPECIES_SCRIPTS_DIR
    scriptName = 'intersect_raster.py'

    # ................................
    def __init__(self, shapegridFilename, rasterFilename, pavFilename, 
                 minPresence, maxPresence, percentPresence, squid=None,
                 layerStatusFilename=None, statusFilename=None):
        """
        @summary: Construct the command object
        @param shapegridFilename: The file location of the shapegrid to
            intersect
        @param rasterFilename: The file location of the raster file to
            intersect with the shapegrid
        @param pavFilename: The file location to write the resulting PAV
        @param resolution: The resolution of the raster
        @param minPresence: The minimum value to be considered present
        @param maxPresence: The maximum value to be considered present
        @param percentPresence: The percent of a shapegrid feature that must be
            present to be considered present
        @param squid: If included, use this for a label on the PAV 
        @param layerStatusFilename: If provided, check this for the status of
            the input layer object
        @param statusFilename: If provided, write out status to this location
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegridFilename, rasterFilename])
        self.outputs.append(pavFilename)
        
        self.args = [
            shapegridFilename, rasterFilename, pavFilename, str(minPresence),
            str(maxPresence), str(percentPresence)]
        
        self.optArgs = ''
        if squid is not None:
            self.optArgs += ' --squid={}'.format(squid)
        
        if layerStatusFilename is not None:
            self.optArgs += '  --layer_status_file={}'.format(
                layerStatusFilename)
            self.inputs.append(layerStatusFilename)
            
        if statusFilename is not None:
            self.optArgs += ' -s {}'.format(statusFilename)
            self.outputs.append(statusFilename)
        
    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {} {}'.format(CMD_PYBIN, self.getScript(),
                            self.optArgs, ' '.join(self.args))

# .............................................................................
class IntersectVectorCommand(_LmCommand):
    """
    @summary: This command intersects a vector layer and a shapegrid
    """
    relDir = SINGLE_SPECIES_SCRIPTS_DIR
    scriptName = 'intersect_vector.py'

    # ................................
    def __init__(self, shapegridFilename, vectorFilename, pavFilename, 
                 presenceAttrib, minPresence, maxPresence, percentPresence, 
                 squid=None):
        """
        @summary: Construct the command object
        @param shapegridFilename: The file location of the shapegrid to
            intersect
        @param vectorFilename: The file location of the vector file to
            intersect with the shapegrid
        @param pavFilename: The file location to write the resulting PAV
        @param presenceAttrib: The shapefile attribute to use for determining 
                                          presence
        @param minPresence: The minimum value to be considered present
        @param maxPresence: The maximum value to be considered present
        @param percentPresence: The percent of a shapegrid feature that must be
            present to be considered present
        @param squid: If included, use this for a label on the PAV 
        """
        _LmCommand.__init__(self)
        self.inputs.extend([shapegridFilename, vectorFilename])
        self.outputs.append(pavFilename)
        
        self.args = [
            shapegridFilename, vectorFilename, pavFilename, str(minPresence),
            str(maxPresence), str(percentPresence), presenceAttrib]
        self.optArgs = ''
        if squid is not None:
            self.optArgs += ' --squid={}'.format(squid)

    # ................................
    def getCommand(self):
        """
        @summary: Get the raw command to run on the system
        """
        return '{} {} {} {}'.format(
            CMD_PYBIN, self.getScript(), self.optArgs, ' '.join(self.args))

# .............................................................................
class SpeciesParameterSweepCommand(_LmCommand):
    """This command will perform a parameter sweep for a single species
    """
    relDir = SINGLE_SPECIES_SCRIPTS_DIR
    scriptName = 'species_controller.py'

    # ...............................
    def __init__(self, config_filename, inputs, outputs):
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
        
        self.args = config_filename
        
    # ................................
    def getCommand(self):
        """Get the raw command to run on the system
        """
        return '{} {} {}'.format(CMD_PYBIN, self.getScript(), self.args)
