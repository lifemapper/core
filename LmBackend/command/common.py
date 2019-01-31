"""Command commands
"""
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import (
    BACKEND_SCRIPTS_DIR, COMMON_SCRIPTS_DIR)

# .............................................................................
class IdigbioQueryCommand(_LmCommand):
    """
    @summary: This command will query iDigBio for a list of GBIF TaxonIds,
              write the results to a CSV file and metadata to a JSON file.
    """
    relDir = COMMON_SCRIPTS_DIR
    scriptName = 'get_idig_data.py'

    # ................................
    def __init__(self, taxon_id_file, point_output_file, meta_output_file,
                 missing_id_file):
        """
        @summary: Construct the command object
        @param configFile: Configuration file for the boom run
        """
        _LmCommand.__init__(self)
        
        # no need to add taxon_id_file to inputs
        self.outputs.append(point_output_file)
        self.outputs.append(meta_output_file)
        self.args = '{} {} {}'.format(taxon_id_file, point_output_file, 
                                         meta_output_file)
        if missing_id_file is not None:
            self.opt_args = '--missing_id_file {}'.format(missing_id_file)
            
# .............................................................................
class ChainCommand(_LmCommand):
    """Chain multiple commands into a single command

    The command chain class is used in cases where the execution of one command
    is required before the next can start but these commands should be treated
    as one for organizational purposes
    """
    # ................................
    def __init__(self, cmdList):
        """Constructor, combines the list of commands into a single command
        """
        _LmCommand.__init__(self)
        
        # Set inputs and outputs
        inSet = set.union(*[set(c.inputs) for c in cmdList])
        outSet = set.union(*[set(c.outputs) for c in cmdList])
            
        self.outputs = list(outSet)
        self.inputs = list(inSet.difference(outSet))

        # Assemble commands
        self._cmd = ' ; '.join([c.getCommand() for c in cmdList])
    
    # ................................
    def getCommand(self):
        """Get the aggregated command for each of the instances
        """
        return self._cmd
    
    
# .............................................................................
class ConcatenateMatricesCommand(_LmCommand):
    """Generate a command to concatenate a list of matrices

    This command will concatenate a list of matrices based on the specified
    axis
    """
    relDir = COMMON_SCRIPTS_DIR
    scriptName = 'concatenate_matrices.py'

    # ................................
    def __init__(self, matrices, axis, outMtxFilename, 
                             mashedPotatoFilename=None):
        """Construct the command object

        Args:
            matrices: A list of zero or more matrix filenames to concatenate
            axis: The axis to concatenate the matrices on
            outMtxFilename: The output location of the resulting matrix
            mashedPotatoFilename: (optional) If present, get the input matrix
                file names from this file instead 
        """
        _LmCommand.__init__(self)
        
        self.args = '{} {} {}'.format(outMtxFilename, axis, matrices)
        
        self.inputs.extend(matrices)
        self.outputs.append(outMtxFilename)
        if mashedPotatoFilename is not None:
            self.outputs.append(mashedPotatoFilename)
            self.opt_args = '--mashedPotato={}'.format(mashedPotatoFilename)
            
# .............................................................................
class ConvertLayerCommand(_LmCommand):
    """This command will convert a tiff to ascii
    """
    relDir = BACKEND_SCRIPTS_DIR
    scriptName = 'convert_single_layer.py'

    # ................................
    def __init__(self, origAsciiFilename, modifiedAsciiFilename):
        """Construct the command object

        Args:
            origAsciiFilename: The original ASCII file
            modifiedAsciiFilename: The modified ASCII file
        """
        _LmCommand.__init__(self)
        
        self.args = '{} {}'.format(origAsciiFilename, modifiedAsciiFilename)
        
        self.inputs.append(origAsciiFilename)
        self.outputs.append(modifiedAsciiFilename)

# .............................................................................
class CreateFMatrixCommand(_LmCommand):
    """This command will create a F-Matrix comparing observed and random data
    """
    relDir = BACKEND_SCRIPTS_DIR
    scriptName = 'create_f_matrix.py'

    # ................................
    def __init__(self, observed_filename, f_matrix_filename, random_matrices,
                 use_abs=False):
        """Constructor for command

        Args:
            observed_filename : The observed matrix
            f_matrix_filename : The location to write the output matrix
            random_matrices : One or more random matrices to compare to the
                observed
            use_abs : If True, use absolute value for comparisons
        """
        self.inputs.append(observed_filename)
        self.outputs.append(f_matrix_filename)
        if not isinstance(random_matrices, list):
            random_matrices = [random_matrices]
        self.inputs.extend(random_matrices)
        self.args = '{} {} {}'.format(
            observed_filename, f_matrix_filename, ' '.join(random_matrices))
        if use_abs:
            self.opt_args += ' -a'

# .............................................................................
class ModifyAsciiHeadersCommand(_LmCommand):
    """This command will reduce the number of decimal digits in ASCII headers
    """
    relDir = COMMON_SCRIPTS_DIR
    scriptName = 'modify_ascii_headers.py'

    # ................................
    def __init__(self, origAsciiFilename, modifiedAsciiFilename, digits=7):
        """Construct the command object

        Args:
            origAsciiFilename: The original ASCII file
            modifiedAsciiFilename: The modified ASCII file
            digits: The number of decimal digits to keep
        """
        _LmCommand.__init__(self)
        
        self.args = '{} {}'.format(origAsciiFilename, modifiedAsciiFilename)
        self.opt_args = '-d {}'.format(digits)
        
        self.inputs.append(origAsciiFilename)
        self.outputs.append(modifiedAsciiFilename)
            
# .............................................................................
class SystemCommand(_LmCommand):
    """This command will run a system command (not a Python script)
    """
    # ................................
    def __init__(self, script, args, inputs=None, outputs=None):
        """Construct the command object

        Args:
            script: The system script to run
            args: A list of arguments to pass to the script
            inputs: An optional list of input files needed by this script
            outputs: An optional list of output files generated by this script
        """
        _LmCommand.__init__(self)
        
        if inputs is not None:
            if isinstance(inputs, list):
                self.inputs.extend(inputs)
            else:
                self.inputs.append(inputs)
        
        if outputs is not None:
            if isinstance(outputs, list):
                self.outputs.extend(outputs)
            else:
                self.outputs.append(outputs)
            
        self.script = script
        self.args = args
        # System commands won't be able to handle empty inputs automatically,
        #     therefore, we tell the wrapper about them and it can skip the 
        #     command if these arguments are missing
        self.required_inputs = inputs
        
    # ................................
    def getCommand(self):
        """Get the concatenate matrices command
        """
        return '{} {}'.format(self.script, self.args)
