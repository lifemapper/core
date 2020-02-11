"""Common commands
"""
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import (
    BACKEND_SCRIPTS_DIR, COMMON_SCRIPTS_DIR)


# .............................................................................
class IdigbioQueryCommand(_LmCommand):
    """Command class for querying iDigBio.
    """
    relative_directory = COMMON_SCRIPTS_DIR
    script_name = 'get_idig_data.py'

    # ................................
    def __init__(self, taxon_id_file, point_output_file, meta_output_file,
                 success_file, missing_id_file=None):
        """Construct the command object
        """
        _LmCommand.__init__(self)

        # no need to add taxon_id_file to inputs
        self.outputs.extend(
            [point_output_file, meta_output_file, success_file])
        self.args = '{} {} {} {}'.format(
            taxon_id_file, point_output_file, meta_output_file, success_file)
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
    def __init__(self, cmd_list):
        """Constructor, combines the list of commands into a single command
        """
        _LmCommand.__init__(self)

        # Set inputs and outputs
        in_set = set.union(*[set(c.inputs) for c in cmd_list])
        out_set = set.union(*[set(c.outputs) for c in cmd_list])

        self.outputs = list(out_set)
        self.inputs = list(in_set.difference(out_set))

        # Assemble commands
        self._cmd = ' ; '.join([c.get_command() for c in cmd_list])

    # ................................
    def get_command(self):
        """Get the aggregated command for each of the instances
        """
        return self._cmd


# .............................................................................
class ConcatenateMatricesCommand(_LmCommand):
    """Generate a command to concatenate a list of matrices

    This command will concatenate a list of matrices based on the specified
    axis
    """
    relative_directory = COMMON_SCRIPTS_DIR
    script_name = 'concatenate_matrices.py'

    # ................................
    def __init__(self, matrices, axis, out_mtx_file_name):
        """Construct the command object

        Args:
            matrices: A list of zero or more matrix filenames to concatenate
            axis: The axis to concatenate the matrices on
            out_mtx_file_name: The output location of the resulting matrix
        """
        _LmCommand.__init__(self)

        self.args = '{} {} {}'.format(
            out_mtx_file_name, axis, ' '.join(matrices))

        self.inputs.extend(matrices)
        self.outputs.append(out_mtx_file_name)


# .............................................................................
class CreateSignificanceMatrixCommand(_LmCommand):
    """This command will create a significance matrix

    The significance matrix is composed of the observed data, p-values
    determined by comparing random values, and a significance layer indicating
    which values are significant after p-value correction.
    """
    relative_directory = BACKEND_SCRIPTS_DIR
    script_name = 'create_significance_matrix.py'

    # ................................
    def __init__(self, observed_filename, out_matrix_filename, random_matrices,
                 use_abs=False, fdr=None, test_matrix=None):
        """Constructor for command

        Args:
            observed_filename : The observed matrix
            out_matrix_filename : The location to write the output matrix
            random_matrices : One or more random matrices to compare to the
                observed
            use_abs : If True, use absolute value for comparisons
            fdr : The false discovery rate, or alpha, value to use when
                determining significance
            test_matrix: An expected values matrix.  If provided, use the
                Matrix at this file location for comparisons instead of the
                observed
        """
        _LmCommand.__init__(self)
        self.inputs.append(observed_filename)
        self.outputs.append(out_matrix_filename)
        if not isinstance(random_matrices, list):
            random_matrices = [random_matrices]
        self.inputs.extend(random_matrices)
        self.args = '{} {} {}'.format(
            observed_filename, out_matrix_filename, ' '.join(random_matrices))
        if use_abs:
            self.opt_args += ' -a'
        if fdr is not None:
            self.opt_args += ' --fdr {}'.format(fdr)
        if test_matrix is not None:
            self.opt_args += ' -t {}'.format(test_matrix)
            self.inputs.append(test_matrix)


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

    # ................................
    def get_command(self):
        """Get the concatenate matrices command
        """
        return '{} {}'.format(self.script, self.args)
