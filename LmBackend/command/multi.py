"""Module containing multi-species command wrappers
"""
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import MULTI_SPECIES_SCRIPTS_DIR


# .............................................................................
class CreateAncestralPamCommand(_LmCommand):
    """This command will create an ancestral PAM from a PAM and tree
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'create_ancestral_pam.py'

    # ................................
    def __init__(self, pam_file_name, tree_file_name, output_file_name):
        """Construct the command object

        Args:
            pam_file_name: The file path to the PAM to use
            tree_file_name: The file path to the tree to use
            output_file_name: The file path to store the output matrix
        """
        _LmCommand.__init__(self)
        self.inputs.extend([pam_file_name, tree_file_name])
        self.outputs.append(output_file_name)
        self.args = '{} {} {}'.format(
            pam_file_name, tree_file_name, output_file_name)


# .............................................................................
class EncodePhylogenyCommand(_LmCommand):
    """This command will encode a tree and PAM into a matrix
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'encode_phylogeny.py'

    # ................................
    def __init__(self, tree_file_name, pam_file_name, out_mtx_file_name):
        """Construct the command object

        Args:
            tree_file_name: The file location of the tree to use for encoding
            pam_file_name: The file location of the PAM to use for encoding
            out_mtx_file_name: The file location to write the encoded tree

        Todo:
            * Evaluate if we can remove mashed_potato_file_name
        """
        _LmCommand.__init__(self)

        self.inputs.extend([tree_file_name, pam_file_name])
        self.outputs.append(out_mtx_file_name)

        self.args = '{} {} {}'.format(
            tree_file_name, pam_file_name, out_mtx_file_name)


# .............................................................................
class MultiSpeciesRunCommand(_LmCommand):
    """This command performs an observed or randomized multi-species run
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'multi_species_run.py'

    # ................................
    def __init__(self, pam_filename, num_permutations, do_pam_stats, do_mcpa,
                 parallel=False, grim_filename=None, biogeo_filename=None,
                 phylo_filename=None, tree_filename=None,
                 diversity_stats_filename=None, site_stats_filename=None,
                 species_stats_filename=None, site_covariance_filename=None,
                 species_covariance_filename=None, mcpa_output_filename=None,
                 mcpa_f_matrix_filename=None, pam_success_filename=None):
        """Constructor for command object

        Args:
            pam_filename (:obj: `str`): The file location of the PAM or
                incidence matrix to be used for these computations
            num_permutations (:obj: `int`): The number of permutations to
                perform in this run.  Setting this to zero will perform an
                observed run rather than permuted runs.
            do_pam_stats (:obj: `bool`): A boolean value indicating if PAM
                stats should be calculated
            do_mcpa (:obj: `bool`): A boolean value indicating if MCPA should
                be performed
            parallel (:obj: `bool`, optional): A boolean value indicating if
                the parallel version of MCPA should be used
            grim_filename (:obj: `str`, optional): The file location of the
                GRIM or environment matrix for MCPA
            biogeo_filename (:obj: `str`, optional): The file location of the
                biogeographic hypotheses matrix for MCPA
            phylo_filename (:obj: `str`, optional): The file location of the
                encoded phylogenetic tree matrix to be used in MCPA
            tree_filename (:obj: `str`, optional): The file location of the
                tree to be used for PAM stats
            diversity_stats_filename (:obj: `str`, optional): The file
                location to store PAM diversity statistics
            site_stats_filename (:obj: `str`, optional): The file location to
                store PAM site statistics
            species_stats_filename (:obj: `str`, optional): The file location
                to store PAM species statistics
            site_covariance_filename (:obj: `str`, optional): The file
                location to store PAM site covariance matrix
            species_covariance_filename (:obj: `str`, optional): The file
                location to store PAM species covariance matrix
            mcpa_output_filename (:obj: `str`, optional): The file location to
                store MCPA observed outputs
            mcpa_f_matrix_filename (:obj: `str`, optional): The file location
                to store MCPA F-values
        """
        _LmCommand.__init__(self)
        self.opt_args = ''
        self.args = '{} {} {} {}'.format(
            pam_filename, num_permutations, int(do_pam_stats), int(do_mcpa))
        self.inputs.append(pam_filename)
        if parallel:
            self.opt_args += ' -p'
        # Inputs
        if grim_filename is not None:
            self.inputs.append(grim_filename)
            self.opt_args += ' -g {}'.format(grim_filename)
        if biogeo_filename is not None:
            self.inputs.append(biogeo_filename)
            self.opt_args += ' -b {}'.format(biogeo_filename)
        if phylo_filename is not None:
            self.inputs.append(phylo_filename)
            self.opt_args += ' -tm {}'.format(phylo_filename)
        if tree_filename is not None:
            self.inputs.append(tree_filename)
            self.opt_args += ' -t {}'.format(tree_filename)
        if pam_success_filename is not None:
            self.inputs.append(pam_success_filename)

        # Outputs
        if diversity_stats_filename is not None:
            self.outputs.append(diversity_stats_filename)
            self.opt_args += ' --diversity_stats_filename={}'.format(
                diversity_stats_filename)
        if site_stats_filename is not None:
            self.outputs.append(site_stats_filename)
            self.opt_args += ' --site_stats_filename={}'.format(
                site_stats_filename)
        if species_stats_filename is not None:
            self.outputs.append(species_stats_filename)
            self.opt_args += ' --species_stats_filename={}'.format(
                species_stats_filename)
        if site_covariance_filename is not None:
            self.outputs.append(site_covariance_filename)
            self.opt_args += ' --site_covariance_filename={}'.format(
                site_covariance_filename)
        if species_covariance_filename is not None:
            self.outputs.append(species_covariance_filename)
            self.opt_args += ' --species_covariance_filename={}'.format(
                species_covariance_filename)
        if mcpa_output_filename is not None:
            self.outputs.append(mcpa_output_filename)
            self.opt_args += ' --mcpa_output_matrix_filename={}'.format(
                mcpa_output_filename)
        if mcpa_f_matrix_filename is not None:
            self.outputs.append(mcpa_f_matrix_filename)
            self.opt_args += ' --mcpa_f_matrix_filename={}'.format(
                mcpa_f_matrix_filename)


# .............................................................................
class OccurrenceBucketeerCommand(_LmCommand):
    """This command will split a CSV into buckets based on the group field
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'occurrence_bucketeer.py'

    # ................................
    def __init__(self, out_base_name, group_position, in_file_name,
                 position=None, width=None, header_row=False):
        """Construct the command object

        Args:
            out_base_name: The base name to use for output files
            group_position: The field to use for grouping / bucketing
            in_file_name: A file location or list of file locations to use as
                input
            position: The position in the group field to use for bucketing
            width: The number of characters to use for bucketing
            header_row: Does the input file have a header row?
        """
        _LmCommand.__init__(self)

        if not isinstance(in_file_name, list):
            in_file_name = [in_file_name]

        self.inputs.extend(in_file_name)

        # Outputs are unknown unless you know the data
        # self.outputs.append()

        self.args = '{} {} {}'.format(
            out_base_name, group_position, ' '.join(in_file_name))

        if position is not None:
            self.opt_args += ' -pos {}'.format(position)

        if width is not None:
            self.opt_args += ' -num {}'.format(width)

        if header_row:
            self.opt_args += ' -header'


# .............................................................................
class OccurrenceSorterCommand(_LmCommand):
    """This command will sort a CSV file on a group field
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'occurrence_sorter.py'

    # ................................
    def __init__(self, in_file_name, out_file_name, group_position):
        """Construct the command object

        Args:
            in_file_name: The CSV input file to sort
            out_file_name: The file location of the output CSV
            group_position: The field position to use for sorting
        """
        _LmCommand.__init__(self)
        self.inputs.append(in_file_name)
        self.outputs.append(out_file_name)

        self.args = '{} {} {}'.format(
            out_file_name, group_position, in_file_name)


# .............................................................................
class OccurrenceSplitterCommand(_LmCommand):
    """This command will split a sorted CSV file on a group field
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'occurrence_splitter.py'

    # ................................
    def __init__(self, group_position, in_file_name, out_dir, prefix=None):
        """Construct the command object

        Args:
            group_position: The field to group on
            in_file_name: The input CSV file
            out_dir: A directory location to write the output files
            prefix: A file name prefix to use for the output files
        """
        _LmCommand.__init__(self)
        self.inputs.append(in_file_name)
        # output files are not deterministic from inputs, need to look at file
        # self.outputs.append()

        self.args = '{} {} {}'.format(group_position, in_file_name, out_dir)
        if prefix is not None:
            self.opt_args += ' -p {}'.format(prefix)


# .............................................................................
class SyncPamAndTreeCommand(_LmCommand):
    """This command synchronizes a PAM and Tree for MCPA computations
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'sync_pam_and_tree.py'

    # ................................
    def __init__(self, in_pam_file_name, out_pam_file_name, in_tree_file_name,
                 out_tree_file_name, metadata_file_name):
        """Construct the command object

        Args:
            in_pam_file_name: The file location of the PAM to prune
            out_pam_file_name: The file location to write the pruned PAM
            in_tree_file_name: The file location of the tree to prune
            out_tree_file_name: The file location to write the pruned tree
            metadata_file_name: The file location to write the summary metadata
        """
        _LmCommand.__init__(self)
        self.inputs.extend([in_pam_file_name, in_tree_file_name])
        self.outputs.extend(
            [out_pam_file_name, out_tree_file_name, metadata_file_name])

        self.args = '{} {} {} {} {}'.format(
            in_pam_file_name, out_pam_file_name, in_tree_file_name,
            out_tree_file_name, metadata_file_name)
