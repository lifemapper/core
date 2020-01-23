"""Module containing multi-species command wrappers
"""
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import MULTI_SPECIES_SCRIPTS_DIR


# .............................................................................
class BuildShapegridCommand(_LmCommand):
    """This command will build a shapegrid

    Todo:
        * Document arguments
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'build_shapegrid.py'

    # ................................
    def __init__(self, shapegrid_file_name, min_x, min_y, max_x, max_y,
                 cell_size, epsg, num_sides, cutout_wkt_file_name=None):
        """Construct the command object
        """
        _LmCommand.__init__(self)
        self.outputs.append(shapegrid_file_name)

        self.args = '{} {} {} {} {} {} {} {}'.format(
            shapegrid_file_name, min_x, min_y, max_x, max_y, cell_size, epsg,
            num_sides)

        if cutout_wkt_file_name is not None:
            self.inputs.append(cutout_wkt_file_name)
            self.opt_args = ' --cutoutWktFn={}'.format(cutout_wkt_file_name)


# .............................................................................
class CalculateStatsCommand(_LmCommand):
    """This command will calculate statistics for a PAM

    Todo:
        * Determine if we want to continue supporting this as a stand alone
            command since MultiSpeciesRunCommand provides the same
            functionality
        * Document arguments
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'calculate_pam_stats.py'

    # ................................
    def __init__(self, pam_file_name, sites_file_name, species_file_name,
                 diversity_file_name, tree_file_name=None, schluter=False,
                 species_cov_file_name=None, sitess_cov_file_name=None):
        """Construct the command object
        """
        _LmCommand.__init__(self)
        self.inputs.append(pam_file_name)
        self.outputs.extend(
            [sites_file_name, species_file_name, diversity_file_name])

        self.args = '{} {} {} {}'.format(
            pam_file_name, sites_file_name, species_file_name,
            diversity_file_name)

        if tree_file_name is not None:
            self.inputs.append(tree_file_name)
            self.opt_args += ' -t {}'.format(tree_file_name)

        if schluter:
            self.opt_args += ' --schluter'

        if species_cov_file_name is not None:
            self.outputs.append(species_cov_file_name)
            self.opt_args += ' --speciesCovFn={}'.format(
                species_cov_file_name)

        if sitess_cov_file_name is not None:
            self.outputs.append(sitess_cov_file_name)
            self.opt_args += ' --sitCovFn={}'.format(sitess_cov_file_name)


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
class EncodeHypothesesCommand(_LmCommand):
    """This command will encode biogeographic hypotheses with a shapegrid
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'encode_hypotheses.py'

    # ................................
    def __init__(self, shapegrid_file_name, layer_file_names, output_file_name,
                 event_field=None):
        """Construct the command object

        Args:
            shapegrid_file_name: The file location of the shapegrid to use for
                encoding
            layer_file_names: File location(s) of layers to encode
            output_file_name: The file location to store the encoded matrix
            event_field: Use this field in the layers to determine events
        """
        _LmCommand.__init__(self)

        if not isinstance(layer_file_names, list):
            layer_file_names = [layer_file_names]

        self.args = '{} {} {}'.format(
            shapegrid_file_name, output_file_name, ' '.join(layer_file_names))
        if event_field is not None:
            self.opt_args += ' -e {}'.format(event_field)

        self.inputs.append(shapegrid_file_name)
        self.inputs.extend(layer_file_names)

        self.outputs.append(output_file_name)


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
class McpaRunCommand(_LmCommand):
    """This command will perform one run of MCPA

    Note:
        This will likely be obsolete in favor of MultiSpeciesRun
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'mcpa_run.py'

    # ................................
    def __init__(self, pam_file_name, tree_mtx_file_name, grim_file_name,
                 bg_file_name, obs_file_name=None, f_mtx_file_name=None,
                 randomize=False, num_permutations=1):
        """Construct the command object

        Args:
            pam_file_name: The file location of the PAM Matrix to use
            tree_mtx_file_name: The file location of the encoded phylogenetic
                tree Matrix to use
            grim_file_name: The file location of the grim Matrix
            bg_file_name: The file location of the encoded biogeographic
                hypotheses Matrix to use
            obs_file_name: If provided, write the observed semi-partial
                correlation values Matrix here (only for observed runs).
            f_mtx_file_name: If provided, write the F-values Matrix, or stack,
                to this location
            randomize: If True, perform a randomized run
            num_permutations: If randomizing, perform this many runs in this
                call
        """
        _LmCommand.__init__(self)
        self.args = ' {} {} {} {}'.format(
            pam_file_name, tree_mtx_file_name, grim_file_name, bg_file_name)
        self.inputs.extend(
            [pam_file_name, tree_mtx_file_name, grim_file_name, bg_file_name])

        if obs_file_name is not None:
            self.outputs.append(obs_file_name)
            self.opt_args += ' -co {}'.format(obs_file_name)

        if f_mtx_file_name is not None:
            self.outputs.append(f_mtx_file_name)
            self.opt_args += ' -fo {}'.format(f_mtx_file_name)

        if randomize:
            self.opt_args += ' --randomize -n {}'.format(num_permutations)


# .............................................................................
class MultiSpeciesRunCommand(_LmCommand):
    """This command performs an observed or randomized multi-species run
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'multi_species_run.py'

    # ................................
    def __init__(self, pam_file_name, num_permutations, do_pam_stats, do_mcpa,
                 parallel=False, grim_file_name=None, biogeo_file_name=None,
                 phylo_file_name=None, tree_file_name=None,
                 diversity_stats_file_name=None, site_stats_file_name=None,
                 species_stats_file_name=None, site_covariance_file_name=None,
                 species_covariance_file_name=None, mcpa_output_file_name=None,
                 mcpa_f_matrix_file_name=None, pam_success_file_name=None):
        """Constructor for command object

        Args:
            pam_file_name (:obj: `str`): The file location of the PAM or
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
            grim_file_name (:obj: `str`, optional): The file location of the
                GRIM or environment matrix for MCPA
            biogeo_file_name (:obj: `str`, optional): The file location of the
                biogeographic hypotheses matrix for MCPA
            phylo_file_name (:obj: `str`, optional): The file location of the
                encoded phylogenetic tree matrix to be used in MCPA
            tree_file_name (:obj: `str`, optional): The file location of the
                tree to be used for PAM stats
            diversity_stats_file_name (:obj: `str`, optional): The file
                location to store PAM diversity statistics
            site_stats_file_name (:obj: `str`, optional): The file location to
                store PAM site statistics
            species_stats_file_name (:obj: `str`, optional): The file location
                to store PAM species statistics
            site_covariance_file_name (:obj: `str`, optional): The file
                location to store PAM site covariance matrix
            species_covariance_file_name (:obj: `str`, optional): The file
                location to store PAM species covariance matrix
            mcpa_output_file_name (:obj: `str`, optional): The file location to
                store MCPA observed outputs
            mcpa_f_matrix_file_name (:obj: `str`, optional): The file location
                to store MCPA F-values
        """
        _LmCommand.__init__(self)
        self.opt_args = ''
        self.args = '{} {} {} {}'.format(
            pam_file_name, num_permutations, int(do_pam_stats), int(do_mcpa))
        self.inputs.append(pam_file_name)
        if parallel:
            self.opt_args += ' -p'
        # Inputs
        if grim_file_name is not None:
            self.inputs.append(grim_file_name)
            self.opt_args += ' -g {}'.format(grim_file_name)
        if biogeo_file_name is not None:
            self.inputs.append(biogeo_file_name)
            self.opt_args += ' -b {}'.format(biogeo_file_name)
        if phylo_file_name is not None:
            self.inputs.append(phylo_file_name)
            self.opt_args += ' -tm {}'.format(phylo_file_name)
        if tree_file_name is not None:
            self.inputs.append(tree_file_name)
            self.opt_args += ' -t {}'.format(tree_file_name)
        if pam_success_file_name is not None:
            self.inputs.append(pam_success_file_name)

        # Outputs
        if diversity_stats_file_name is not None:
            self.outputs.append(diversity_stats_file_name)
            self.opt_args += ' --diversity_stats_file_name={}'.format(
                diversity_stats_file_name)
        if site_stats_file_name is not None:
            self.outputs.append(site_stats_file_name)
            self.opt_args += ' --site_stats_file_name={}'.format(
                site_stats_file_name)
        if species_stats_file_name is not None:
            self.outputs.append(species_stats_file_name)
            self.opt_args += ' --species_stats_file_name={}'.format(
                species_stats_file_name)
        if site_covariance_file_name is not None:
            self.outputs.append(site_covariance_file_name)
            self.opt_args += ' --site_covariance_file_name={}'.format(
                site_covariance_file_name)
        if species_covariance_file_name is not None:
            self.outputs.append(species_covariance_file_name)
            self.opt_args += ' --species_covariance_file_name={}'.format(
                species_covariance_file_name)
        if mcpa_output_file_name is not None:
            self.outputs.append(mcpa_output_file_name)
            self.opt_args += ' --mcpa_output_matrix_file_name={}'.format(
                mcpa_output_file_name)
        if mcpa_f_matrix_file_name is not None:
            self.outputs.append(mcpa_f_matrix_file_name)
            self.opt_args += ' --mcpa_f_matrix_file_name={}'.format(
                mcpa_f_matrix_file_name)


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
class RandomizeGradyCommand(_LmCommand):
    """This command will randomize a PAM using CJ's method
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'grady_randomize.py'

    # ................................
    def __init__(self, pam_file_name, rand_pam_file_name):
        """Construct the command object

        Args:
            pam_file_name: The file location of the PAM to randomize
            rand_pam_file_name: The file location to write the randomized PAM
        """
        _LmCommand.__init__(self)
        self.inputs.append(pam_file_name)
        self.outputs.append(rand_pam_file_name)

        self.args = '{} {}'.format(pam_file_name, rand_pam_file_name)


# .............................................................................
class RandomizeSwapCommand(_LmCommand):
    """This command will randomize a PAM using the swap method
    """
    relative_directory = MULTI_SPECIES_SCRIPTS_DIR
    script_name = 'swap_randomize.py'

    # ................................
    def __init__(self, pam_file_name, num_swaps, out_file_name):
        """Construct the command object

        Args:
            pam_file_name: The file location of the PAM
            num_swaps: The number of successful swaps to perform
            out_file_name: The file location to write the randomized PAM
        """
        _LmCommand.__init__(self)
        self.inputs.append(pam_file_name)
        self.outputs.append(out_file_name)

        self.args = '{} {} {}'.format(pam_file_name, num_swaps, out_file_name)


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
