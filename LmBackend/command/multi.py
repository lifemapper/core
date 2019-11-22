"""Module containing multi-species command wrappers
"""
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import CMD_PYBIN, MULTI_SPECIES_SCRIPTS_DIR

# .............................................................................
class BuildShapegridCommand(_LmCommand):
    """This command will build a shapegrid

    Todo:
        * Document arguments
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'build_shapegrid.py'

    # ................................
    def __init__(self, shapegridFilename, minX, minY, maxX, maxY, cellSize, 
                     epsg, numSides, cutoutWKTFilename=None):
        """Construct the command object
        """
        _LmCommand.__init__(self)
        self.outputs.append(shapegridFilename)
        
        self.args = '{} {} {} {} {} {} {} {}'.format(
            shapegridFilename, minX, minY, maxX, maxY, cellSize, epsg,
            numSides)
        
        if cutoutWKTFilename is not None:
            self.inputs.append(cutoutWKTFilename)
            self.opt_args = ' --cutoutWktFn={}'.format(cutoutWKTFilename)

# .............................................................................
class CalculateStatsCommand(_LmCommand):
    """This command will calculate statistics for a PAM

    Todo:
        * Determine if we want to continue supporting this as a stand alone
            command since MultiSpeciesRunCommand provides the same
            functionality
        * Document arguments
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'calculate_pam_stats.py'

    # ................................
    def __init__(self, pamFilename, sitesFilename, speciesFilename, 
                 diversityFilename, treeFilename=None, schluter=False,
                 speciesCovarianceFilename=None, sitesCovarianceFilename=None):
        """Construct the command object
        """
        _LmCommand.__init__(self)
        self.inputs.append(pamFilename)
        self.outputs.extend([sitesFilename, speciesFilename, diversityFilename])
        
        self.args = '{} {} {} {}'.format(
            pamFilename, sitesFilename, speciesFilename, diversityFilename)

        if treeFilename is not None:
            self.inputs.append(treeFilename)
            self.opt_args += ' -t {}'.format(treeFilename)
            
        if schluter:
            self.opt_args += ' --schluter'
            
        if speciesCovarianceFilename is not None:
            self.outputs.append(speciesCovarianceFilename)
            self.opt_args += ' --speciesCovFn={}'.format(
                speciesCovarianceFilename)
        
        if sitesCovarianceFilename is not None:
            self.outputs.append(sitesCovarianceFilename)
            self.opt_args += ' --sitCovFn={}'.format(sitesCovarianceFilename)

# .............................................................................
class CreateAncestralPamCommand(_LmCommand):
    """This command will create an ancestral PAM from a PAM and tree
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'create_ancestral_pam.py'

    # ................................
    def __init__(self, pamFilename, treeFilename, outputFilename):
        """Construct the command object

        Args:
            pamFilename: The file path to the PAM to use
            treeFilename: The file path to the tree to use
            outputFilename: The file path to store the output matrix
        """
        _LmCommand.__init__(self)
        self.inputs.extend([pamFilename, treeFilename])
        self.outputs.append(outputFilename)
        self.args = '{} {} {}'.format(
            pamFilename, treeFilename, outputFilename)

# .............................................................................
class EncodeHypothesesCommand(_LmCommand):
    """This command will encode biogeographic hypotheses with a shapegrid
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'encode_hypotheses.py'

    # ................................
    def __init__(self, shapegridFilename, layerFilenames, outputFilename,
                 eventField=None):
        """Construct the command object

        Args:
            shapegridFilename: The file location of the shapegrid to use for
                encoding
            layerFilenames: File location(s) of layers to encode
            outputFilename: The file location to store the encoded matrix
            eventField: Use this field in the layers to determine events
        """
        _LmCommand.__init__(self)
        
        self.sgFn = shapegridFilename
        if not isinstance(layerFilenames, list):
            layerFilenames = [layerFilenames]

        self.args = '{} {} {}'.format(
            shapegridFilename, outputFilename, ' '.join(layerFilenames))
        if eventField is not None:
            self.opt_args += ' -e {}'.format(eventField)

        self.inputs.append(shapegridFilename)
        self.inputs.extend(layerFilenames)
        
        self.outputs.append(outputFilename)

# .............................................................................
class EncodePhylogenyCommand(_LmCommand):
    """This command will encode a tree and PAM into a matrix
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'encode_phylogeny.py'

    # ................................
    def __init__(self, treeFilename, pamFilename, outMtxFilename,
                 mashedPotato=None):
        """Construct the command object

        Args:
            treeFilename: The file location of the tree to use for encoding
            pamFilename: The file location of the PAM to use for encoding
            outMtxFilename: The file location to write the encoded tree
            mashedPotato: The file location of the mashed potato

        Todo:
            * Evaluate if we can remove mashedPotato
        """
        _LmCommand.__init__(self)
        
        self.inputs.extend([treeFilename, pamFilename])
        self.outputs.append(outMtxFilename)
        
        self.args = '{} {} {}'.format(
            treeFilename, pamFilename, outMtxFilename)
        if mashedPotato is not None:
            self.opt_args += ' -m {}'.format(mashedPotato)
            self.inputs.append(mashedPotato)
        
# # .............................................................................
# class McpaCorrectPValuesCommand(_LmCommand):
#     """This command will correct the P-values generated by MCPA
#     """
#     relDir = MULTI_SPECIES_SCRIPTS_DIR
#     scriptName = 'mcpa_correct_pvalues.py'
# 
#     # ................................
#     def __init__(self, observedFilename, outPvaluesFilename, bhValuesFilename,
#                  fValueFilenames):
#         """Construct the command object
# 
#         Args:
#             observedFilename: The file location of the observed values to test
#             outPvaluesFilename: The file location to store the P-values
#             bhValuesFilename: The file location to store the Benjamini-Hochberg
#                 ouptut matrix used for determining significant results
#             fValueFilenames: The file location or list of file locations of
#                 F-values or a stack of F-values to correct
#         """
#         _LmCommand.__init__(self)
#         
#         if not isinstance(fValueFilenames, list):
#             fValueFilenames = [fValueFilenames]
# 
#         self.inputs.append(observedFilename)
#         self.inputs.extend(fValueFilenames)
#         self.outputs.extend([outPvaluesFilename, bhValuesFilename])
#         
#         self.args = '{} {} {} {}'.format(
#             observedFilename, outPvaluesFilename, bhValuesFilename, 
#             ' '.join(fValueFilenames))

# .............................................................................
class McpaRunCommand(_LmCommand):
    """This command will perform one run of MCPA

    Note:
        This will likely be obsolete in favor of MultiSpeciesRun
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'mcpa_run.py'

    # ................................
    def __init__(self, pam_filename, tree_mtx_filename, grim_filename,
                 bg_filename, obs_filename=None, f_mtx_filename=None,
                 randomize=False, num_permutations=1):
        """Construct the command object

        Args:
            pam_filename: The file location of the PAM Matrix to use
            tree_mtx_filename: The file location of the encoded phylogenetic
                tree Matrix to use
            grim_filename: The file location of the grim Matrix
            bg_filename: The file location of the encoded biogeographic
                hypotheses Matrix to use
            obs_filename: If provided, write the observed semi-partial
                correlation values Matrix here (only for observed runs).
            f_mtx_filename: If provided, write the F-values Matrix, or stack,
                to this location
            randomize: If True, perform a randomized run
            num_permutations: If randomizing, perform this many runs in this
                call
        """
        _LmCommand.__init__(self)
        self.args = ' {} {} {} {}'.format(
            pam_filename, tree_mtx_filename, grim_filename, bg_filename)
        self.inputs.extend(
            [pam_filename, tree_mtx_filename, grim_filename, bg_filename])
        
        if obs_filename is not None:
            self.outputs.append(obs_filename)
            self.opt_args += ' -co {}'.format(obs_filename)
     
        if f_mtx_filename is not None:
            self.outputs.append(f_mtx_filename)
            self.opt_args += ' -fo {}'.format(f_mtx_filename)
     
        if randomize:
            self.opt_args += ' --randomize -n {}'.format(num_permutations)

# .............................................................................
class MultiSpeciesRunCommand(_LmCommand):
    """This command performs an observed or randomized multi-species run
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'multi_species_run.py'
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
            diversity_stats_filename (:obj: `str`, optional): The file location
                to store PAM diversity statistics
            site_stats_filename (:obj: `str`, optional): The file location to
                store PAM site statistics
            species_stats_filename (:obj: `str`, optional): The file location
                to store PAM species statistics
            site_covariance_filename (:obj: `str`, optional): The file location
                to store PAM site covariance matrix
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
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'occurrence_bucketeer.py'

    # ................................
    def __init__(self, outBasename, groupPosition, inFilename, position=None,
                 width=None, headerRow=False):
        """Construct the command object

        Args:
            outBasename: The base name to use for output files
            groupPosition: The field to use for grouping / bucketing
            inFilename: A file location or list of file locations to use as
                input
            position: The position in the group field to use for bucketing
            width: The number of characters to use for bucketing
            headerRow: Does the input file have a header row?
        """
        _LmCommand.__init__(self)
        
        if not isinstance(inFilename, list):
            inFilename = [inFilename]
        
        self.inputs.extend(inFilename)
        
        # Outputs are unknown unless you know the data
        #self.outputs.append()
        
        self.args = '{} {} {}'.format(
            outBasename, groupPosition, ' '.join(inFilename))
        
        if position is not None:
            self.opt_args += ' -pos {}'.format(position)
        
        if width is not None:
            self.opt_args += ' -num {}'.format(width)
        
        if headerRow:
            self.opt_args += ' -header'

# .............................................................................
class OccurrenceSorterCommand(_LmCommand):
    """This command will sort a CSV file on a group field
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'occurrence_sorter.py'

    # ................................
    def __init__(self, inFilename, outFilename, groupPosition):
        """Construct the command object

        Args:
            inFilename: The CSV input file to sort
            outFilename: The file location of the output CSV
            groupPosition: The field position to use for sorting
        """
        _LmCommand.__init__(self)
        self.inputs.append(inFilename)
        self.outputs.append(outFilename)
        
        self.args = '{} {} {}'.format(outFilename, groupPosition, inFilename)

# .............................................................................
class OccurrenceSplitterCommand(_LmCommand):
    """This command will split a sorted CSV file on a group field
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'occurrence_splitter.py'

    # ................................
    def __init__(self, groupPosition, inFilename, outDir, prefix=None):
        """Construct the command object

        Args:
            groupPosition: The field to group on
            inFilename: The input CSV file
            outDir: A directory location to write the output files
            prefix: A filename prefix to use for the output files
        """
        _LmCommand.__init__(self)
        self.inputs.append(inFilename)
        # output files are not deterministic from inputs, need to look at file
        #self.outputs.append()
        
        self.args = '{} {} {}'.format(groupPosition, inFilename, outDir)
        if prefix is not None:
            self.opt_args += ' -p {}'.format(prefix)
        
# .............................................................................
class RandomizeGradyCommand(_LmCommand):
    """This command will randomize a PAM using CJ's method
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'grady_randomize.py'

    # ................................
    def __init__(self, pamFilename, randPamFilename):
        """Construct the command object

        Args:
            pamFilename: The file location of the PAM to randomize
            randPamFilename: The file location to write the randomized PAM
        """
        _LmCommand.__init__(self)
        self.inputs.append(pamFilename)
        self.outputs.append(randPamFilename)
        
        self.args = '{} {}'.format(pamFilename, randPamFilename)

# .............................................................................
class RandomizeSwapCommand(_LmCommand):
    """This command will randomize a PAM using the swap method
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'swap_randomize.py'

    # ................................
    def __init__(self, pamFilename, numSwaps, outFilename):
        """Construct the command object

        Args:
            pamFilename: The file location of the PAM
            numSwaps: The number of successful swaps to perform
            outFilename: The file location to write the randomized PAM
        """
        _LmCommand.__init__(self)
        self.inputs.append(pamFilename)
        self.outputs.append(outFilename)
        
        self.args = '{} {} {}'.format(pamFilename, numSwaps, outFilename)

# .............................................................................
class SyncPamAndTreeCommand(_LmCommand):
    """This command synchronizes a PAM and Tree for MCPA computations
    """
    relDir = MULTI_SPECIES_SCRIPTS_DIR
    scriptName = 'sync_pam_and_tree.py'
    
    # ................................
    def __init__(self, inPamFilename, outPamFilename, inTreeFilename,
                 outTreeFilename, metadataFilename):
        """Construct the command object

        Args:
            inPamFilename: The file location of the PAM to prune
            outPamFilename: The file location to write the pruned PAM
            inTreeFilename: The file location of the tree to prune
            outTreeFilename: The file location to write the pruned tree
            metadataFilename: The file location to write the summary metadata
        """
        _LmCommand.__init__(self)
        self.inputs.extend([inPamFilename, inTreeFilename])
        self.outputs.extend([outPamFilename, outTreeFilename, metadataFilename])
        
        self.args = '{} {} {} {} {}'.format(
            inPamFilename, outPamFilename, inTreeFilename, outTreeFilename,
            metadataFilename)
