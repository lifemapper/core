"""Tools for generating Makeflow rules for multi-species analyses

Todo:
    Constants for groups
"""
import argparse
import mx.DateTime
import os

from LmBackend.command.common import CreateSignificanceMatrixCommand
from LmBackend.command.multi import (
    CreateAncestralPamCommand, EncodePhylogenyCommand, MultiSpeciesRunCommand,
    SyncPamAndTreeCommand)
from LmBackend.command.server import (
    AssemblePamFromSolrQueryCommand, SquidIncCommand, StockpileCommand)
from LmBackend.common.lmobj import LMObject

from LmCommon.common.lmconstants import (
    JobStatus, LMFormat, MatrixType, ProcessType)

from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.processchain import MFChain


DEFAULT_NUM_PERMUTATIONS = 1000
DEFAULT_RANDOM_GROUP_SIZE = 10

# .............................................................................
class BoomCollate(LMObject):
    """Class to manage multi-species calculation initiation
    """
    # ................................
    def __init__(self, gridset, dependencies=None, do_pam_stats=True,
                 do_mcpa=False, num_permutations=DEFAULT_NUM_PERMUTATIONS,
                 random_group_size=DEFAULT_RANDOM_GROUP_SIZE, work_dir=None,
                 log=None):
        """Constructor for collate

        Args:
            gridset (:obj: `Gridset`): A gridset object for which to generate
                multi-species computations
            dependencies (:obj: `list`): If provided, set these as dependencies
                before the assembly rules may begin
            do_pam_stats (:obj: `bool`): Should PAM stats be performed
            do_mcpa (:obj: `bool`): Should MCPA be performed
            num_permutations (:obj: `int`): The number of permutations to
                perform for the analyses
            random_group_size (:obj: `int`): The number of permutations to
                perform in each group of computations
        """
        if log is None:
            self.log = ConsoleLogger()
        else:
            self.log = log
        self._scribe = BorgScribe(self.log)
        self.gridset = gridset
        self.user_id = gridset.getUserId()
        if work_dir is not None:
            self.workspace_dir = os.path.join(
                work_dir, 'gs_{}'.format(gridset.getId()))
        else:
            self.workspace_dir = 'gs_{}'.format(gridset.getId())
        
        if dependencies is None:
            self.dependencies = []
        elif isinstance(dependencies, list):
            self.dependencies = dependencies
        else:
            self.dependencies = [dependencies]
            
        # Todo: Check if required inputs are present
        self.do_mcpa = do_mcpa
        self.do_pam_stats = do_pam_stats
        self.num_permutations = num_permutations
        self.random_group_size = random_group_size
        self.do_parallel = False
        self.fdr = 0.05
        
    # ................................
    def _create_filename(self, pam_id, *parts):
        """Create a file path

        Args:
            pam_id (:obj: `int`): The database identifier for a PAM
            parts (:obj: *`list`): Additional path parts
        """
        path_parts = [self.workspace_dir, 'pam_{}'.format(pam_id)]
        path_parts.extend(list(parts))
        return os.path.join(*path_parts)
        
    # ................................
    def _get_aggregation_rules_for_pam(
            self, pam_id, obs_pam_stats_filenames, obs_mcpa_filenames,
            rand_pam_stats_filenames, rand_mcpa_filenames):
        """Get the aggregation and summary rules for the various analyses
        """
        aggregation_rules = []
        sig_pam_stats_filenames = []
        sig_mcpa_stats_filenames = []
        for i in range(len(obs_pam_stats_filenames)):
            # TODO: Need a better way to name this file
            out_mtx_filename = obs_pam_stats_filenames[i].replace(
                LMFormat.MATRIX.ext, '_sig{}'.format(LMFormat.MATRIX.ext))
            rand_fns = [fn[i] for fn in rand_pam_stats_filenames]
            aggregation_rules.append(
                CreateSignificanceMatrixCommand(
                    obs_pam_stats_filenames[i], out_mtx_filename, rand_fns,
                    use_abs=True, fdr=self.fdr).getMakeflowRule())
            sig_pam_stats_filenames.append(out_mtx_filename)

        if len(obs_mcpa_filenames) > 0:
            # Create rule
            out_mtx_filename = self._create_filename(
                pam_id, 'mcpa_out{}'.format(LMFormat.MATRIX.ext))
            aggregation_rules.append(
                CreateSignificanceMatrixCommand(
                    obs_mcpa_filenames[0], out_mtx_filename,
                    rand_mcpa_filenames, use_abs=True, fdr=self.fdr,
                    test_matrix=obs_mcpa_filenames[1]).getMakeflowRule())
            sig_mcpa_stats_filenames.append(out_mtx_filename)
        return (sig_pam_stats_filenames, sig_mcpa_stats_filenames,
                aggregation_rules)

    # ................................
    def _get_analysis_rules_for_pam(
            self, pam_id, pam_filename, grim_filename=None,
            biogeo_filename=None, phylo_filename=None, tree_filename=None):
        """Get the analysis rules for a PAM
        """
        pam_analysis_rules = []
        # Observed rules
        (obs_pam_stats_filenames, obs_mcpa_filenames, obs_rules
         ) = self._get_multispecies_run_rules_for_pam(
             pam_id, 'obs', pam_filename, 0, grim_filename=grim_filename,
             biogeo_filename=biogeo_filename, phylo_filename=phylo_filename,
             tree_filename=tree_filename)
        pam_analysis_rules.extend(obs_rules)
        
        # Ancestral PAM
        if tree_filename is not None:
            anc_pam_filename = self._create_filename(
                pam_id, 'anc_pam{}'.format(LMFormat.MATRIX.ext))
            pam_analysis_rules.append(
                CreateAncestralPamCommand(
                    pam_filename, tree_filename, anc_pam_filename
                    ).getMakeflowRule())
        else:
            anc_pam_filename = None

        # Randomized
        rand_pam_stats_filenames = []
        rand_mcpa_stats_filenames = []
        for i, v in enumerate(
            range(0, self.num_permutations, self.random_group_size)):
            
            (pam_stats_filenames, mcpa_filenames, rand_rules
             ) = self._get_multispecies_run_rules_for_pam(
                 pam_id, 'rand{}'.format(i), pam_filename,
                 min(self.random_group_size, self.num_permutations - v),
                 grim_filename=grim_filename, biogeo_filename=biogeo_filename,
                 phylo_filename=phylo_filename, tree_filename=tree_filename)
            rand_pam_stats_filenames.append(pam_stats_filenames)
            rand_mcpa_stats_filenames.append(mcpa_filenames)
            pam_analysis_rules.extend(rand_rules)
        
        # Aggregates
        (sig_pam_stats_filenames, sig_mcpa_filenames, agg_rules
         ) = self._get_aggregation_rules_for_pam(pam_id,
             obs_pam_stats_filenames, obs_mcpa_filenames,
             rand_pam_stats_filenames, rand_mcpa_stats_filenames)
        # Add aggregation rules
        pam_analysis_rules.extend(agg_rules)
        
        return (anc_pam_filename, sig_pam_stats_filenames, sig_mcpa_filenames,
                pam_analysis_rules)

    # ................................
    def _get_mcpa_tree_encode_rules_for_pam(self, pam):
        """Get MCPA tree encoding rules for PAM

        Args:
            pam (:obj: `Matrix`): The PAM to use for MCPA
        """
        pam_id = pam.getId()
        mcpa_tree_encode_rules = []
        
        # File names
        encoded_tree_filename = self._create_filename(
            pam_id, 'encoded_tree{}'.format(LMFormat.MATRIX.ext))
        pruned_pam_filename = self._create_filename(
            pam_id, 'pruned_pam{}'.format(LMFormat.MATRIX.ext))
        pruned_tree_filename = self._create_filename(
            pam_id, 'pruned_tree{}'.format(LMFormat.NEXUS.ext))
        pruned_metadata_filename = self._create_filename(
            pam_id, 'pruned_metadata{}'.format(LMFormat.JSON.ext))
        # Synchronize PAM and Squidded tree
        mcpa_tree_encode_rules.append(
            SyncPamAndTreeCommand(
                pam.getDLocation(), pruned_pam_filename,
                self.squid_tree_filename, pruned_tree_filename,
                pruned_metadata_filename).getMakeflowRule())
        # Encode tree
        mcpa_tree_encode_rules.append(
            EncodePhylogenyCommand(
                pruned_tree_filename, pruned_pam_filename,
                encoded_tree_filename).getMakeflowRule())
        
        return (pruned_pam_filename, pruned_tree_filename,
                encoded_tree_filename, mcpa_tree_encode_rules)
    
    # ................................
    def _get_multispecies_run_rules_for_pam(
            self, pam_id, group_prefix, pam_filename, num_permutations,
            grim_filename=None, biogeo_filename=None, phylo_filename=None,
            tree_filename=None):
        """Get the rules for running a set of multi species analyses

        Return:
            * Tuple of tuple of pam stats files created, tuple of mcpa files
                created, list of rules
        """
        run_rules = []
        div_stats_filename = None
        species_stats_filename = None
        site_stats_filename = None
        mcpa_filename = None
        mcpa_f_vals_filename = None
        pam_stats_return = None
        mcpa_return = None
        
        if self.do_pam_stats:
            div_stats_filename = self._create_filename(
                pam_id, 'div_stats_{}{}'.format(
                    group_prefix, LMFormat.MATRIX.ext))
            species_stats_filename = self._create_filename(
                pam_id, 'sp_stats_{}{}'.format(
                    group_prefix, LMFormat.MATRIX.ext))
            site_stats_filename = self._create_filename(
                pam_id, 'site_stats_{}{}'.format(
                    group_prefix, LMFormat.MATRIX.ext))
            pam_stats_return = (
                div_stats_filename, species_stats_filename,
                site_stats_filename)
        if self.do_mcpa:
            mcpa_filename = self._create_filename(
                pam_id, 'mcpa_out_{}{}'.format(
                    group_prefix, LMFormat.MATRIX.ext))
            mcpa_f_vals_filename = self._create_filename(
                pam_id, 'mcpa_f_{}{}'.format(
                    group_prefix, LMFormat.MATRIX.ext))
            if num_permutations == 0:
                mcpa_return = (mcpa_filename, mcpa_f_vals_filename)
            else:
                mcpa_return = mcpa_f_vals_filename
        run_rules.append(
            MultiSpeciesRunCommand(
                pam_filename, num_permutations, self.do_pam_stats,
                self.do_mcpa, parallel=self.do_parallel,
                grim_filename=grim_filename, biogeo_filename=biogeo_filename,
                phylo_filename=phylo_filename, tree_filename=tree_filename,
                diversity_stats_filename=div_stats_filename,
                site_stats_filename=site_stats_filename,
                species_stats_filename=species_stats_filename,
                #site_covariance_filename=None,
                #species_covariance_filename=None,
                mcpa_output_filename=mcpa_filename,
                mcpa_f_matrix_filename=mcpa_f_vals_filename).getMakeflowRule())
        return pam_stats_return, mcpa_return, run_rules

    # ................................
    def _get_or_insert_matrix(self, mtx_type, process_type, gcm_code,
                              altpred_code, date_code):
        """Attempt to find a matching matrix or create a new one

        Args:
            mtx_type (:obj: `int`): A MatrixType constant
            process_type (:obj: `int`): A ProcessType constant
            gcm_code (:obj: `str`): A GCM code for this LMMatrix
            altpred_code (:obj: `str`): An alternate prediction code to use for
                this LMMatrix
            date_code (:obj: `str`): A date code to use for this LMMatrix
        """
        new_mtx = LMMatrix(
            None, matrixType=mtx_type, processType=process_type,
            gcmCode=gcm_code, altpredCode=altpred_code, dateCode=date_code,
            userId=self.user_id, gridset=self.gridset)
        mtx = self._scribe.findOrInsertMatrix(new_mtx)
        mtx.updateStatus(JobStatus.INITIALIZE)
        return mtx

    # ................................
    def _get_rules_for_pam(self, pam):
        """Get the rules for a PAM

        Args:
            pam (:obj: `Matrix`): The PAM for which to generate compute rules
        """
        pam_rules = []
        # Generate the PAM
        pam_rules.extend(self._get_rules_for_pam_assembly(pam))
        pam_id = pam.getId()
        
        # Initialize variables
        pruned_pam_filename = pam.getDLocation()
        pruned_tree_filename = self.squid_tree_filename
        encoded_tree_filename = None
        grim_filename = None
        biogeo_filename = None
        
        # If MCPA, sync tree and PAM
        if self.do_mcpa:
            (pruned_pam_filename, pruned_tree_filename, encoded_tree_filename,
             tree_encode_rules) = self._get_mcpa_tree_encode_rules_for_pam(pam)
            pam_rules.extend(tree_encode_rules)
            # Initialize MCPA output matrix
            mcpa_out_mtx = self._get_or_insert_matrix(
                MatrixType.MCPA_OUTPUTS, ProcessType.MCPA_ASSEMBLE,
                pam.gcmCode, pam.altpredCode, pam.dateCode)
            grim = self.gridset.getGRIMForCodes(
                pam.gcmCode, pam.altpredCode, pam.dateCode)
            grim_filename = grim.getDLocation()
            biogeo = self.gridset.getBiogeographicHypotheses()[0]
            biogeo_filename = biogeo.getDLocation()

        # If PAM stats, initialize matrices
        if self.do_pam_stats:
            sites_obs_mtx = self._get_or_insert_matrix(
                MatrixType.SITES_OBSERVED, ProcessType.RAD_CALCULATE,
                pam.gcmCode, pam.altpredCode, pam.dateCode)
            species_obs_mtx = self._get_or_insert_matrix(
                MatrixType.SPECIES_OBSERVED, ProcessType.RAD_CALCULATE,
                pam.gcmCode, pam.altpredCode, pam.dateCode)
            diversity_obs_mtx = self._get_or_insert_matrix(
                MatrixType.DIVERSITY_OBSERVED, ProcessType.RAD_CALCULATE,
                pam.gcmCode, pam.altpredCode, pam.dateCode)
            anc_pam_mtx = self._get_or_insert_matrix(
                MatrixType.ANC_PAM, ProcessType.RAD_CALCULATE, pam.gcmCode,
                pam.altpredCode, pam.dateCode)
        
        # Analysis rules
        (anc_pam_filename, pam_stats_filenames, mcpa_filenames, analysis_rules
         ) = self._get_analysis_rules_for_pam(
             pam.getId(), pruned_pam_filename, grim_filename=grim_filename,
             biogeo_filename=biogeo_filename,
             phylo_filename=encoded_tree_filename,
             tree_filename=pruned_tree_filename)
        # Add analysis rules
        pam_rules.extend(analysis_rules)
        
        # Add stockpile rules
        # TODO: This is ugly and fragile.  Figure out a better way to determine
        #    what stats are present in what order
        if len(pam_stats_filenames) > 0:
            pam_rules.append(
                StockpileCommand(
                    ProcessType.RAD_CALCULATE, diversity_obs_mtx.getId(),
                    self._create_filename(pam_id, 'diversity_stats.success'),
                    [pam_stats_filenames[0]]).getMakeflowRule())
            pam_rules.append(
                StockpileCommand(
                    ProcessType.RAD_CALCULATE, species_obs_mtx.getId(),
                    self._create_filename(pam_id, 'species_stats.success'),
                    [pam_stats_filenames[1]]).getMakeflowRule())
            pam_rules.append(
                StockpileCommand(
                    ProcessType.RAD_CALCULATE, sites_obs_mtx.getId(),
                    self._create_filename(pam_id, 'site_stats.success'),
                    [pam_stats_filenames[2]]).getMakeflowRule())

        # Stockpile ancestral PAM
        if anc_pam_filename is not None:
            pam_rules.append(
                StockpileCommand(
                    ProcessType.RAD_CALCULATE, anc_pam_mtx.getId(),
                    self._create_filename(pam_id, 'anc_pam.success'),
                    [anc_pam_filename]).getMakeflowRule())
        
        # Add stockpile for MCPA
        if len(mcpa_filenames) > 0:
            pam_rules.append(
                StockpileCommand(
                    ProcessType.MCPA_ASSEMBLE, mcpa_out_mtx.getId(),
                    self._create_filename(pam_id, 'mcpa.success'),
                    mcpa_filenames).getMakeflowRule())

        return pam_rules

    # ................................
    def _get_rules_for_pam_assembly(self, pam):
        """Get the rules for creating the PAM

        Args:
            pam (:obj: `Matrix`): The PAM for which to generate assembly rules

        Return:
            Tuple of PAM file location and assembly rules
        """
        assembly_rules = []
        pam_id = pam.getId()
        pam_assembly_success_filename = self._create_filename(
            pam_id, 'pam_{}_assembly.success'.format(pam.getId()))
        assembly_rules.append(AssemblePamFromSolrQueryCommand(
            pam_id, pam_assembly_success_filename,
            dependency_files=self.dependencies).getMakeflowRule())
        #return pam.getDLocation(), assembly_rules
        return assembly_rules
        
    # ................................
    def close(self):
        """Close scribe connections
        """
        try:
            self._scribe.closeConnections()
        except:
            pass

    # ................................
    def create_workflow(self):
        """Creates a workflow to accomplish the computations
        """
        meta = {
            MFChain.META_CREATED_BY: os.path.basename(__file__),
            MFChain.META_DESCRIPTION : 'Makeflow for multispecies analyses'
        }
        new_makeflow = MFChain(
            self.user_id, metadata=meta, status=JobStatus.GENERAL,
            statusModTime=mx.DateTime.gmt().mjd)
        mf_chain = self._scribe.insertMFChain(
            new_makeflow, self.gridset.getId())
        self._scribe.updateObject(mf_chain)
        
        # Get rules
        rules = self.get_collate_rules()
        mf_chain.addCommands(rules)
        # Write makeflow
        mf_chain.write()
        # Update DB
        mf_chain.updateStatus(JobStatus.INITIALIZE)
        self._scribe.updateObject(mf_chain)
        self.log.debug('Wrote Makeflow: {}'.format(mf_chain.getDLocation()))

    # ................................
    def get_collate_rules(self):
        """Generate a list of Makeflow rules for running these analyses

        Return:
            A list of MfRules
        """
        rules = []
        if len(self.gridset.getPAMs()) == 0:
            raise Exception(
                ('There are no PAMs for this gridset.'
                 '  Do they need to be filled by the scribe?'))
        if self.gridset.tree is not None:
            # Squid tree if exists and set self.squid_tree_filename
            self.squid_tree_filename = self._create_filename(
                'squid_tree{}'.format(LMFormat.NEXUS.ext))
            rules.append(
                SquidIncCommand(
                    self.gridset.tree.getDLocation(), self.user_id,
                    self.squid_tree_filename).getMakeflowRule())
        else:
            self.squid_tree_filename = None

        for pam in self.gridset.getPAMs():
            rules.extend(self._get_rules_for_pam(pam))
        
        return rules

# .............................................................................
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create a multi-species workflow for a gridset')
    parser.add_argument(
        'gridset_id', type=int, help='The identifier for a gridset to use')
    parser.add_argument(
        'num_permutations', type=int, help='The number of permutations to perform')
    parser.add_argument(
        '-p', action='store_true', dest='do_pam_stats',
        help='Should PAM stats be computed')
    parser.add_argument(
        '-m', action='store_true', dest='do_mcpa',
        help='Should MCPA be computed')
    parser.add_argument(
        '-g', type=int, default=DEFAULT_RANDOM_GROUP_SIZE,
        help='The number of permutations to group into one process')
    parser.add_argument('-w', type=str, default='./', help='A work directory')

    args = parser.parse_args()
    
    scribe = BorgScribe(ConsoleLogger())
    scribe.openConnections()
    gridset = scribe.getGridset(gridsetId=args.gridset_id, fillMatrices=True)
    scribe.closeConnections()
    collator = BoomCollate(
        gridset, do_pam_stats=args.do_pam_stats, do_mcpa=args.do_mcpa,
        num_permutations=args.num_permutations, random_group_size=args.g,
        work_dir=args.w)
    collator.create_workflow()
    collator.close()
    scribe.closeConnections()
    