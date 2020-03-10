"""Tools for generating Makeflow rules for multi-species analyses
"""
import argparse
import os

from LmBackend.command.common import CreateSignificanceMatrixCommand
from LmBackend.command.multi import (
    CreateAncestralPamCommand, EncodePhylogenyCommand, MultiSpeciesRunCommand,
    SyncPamAndTreeCommand)
from LmBackend.command.server import (
    AssemblePamFromSolrQueryCommand, StockpileCommand,
    SquidAndLabelTreeCommand)
from LmBackend.common.lmobj import LMObject
from LmCommon.common.lmconstants import (
    JobStatus, LMFormat, MatrixType, ProcessType)
from LmCommon.common.time import gmt
from LmServer.common.lmconstants import (
    DEFAULT_NUM_PERMUTATIONS, DEFAULT_RANDOM_GROUP_SIZE)
from LmServer.common.log import ConsoleLogger
from LmServer.db.borg_scribe import BorgScribe
from LmServer.legion.lm_matrix import LMMatrix
from LmServer.legion.process_chain import MFChain


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
        self.user_id = gridset.get_user_id()
        if work_dir is not None:
            self.workspace_dir = os.path.join(
                work_dir, 'gs_{}'.format(gridset.get_id()))
        else:
            self.workspace_dir = 'gs_{}'.format(gridset.get_id())

        if dependencies is None:
            self.dependencies = []
        elif isinstance(dependencies, list):
            self.dependencies = dependencies
        else:
            self.dependencies = [dependencies]

        # Todo: Check if required inputs are present
        self.do_mcpa = bool(do_mcpa)
        self.do_pam_stats = bool(do_pam_stats)
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
        for i, obs_ps_fn in enumerate(obs_pam_stats_filenames):
            # TODO: Need a better way to name this file
            out_mtx_filename = obs_ps_fn.replace(
                LMFormat.MATRIX.ext, '_sig{}'.format(LMFormat.MATRIX.ext))
            rand_fns = [fn[i] for fn in rand_pam_stats_filenames]
            aggregation_rules.append(
                CreateSignificanceMatrixCommand(
                    obs_ps_fn, out_mtx_filename, rand_fns,
                    use_abs=True, fdr=self.fdr).get_makeflow_rule())
            sig_pam_stats_filenames.append(out_mtx_filename)

        if obs_mcpa_filenames is not None and len(obs_mcpa_filenames) > 0:
            # Create rule
            out_mtx_filename = self._create_filename(
                pam_id, 'mcpa_out{}'.format(LMFormat.MATRIX.ext))
            aggregation_rules.append(
                CreateSignificanceMatrixCommand(
                    obs_mcpa_filenames[0], out_mtx_filename,
                    rand_mcpa_filenames, use_abs=True, fdr=self.fdr,
                    test_matrix=obs_mcpa_filenames[1]).get_makeflow_rule())
            sig_mcpa_stats_filenames.append(out_mtx_filename)
        self.log.debug(
            'Created {} aggregation rules for pam {}'.format(
                len(aggregation_rules), pam_id))
        return (sig_pam_stats_filenames, sig_mcpa_stats_filenames,
                aggregation_rules)

    # ................................
    def _get_ancestral_pam_rules_for_pam(self, pam_id, pam_filename,
                                         tree_filename, dependency_filename,
                                         pam_success_filename=None):
        """Get rules for creating ancestral PAM
        """
        anc_pam_rules = []
        # Ancestral PAM
        if tree_filename is not None:
            anc_pam_filename = self._create_filename(
                pam_id, 'anc_pam{}'.format(LMFormat.MATRIX.ext))
            anc_pam_rule = CreateAncestralPamCommand(
                pam_filename, tree_filename, anc_pam_filename)
            anc_pam_rule.inputs.append(dependency_filename)
            if pam_success_filename is not None:
                anc_pam_rule.inputs.append(pam_success_filename)
            anc_pam_rules.append(anc_pam_rule.get_makeflow_rule())
        else:
            anc_pam_filename = None
        return (anc_pam_filename, anc_pam_rules)

    # ................................
    def _get_analysis_rules_for_pam(
            self, pam_id, pam_filename, grim_filename=None,
            biogeo_filename=None, phylo_filename=None, tree_filename=None,
            pam_success_filename=None):
        """Get the analysis rules for a PAM
        """
        pam_analysis_rules = []
        # Observed rules
        (obs_pam_stats_filenames, obs_mcpa_filenames, obs_rules
         ) = self._get_multispecies_run_rules_for_pam(
             pam_id, 'obs', pam_filename, 0, grim_filename=grim_filename,
             biogeo_filename=biogeo_filename, phylo_filename=phylo_filename,
             tree_filename=tree_filename,
             pam_success_filename=pam_success_filename)
        self.log.debug(
            'Created {} observed rules for pam {}'.format(
                len(obs_rules), pam_id))
        pam_analysis_rules.extend(obs_rules)

        # Randomized
        rand_pam_stats_filenames = []
        rand_mcpa_stats_filenames = []
        for i, val in enumerate(
                range(0, self.num_permutations, self.random_group_size)):

            (pam_stats_filenames, mcpa_filenames, rand_rules
             ) = self._get_multispecies_run_rules_for_pam(
                 pam_id, 'rand{}'.format(i), pam_filename,
                 min(self.random_group_size, self.num_permutations - val),
                 grim_filename=grim_filename, biogeo_filename=biogeo_filename,
                 phylo_filename=phylo_filename, tree_filename=tree_filename,
                 pam_success_filename=pam_success_filename)
            rand_pam_stats_filenames.append(pam_stats_filenames)
            rand_mcpa_stats_filenames.append(mcpa_filenames)
            pam_analysis_rules.extend(rand_rules)

        # Aggregates
        (sig_pam_stats_filenames, sig_mcpa_filenames, agg_rules
         ) = self._get_aggregation_rules_for_pam(
             pam_id, obs_pam_stats_filenames, obs_mcpa_filenames,
             rand_pam_stats_filenames, rand_mcpa_stats_filenames)
        # Add aggregation rules
        pam_analysis_rules.extend(agg_rules)

        self.log.debug(
            'Added {} total analysis rules for pam {}'.format(
                len(pam_analysis_rules), pam_id))
        return (sig_pam_stats_filenames, sig_mcpa_filenames,
                pam_analysis_rules)

    # ................................
    def _get_mcpa_tree_encode_rules_for_pam(self, pam, pam_filename,
                                            tree_filename):
        """Get MCPA tree encoding rules for PAM

        Args:
            pam (:obj: `Matrix`): The PAM to use for MCPA
        """
        pam_id = pam.get_id()
        mcpa_tree_encode_rules = []

        # File names
        encoded_tree_filename = self._create_filename(
            pam_id, 'encoded_tree{}'.format(LMFormat.MATRIX.ext))

        # Encode tree
        mcpa_tree_encode_rules.append(
            EncodePhylogenyCommand(
                tree_filename, pam_filename, encoded_tree_filename
                ).get_makeflow_rule())
        self.log.debug(
            'Added {} tree encode rules for pam {}'.format(
                len(mcpa_tree_encode_rules), pam_id))
        return (encoded_tree_filename, mcpa_tree_encode_rules)

    # ................................
    def _get_multispecies_run_rules_for_pam(
            self, pam_id, group_prefix, pam_filename, num_permutations,
            grim_filename=None, biogeo_filename=None, phylo_filename=None,
            tree_filename=None, pam_success_filename=None):
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
                # site_covariance_filename=None,
                # species_covariance_filename=None,
                mcpa_output_filename=mcpa_filename,
                mcpa_f_matrix_filename=mcpa_f_vals_filename,
                pam_success_filename=pam_success_filename).get_makeflow_rule())
        self.log.debug(
            'Adding {} run rules for pam {}'.format(len(run_rules), pam_id))
        return pam_stats_return, mcpa_return, run_rules

    # ................................
    def _get_or_insert_matrix(self, mtx_type, process_type, gcm_code,
                              alt_pred_code, date_code):
        """Attempt to find a matching matrix or create a new one

        Args:
            mtx_type (:obj: `int`): A MatrixType constant
            process_type (:obj: `int`): A ProcessType constant
            gcm_code (:obj: `str`): A GCM code for this LMMatrix
            alt_pred_code (:obj: `str`): An alternate prediction code to use
                for this LMMatrix
            date_code (:obj: `str`): A date code to use for this LMMatrix
        """
        new_mtx = LMMatrix(
            None, matrix_type=mtx_type, process_type=process_type,
            gcm_code=gcm_code, alt_pred_code=alt_pred_code,
            date_code=date_code, user_id=self.user_id, gridset=self.gridset)
        mtx = self._scribe.find_or_insert_matrix(new_mtx)
        mtx.update_status(JobStatus.INITIALIZE)
        self.log.debug('Inserted or found matrix {}'.format(mtx.get_id()))
        return mtx

    # ................................
    def _get_rules_for_pam(self, pam):
        """Get the rules for a PAM

        Args:
            pam (:obj: `Matrix`): The PAM for which to generate compute rules
        """
        pam_rules = []
        # Generate the PAM
        (pam_success_filename, pam_assembly_rules
         ) = self._get_rules_for_pam_assembly(pam)
        pam_rules.extend(pam_assembly_rules)
        pam_id = pam.get_id()

        # Initialize variables
        pruned_pam_filename = pam.get_dlocation()
        pruned_tree_filename = self.squid_tree_filename
        encoded_tree_filename = None
        grim_filename = None
        biogeo_filename = None

        # If there is a tree, the PAM and tree must be synced before we can do
        #    some of the other stats
        if self.gridset.tree is not None:
            # Sync PAM and tree
            pruned_pam_filename = self._create_filename(
                pam_id, 'pruned_pam{}'.format(LMFormat.MATRIX.ext))
            pruned_tree_filename = self._create_filename(
                pam_id, 'pruned_tree{}'.format(LMFormat.NEXUS.ext))
            pruned_metadata_filename = self._create_filename(
                pam_id, 'pruned_metadata{}'.format(LMFormat.JSON.ext))
            sync_command = SyncPamAndTreeCommand(
                pam.get_dlocation(), pruned_pam_filename,
                self.squid_tree_filename, pruned_tree_filename,
                pruned_metadata_filename)
            sync_command.inputs.append(pam_success_filename)

            pam_rules.append(sync_command.get_makeflow_rule())

            # Ancestral PAM rules -- send pruned tree as dependency so that we
            #    know that the tree has squids
            (anc_pam_filename, anc_pam_rules
             ) = self._get_ancestral_pam_rules_for_pam(
                 pam.get_id(), pam.get_dlocation(), self.squid_tree_filename,
                 pruned_tree_filename,
                 pam_success_filename=pam_success_filename)
            pam_rules.extend(anc_pam_rules)
            anc_pam_mtx = self._get_or_insert_matrix(
                MatrixType.ANC_PAM, ProcessType.RAD_CALCULATE, pam.gcm_code,
                pam.alt_pred_code, pam.date_code)
            pam_rules.append(
                StockpileCommand(
                    ProcessType.RAD_CALCULATE, anc_pam_mtx.get_id(),
                    self._create_filename(pam_id, 'anc_pam.success'),
                    [anc_pam_filename]).get_makeflow_rule())

        # If MCPA, sync tree and PAM
        if self.do_mcpa:
            (encoded_tree_filename, tree_encode_rules
             ) = self._get_mcpa_tree_encode_rules_for_pam(
                 pam, pruned_pam_filename, pruned_tree_filename)
            pam_rules.extend(tree_encode_rules)
            # Initialize MCPA output matrix
            mcpa_out_mtx = self._get_or_insert_matrix(
                MatrixType.MCPA_OUTPUTS, ProcessType.MCPA_ASSEMBLE,
                pam.gcm_code, pam.alt_pred_code, pam.date_code)
            grim = self.gridset.get_grim_for_codes(
                pam.gcm_code, pam.alt_pred_code, pam.date_code)
            grim_filename = grim.get_dlocation()
            biogeo = self.gridset.get_biogeographic_hypotheses()[0]
            biogeo_filename = biogeo.get_dlocation()

        # If PAM stats, initialize matrices
        if self.do_pam_stats:
            sites_obs_mtx = self._get_or_insert_matrix(
                MatrixType.SITES_OBSERVED, ProcessType.RAD_CALCULATE,
                pam.gcm_code, pam.alt_pred_code, pam.date_code)
            species_obs_mtx = self._get_or_insert_matrix(
                MatrixType.SPECIES_OBSERVED, ProcessType.RAD_CALCULATE,
                pam.gcm_code, pam.alt_pred_code, pam.date_code)
            diversity_obs_mtx = self._get_or_insert_matrix(
                MatrixType.DIVERSITY_OBSERVED, ProcessType.RAD_CALCULATE,
                pam.gcm_code, pam.alt_pred_code, pam.date_code)

        # Analysis rules
        (pam_stats_filenames, mcpa_filenames, analysis_rules
         ) = self._get_analysis_rules_for_pam(
             pam.get_id(), pruned_pam_filename, grim_filename=grim_filename,
             biogeo_filename=biogeo_filename,
             phylo_filename=encoded_tree_filename,
             tree_filename=pruned_tree_filename,
             pam_success_filename=pam_success_filename)
        # Add analysis rules
        pam_rules.extend(analysis_rules)

        # Add stockpile rules
        # TODO: This is ugly and fragile.  Figure out a better way to determine
        #    what stats are present in what order
        if len(pam_stats_filenames) > 0:
            pam_rules.append(
                StockpileCommand(
                    ProcessType.RAD_CALCULATE, diversity_obs_mtx.get_id(),
                    self._create_filename(pam_id, 'diversity_stats.success'),
                    [pam_stats_filenames[0]]).get_makeflow_rule())
            pam_rules.append(
                StockpileCommand(
                    ProcessType.RAD_CALCULATE, species_obs_mtx.get_id(),
                    self._create_filename(pam_id, 'species_stats.success'),
                    [pam_stats_filenames[1]]).get_makeflow_rule())
            pam_rules.append(
                StockpileCommand(
                    ProcessType.RAD_CALCULATE, sites_obs_mtx.get_id(),
                    self._create_filename(pam_id, 'site_stats.success'),
                    [pam_stats_filenames[2]]).get_makeflow_rule())

        # Add stockpile for MCPA
        if len(mcpa_filenames) > 0:
            pam_rules.append(
                StockpileCommand(
                    ProcessType.MCPA_ASSEMBLE, mcpa_out_mtx.get_id(),
                    self._create_filename(pam_id, 'mcpa.success'),
                    mcpa_filenames).get_makeflow_rule())

        self.log.debug(
            'Adding {} total rules for PAM {}'.format(len(pam_rules), pam_id))

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
        pam_id = pam.get_id()
        pam_assembly_success_filename = self._create_filename(
            pam_id, 'pam_{}_assembly.success'.format(pam.get_id()))
        assembly_rules.append(AssemblePamFromSolrQueryCommand(
            pam_id, pam.get_dlocation(), pam_assembly_success_filename,
            dependency_files=self.dependencies).get_makeflow_rule())
        self.log.debug(
            'Adding {} assembly rules for pam {}'.format(
                len(assembly_rules), pam_id))
        # return pam.get_dlocation(), assembly_rules
        return pam_assembly_success_filename, assembly_rules

    # ................................
    def close(self):
        """Close scribe connections
        """
        try:
            self._scribe.close_connections()
        except Exception:
            pass

    # ................................
    def create_workflow(self):
        """Creates a workflow to accomplish the computations
        """
        meta = {
            MFChain.META_CREATED_BY: os.path.basename(__file__),
            MFChain.META_DESCRIPTION: 'Makeflow for multispecies analyses'
        }
        new_makeflow = MFChain(
            self.user_id, metadata=meta, status=JobStatus.GENERAL,
            status_mod_time=gmt().mjd)
        mf_chain = self._scribe.insert_mf_chain(
            new_makeflow, self.gridset.get_id())
        self._scribe.update_object(mf_chain)

        # Get rules
        rules = self.get_collate_rules()
        mf_chain.add_commands(rules)
        # Write makeflow
        mf_chain.write()
        # Update DB
        mf_chain.update_status(JobStatus.INITIALIZE)
        self._scribe.update_object(mf_chain)
        self.log.debug('Wrote Makeflow: {}'.format(mf_chain.get_dlocation()))

    # ................................
    def get_collate_rules(self):
        """Generate a list of Makeflow rules for running these analyses

        Return:
            A list of MfRules
        """
        rules = []
        if len(self.gridset.get_all_pams()) == 0:
            raise Exception(
                ('There are no PAMs for this gridset.'
                 '  Do they need to be filled by the scribe?'))
        if self.gridset.tree is not None:
            # If tree exists, squid it
            self.squid_tree_filename = self.gridset.tree.get_dlocation()
            tree_success_filename = os.path.join(
                self.workspace_dir, 'tree_squid.success')
            tree_cmd = SquidAndLabelTreeCommand(
                self.gridset.tree.get_id(), self.user_id,
                tree_success_filename)
            tree_cmd.inputs.append(self.squid_tree_filename)
            rules.append(tree_cmd.get_makeflow_rule(local=True))

            # Add tree success to dependencies
            self.dependencies.append(tree_success_filename)

        else:
            self.squid_tree_filename = None

        for pam in self.gridset.get_all_pams():
            self.log.debug('Adding rules for PAM {}'.format(pam.get_id()))
            rules.extend(self._get_rules_for_pam(pam))

        return rules


# .............................................................................
def main():
    """Main method for script
    """
    parser = argparse.ArgumentParser(
        description='Create a multi-species workflow for a gridset')
    parser.add_argument(
        'gridset_id', type=int, help='The identifier for a gridset to use')
    parser.add_argument(
        'num_permutations', type=int,
        help='The number of permutations to perform')
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
    scribe.open_connections()
    gridset = scribe.get_gridset(
        gridset_id=args.gridset_id, fill_matrices=True)
    scribe.close_connections()
    collator = BoomCollate(
        gridset, do_pam_stats=args.do_pam_stats, do_mcpa=args.do_mcpa,
        num_permutations=args.num_permutations, random_group_size=args.g,
        work_dir=args.w)
    collator.create_workflow()
    collator.close()
    scribe.close_connections()


# .............................................................................
if __name__ == '__main__':
    main()
