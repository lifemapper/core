"""Module containing functions to calculate PAM statistics

Todo:
    Convert to use Matrix instead of numpy matrices
"""
import numpy as np

from LmCommon.common.lmconstants import PamStatKeys, PhyloTreeKeys
from LmCompute.plugins.multi.calculate import ot_phylo
from lmpy import Matrix


# .............................................................................
class PamStats:
    """This class is used to calculate statistics for a PAM
    """

    # ...........................
    def __init__(self, pam, tree=None):
        """Constructor

        Args:
            pam: A Present / Absence Matrix to compute statistics for
            tree: An optional TreeWrapper object to use for additional
                statistics
        """
        # Ensure PAM is a Matrix object.  PAM data will be shortcut to data
        if isinstance(pam, Matrix):
            self.pam = pam
        else:
            self.pam = Matrix(pam)

        self.tree = tree
        self.alpha = None
        self.alpha_prop = None
        self.c_score = None
        self.lande = None
        self.legendre = None
        self.num_sites = None
        self.num_species = None
        self.omega = None
        self.omega_prop = None
        self.phi = None
        self.phi_avg_prop = None
        self.psi = None
        self.psi_avg_prop = None
        self.sigma_sites = None
        self.sigma_species = None
        self.whittaker = None

        self._calculate_core_stats()
        self._calculate_diversity_statistics()

    # ...........................
    def get_covariance_matrices(self):
        """Returns the covariance matrices for the PAM

        Todo:
            Add headers
        """
        try:
            return self.sigma_sites, self.sigma_species
        except Exception:
            # We haven't calculated them yet
            self._calculate_covariance_matrices()
            return self.sigma_sites, self.sigma_species

    # ...........................
    def get_diversity_statistics(self):
        """Get the (beta) diversity statistics
        """
        return Matrix.concatenate(
            [self.whittaker, self.lande, self.legendre, self.c_score], axis=1)

    # ...........................
    def get_schluter_covariances(self):
        """Calculate and return the Schluter variance ratio statistics
        """
        # Try to use already computed co-variance matrices, if that fails,
        #     calculate them too
        try:
            sp_var_ratio = float(
                self.sigma_species.sum()) / self.sigma_species.trace()
            site_var_ratio = float(
                self.sigma_sites.sum()) / self.sigma_sites.trace()
        except Exception:
            # If we haven't calculated sigma sites and sigma species
            self._calculate_covariance_matrices()
            sp_var_ratio = float(
                self.sigma_species.sum()) / self.sigma_species.trace()
            site_var_ratio = float(
                self.sigma_sites.sum()) / self.sigma_sites.trace()

        return Matrix(
            np.nan_to_num(np.array([[sp_var_ratio, site_var_ratio]])),
            headers={
                '0': ['Value'],
                '1': [PamStatKeys.SPECIES_VARIANCE_RATIO,
                      PamStatKeys.SITES_VARIANCE_RATIO]})

    # ...........................
    def get_site_statistics(self):
        """Retrieves the site statistics as a Matrix of site statistic columns
        """
        num_rows = self.alpha.shape[0]
        stat_columns = [
            self.alpha.reshape(num_rows, 1),
            self.alpha_prop.reshape(num_rows, 1),
            self.phi.reshape(num_rows, 1),
            self.phi_avg_prop.reshape(num_rows, 1)]
        sites_headers = [
            PamStatKeys.ALPHA, PamStatKeys.ALPHA_PROP, PamStatKeys.PHI,
            PamStatKeys.PHI_AVG_PROP]

        # Check if we have tree stats too
        if self.tree is not None:
            # Get phylogenetic distance matrix
            phylo_dist_mtx = self.tree.get_distance_matrix()

            squid_annotations = self.tree.get_annotations(PhyloTreeKeys.SQUID)
            squid_dict = {squid: label for label, squid in squid_annotations}
            taxon_labels = []
            keep_columns = []
            squids = self.pam.get_column_headers()
            for i, squid in enumerate(squids):
                if squid in list(squid_dict.keys()):
                    keep_columns.append(i)
                    taxon_labels.append(squid_dict[squid])
            # Slice the PAM to remove missing squid columns
            sl_pam = self.pam.slice(
                list(range(self.pam.shape[0])), keep_columns)

            stat_columns.extend([
                ot_phylo.mean_nearest_taxon_distance(sl_pam, phylo_dist_mtx),
                ot_phylo.mean_pairwise_distance(sl_pam, phylo_dist_mtx),
                ot_phylo.pearson_correlation(sl_pam, phylo_dist_mtx),
                ot_phylo.phylogenetic_diversity(
                    sl_pam, self.tree, taxon_labels),
                ot_phylo.sum_pairwise_distance(sl_pam, phylo_dist_mtx)])

            sites_headers.extend(
                [PamStatKeys.MNTD, PamStatKeys.MPD, PamStatKeys.PEARSON,
                 PamStatKeys.PD, PamStatKeys.SPD])

        # Return a matrix
        return Matrix(
            np.nan_to_num(np.concatenate(stat_columns, axis=1)),
            headers={'0': self.pam.get_row_headers(), '1': sites_headers})

    # ...........................
    def get_species_statistics(self):
        """Retrieves the species statistics as a Matrix
        """
        num_sp = self.omega.shape[0]
        sp_data = np.concatenate(
            [self.omega.reshape(num_sp, 1),
             self.omega_prop.reshape(num_sp, 1),
             self.psi.reshape(num_sp, 1),
             self.psi_avg_prop.reshape(num_sp, 1)], axis=1)
        sp_headers = {
            '0': self.pam.get_column_headers(),
            '1': [PamStatKeys.OMEGA, PamStatKeys.OMEGA_PROP, PamStatKeys.PSI,
                  PamStatKeys.PSI_AVG_PROP]}
        # Return a Matrix
        return Matrix(np.nan_to_num(sp_data), headers=sp_headers)

    # ...........................
    def _calculate_core_stats(self):
        """This function calculates the standard PAM statistics
        """
        # Number of species at each site
        self.alpha = Matrix(np.sum(self.pam, axis=1))

        # Number of sites for each species
        self.omega = Matrix(np.sum(self.pam, axis=0))

        # Calculate the number of species by looking for columns that have any
        #     presences.  This will let the stats ignore empty columns
        self.num_species = np.sum(np.any(self.pam, axis=0))

        # Calculate the number of sites that have at least one species present
        self.num_sites = np.sum(np.any(self.pam, axis=1))

        # Site statistics
        self.alpha_prop = self.alpha.astype(float) / self.num_species
        self.phi = self.pam.dot(self.omega)
        # phiAvgProp can have np.nan values if empty rows and columns,
        #     set to zero
        self.phi_avg_prop = np.nan_to_num(
            self.phi.astype(float) / (self.num_sites * self.alpha))

        # Species statistics
        self.omega_prop = self.omega.astype(float) / self.num_sites
        self.psi = self.alpha.dot(self.pam)
        # psi_avg_prop can produce np.nan for empty row and columns, set to
        #    zero
        self.psi_avg_prop = np.nan_to_num(
            self.psi.astype(float) / (self.num_species * self.omega))

    # ...........................
    def _calculate_covariance_matrices(self):
        """Calculates the sigmaSpecies and sigmaSites covariance matrices
        """
        alpha = self.pam.dot(self.pam.T).astype(float)  # Site by site
        omega = self.pam.T.dot(self.pam).astype(float)  # Species by sites

        self.sigma_sites = (alpha / self.num_species) - np.outer(
            self.alpha_prop, self.alpha_prop)
        self.sigma_species = (omega / self.num_sites) - np.outer(
            self.omega_prop, self.omega_prop)

    # ...........................
    def _calculate_diversity_statistics(self):
        """Calculate the (beta) diversity statistics for this PAM
        """
        self.whittaker = Matrix(
            np.array([[float(self.num_species) / self.omega_prop.sum()]]),
            headers={'0': ['value'], '1': [PamStatKeys.WHITTAKERS_BETA]})

        self.lande = Matrix(
            np.array([[self.num_species - self.omega_prop.sum()]]),
            headers={'0': ['value'], '1': [PamStatKeys.LANDES_ADDATIVE_BETA]})

        self.legendre = Matrix(
            np.array([[self.omega.sum() - (
                float((self.omega ** 2).sum()) / self.num_sites)]]),
            headers={'0': ['value'], '1': [PamStatKeys.LEGENDRES_BETA]})

        temp = 0.0
        for i in range(self.num_species):
            for j in range(i, self.num_species):
                # Get the number shared (where both are == 1, so sum == 2)
                num_shared = len(
                    np.where(np.sum(self.pam[:, [i, j]], axis=1) == 2)[0])
                p_1 = self.omega[i] - num_shared
                p_2 = self.omega[j] - num_shared
                temp += p_1 * p_2
        self.c_score = Matrix(
            np.array([
                [2 * temp / (self.num_species * (self.num_species - 1))]]),
            headers={'0': ['value'], '1': [PamStatKeys.C_SCORE]})
