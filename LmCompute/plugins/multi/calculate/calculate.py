"""
@summary: Module containing functions to calculate PAM statistics
@author: CJ Grady
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

             Lifemapper Project, lifemapper [at] ku [dot] edu, 
             Biodiversity Institute,
             1345 Jayhawk Boulevard, Lawrence, Kansas, 66045, USA
    
             This program is free software; you can redistribute it and/or modify 
             it under the terms of the GNU General Public License as published by 
             the Free Software Foundation; either version 2 of the License, or (at 
             your option) any later version.
  
             This program is distributed in the hope that it will be useful, but 
             WITHOUT ANY WARRANTY; without even the implied warranty of 
             MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
             General Public License for more details.
  
             You should have received a copy of the GNU General Public License 
             along with this program; if not, write to the Free Software 
             Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
             02110-1301, USA.
"""
import numpy as np

from LmCommon.common.lmconstants import PamStatKeys, PhyloTreeKeys
from LmCommon.common.matrix import Matrix

from LmCompute.plugins.multi.calculate import ot_phylo

# .............................................................................
class PamStats(object):
    """
    @summary: This class is used to calculate statistics for a PAM
    """
    # ...........................
    def __init__(self, pam, tree=None):
        """
        @summary: Constructor
        @param pam: A Present / Absence Matrix to compute statistics for
        @param tree: An optional LmTree object to use for additional statistics
        """
        # Ensure PAM is a Matrix object.  PAM data will be shortcut to data
        if isinstance(pam, Matrix):
            self.pam = pam
            self.pamData = pam.data
        else:
            self.pam = Matrix(pam)
            self.pamData = pam

        self.tree = tree
        
        self._calculateCoreStats()
        self._calculateDiversityStatistics()
            
    # ...........................
    def getCovarianceMatrices(self):
        """
        @summary: Returns the covariance matrices for the PAM
        @todo: Add headers
        """
        try:
            return self.sigmaSites, self.sigmaSpecies
        except:
            # We haven't calculated them yet
            self._calculateCovarianceMatrices()
            return Matrix(self.sigmaSites), Matrix(self.sigmaSpecies)

    # ...........................
    def getDiversityStatistics(self):
        """
        @summary: Get the (beta) diversity statistics as a Matrix object with
                         column headers indicating which column is which
        @todo: Avoid hard code in reshape
        """
        return Matrix(np.array([self.whittaker, self.lande, 
                                self.legendre]).reshape((1, 3)),
                      headers={'0' : ['value'],
                               '1' : [PamStatKeys.WHITTAKERS_BETA,
                                      PamStatKeys.LANDES_ADDATIVE_BETA,
                                      PamStatKeys.LEGENDRES_BETA]})

    # ...........................
    def getSchluterCovariances(self):
        """
        @summary: Calculate and return the Schluter variance ratio statistics
        @todo: Avoid hard code in reshape
        """
        # Try to use already computed co-variance matrices, if that fails, 
        #     calculate them too
        try:
            spVarRatio = float(self.sigmaSpecies.sum()
                               ) / self.sigmaSpecies.trace()
            siteVarRatio = float(self.sigmaSites.sum()
                                 ) / self.sigmaSites.trace()
        except:
            # If we haven't calculated sigma sites and sigma species
            self._calculateCovarianceMatrices()
            spVarRatio = float(self.sigmaSpecies.sum()
                               ) / self.sigmaSpecies.trace()
            siteVarRatio = float(self.sigmaSites.sum()
                                 ) / self.sigmaSites.trace()
        
        return Matrix(np.nan_to_num(
                        np.array([spVarRatio, siteVarRatio]).reshape((1, 2))),
                      headers={'0' : ['Value'],
                               '1' : [PamStatKeys.SPECIES_VARIANCE_RATIO,
                                      PamStatKeys.SITES_VARIANCE_RATIO]})
    # ...........................
    def getSiteStatistics(self):
        """
        @summary: Retrieves the site statistics as a Matrix of site statistic 
                         columns
        """
        numRows = self.alpha.shape[0]
        statColumns = [self.alpha.reshape(numRows, 1),
                            self.alphaProp.reshape(numRows, 1),
                            self.phi.reshape(numRows, 1),
                            self.phiAvgProp.reshape(numRows, 1)]
        sitesHeaders = [PamStatKeys.ALPHA, PamStatKeys.ALPHA_PROP, 
                             PamStatKeys.PHI, PamStatKeys.PHI_AVG_PROP]
        
        # Check if we have tree stats too
        if self.tree is not None:
            # Get phylogenetic distance matrix
            phylo_dist_mtx = self.tree.getDistanceMatrix()
            
            squid_annotations = self.tree.getAnnotations(PhyloTreeKeys.SQUID)
            squid_dict = dict((squid, label
                               ) for label, squid in squid_annotations)
            taxon_labels = []
            keep_columns = []
            squids = self.pam.getColumnHeaders()
            for i in range(len(squids)):
                if squids[i] in squid_dict.keys():
                    keep_columns.append(i)
                    taxon_labels.append(squid_dict[squids[i]])
            # Slice the PAM to remove missing squid columns
            sl_pam = self.pam.slice(range(self.pam.data.shape[0]), 
                                    keep_columns)
            
            statColumns.extend([
                 ot_phylo.mean_nearest_taxon_distance(sl_pam, phylo_dist_mtx),
                 ot_phylo.mean_pairwise_distance(sl_pam, phylo_dist_mtx),
                 ot_phylo.pearson_correlation(sl_pam, phylo_dist_mtx),
                 ot_phylo.phylogenetic_diversity(sl_pam, self.tree, 
                                                 taxon_labels),
                 ot_phylo.sum_pairwise_distance(sl_pam, phylo_dist_mtx)
                                      ])
            sitesHeaders.extend([PamStatKeys.MNTD, PamStatKeys.MPD, 
                                        PamStatKeys.PEARSON, PamStatKeys.PD,
                                        PamStatKeys.SPD])
        
        # Return a matrix
        return Matrix(np.nan_to_num(np.concatenate(statColumns, axis=1)), 
                          headers={'0' : self.pam.getRowHeaders(),
                                      '1': sitesHeaders})
        
    # ...........................
    def getSpeciesStatistics(self):
        """
        @summary: Retrieves the species statistics as a Matrix of species 
                         statistic columns
        """
        numSp = self.omega.shape[0]
        spData = np.concatenate([self.omega.reshape(numSp, 1),
                                         self.omegaProp.reshape(numSp, 1),
                                         self.psi.reshape(numSp, 1),
                                         self.psiAvgProp.reshape(numSp, 1)],
                                        axis=1)
        spHeaders = {'0' : self.pam.getColumnHeaders(),
                         '1': [PamStatKeys.OMEGA, PamStatKeys.OMEGA_PROP, 
                              PamStatKeys.PSI, PamStatKeys.PSI_AVG_PROP]}
        # Return a Matrix
        return Matrix(np.nan_to_num(spData), headers=spHeaders)
    
    # ...........................
    def _calculateCoreStats(self):
        """
        @summary: This function calculates the standard PAM statistics
        """
        # Number of species at each site
        self.alpha = np.sum(self.pamData, axis=1)
        
        # Number of sites for each species
        self.omega = np.sum(self.pamData, axis=0)
        
        # Calculate the number of species by looking for columns that have any 
        #     presences.  This will let the stats ignore empty columns
        self.numSpecies = np.sum(np.any(self.pamData, axis=0))

        # Calculate the number of sites that have at least one species present
        self.numSites = np.sum(np.any(self.pamData, axis=1))
        
        # Site statistics
        self.alphaProp = self.alpha.astype(float) / self.numSpecies
        self.phi = self.pamData.dot(self.omega)
        # phiAvgProp can have np.nan values if empty rows and columns, 
        #     set to zero
        self.phiAvgProp = np.nan_to_num(
            self.phi.astype(float) / (self.numSites * self.alpha))
        
        # Species statistics
        self.omegaProp = self.omega.astype(float) / self.numSites
        self.psi = self.alpha.dot(self.pamData)
        # psiAvgProp can produce np.nan for empty row and columns, set to zero
        self.psiAvgProp = np.nan_to_num(
            self.psi.astype(float) / (self.numSpecies * self.omega))
    
    # ...........................
    def _calculateCovarianceMatrices(self):
        """
        @summary: Calculates the sigmaSpecies and sigmaSites covariance matrices
        """
        alphaMtx = self.pamData.dot(self.pamData.T).astype(float) # Site by site
        omegaMtx = self.pamData.T.dot(self.pamData).astype(float) # species by species
        self.sigmaSites = (alphaMtx / self.numSpecies
                           ) - np.outer(self.alphaProp, self.alphaProp)
        self.sigmaSpecies = (omegaMtx / self.numSites
                             ) - np.outer(self.omegaProp, self.omegaProp)
    
    # ...........................
    def _calculateDiversityStatistics(self):
        """
        @summary: Calculate the (beta) diversity statistics for this PAM
        """
        self.whittaker = float(self.numSpecies) / self.omegaProp.sum()
        self.lande = self.numSpecies - self.omegaProp.sum()
        self.legendre = self.omega.sum() - (float((self.omega**2
                                                   ).sum()) / self.numSites)
