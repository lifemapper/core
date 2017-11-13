"""
@summary: Module containing functions to calculate PAM statistics
@author: CJ Grady
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
from math import sqrt
import numpy as np

from LmCommon.common.lmconstants import PamStatKeys
from LmCommon.common.matrix import Matrix

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
      
      if self.tree is not None:
         phyloDistMtx = self.tree.getDistanceMatrix()
         self._calculateMNTD(phyloDistMtx)
         self._calculateTreeStats(phyloDistMtx)
   
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
      return Matrix(
         np.array([self.whittaker, self.lande, self.legendre]).reshape((1, 3)),
                    headers={'0': ['value'],
                             '1': [PamStatKeys.WHITTAKERS_BETA,
                                 PamStatKeys.LANDES_ADDATIVE_BETA,
                                 PamStatKeys.LEGENDRES_BETA]})
   
   # ...........................
   def getSchluterCovariances(self):
      """
      @summary: Calculate and return the Schluter variance ratio statistics
      @todo: Avoid hard code in reshape
      """
      # Try to use already computed co-variance matrices, if that fails, 
      #    calculate them too
      try:
         spVarRatio = float(self.sigmaSpecies.sum()) / self.sigmaSpecies.trace()
         siteVarRatio = float(self.sigmaSites.sum()) / self.sigmaSites.trace()
      except:
         # If we haven't calculated sigma sites and sigma species
         self._calculateCovarianceMatrices()
         spVarRatio = float(self.sigmaSpecies.sum()) / self.sigmaSpecies.trace()
         siteVarRatio = float(self.sigmaSites.sum()) / self.sigmaSites.trace()
      
      return Matrix(np.array([spVarRatio, siteVarRatio]).reshape((1, 2)),
                    headers={'0': ['Value'],
                             '1': [PamStatKeys.SPECIES_VARIANCE_RATIO,
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
         statColumns.extend([self.mntd, self.mpd, self.pearson])
         sitesHeaders.extend([PamStatKeys.MNTD, PamStatKeys.MPD, 
                              PamStatKeys.PEARSON])
      
      # Return a matrix
      return Matrix(np.concatenate(statColumns, axis=1), 
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
      spHeaders = {'1': [PamStatKeys.OMEGA, PamStatKeys.OMEGA_PROP, 
                       PamStatKeys.PSI, PamStatKeys.PSI_AVG_PROP]}
      # Return a Matrix
      return Matrix(spData, headers=spHeaders)
   
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
      #    presences.  This will let the stats ignore empty columns
      self.numSpecies = np.sum(np.any(self.pamData, axis=0))

      # Calculate the number of sites that have at least one species present
      self.numSites = np.sum(np.any(self.pamData, axis=1))
      
      # Site statistics
      self.alphaProp = self.alpha.astype(float) / self.numSpecies
      self.phi = self.pamData.dot(self.omega)
      # phiAvgProp can have np.nan values if empty rows and columns, set to zero
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
      self.sigmaSites = (alphaMtx / self.numSpecies) - np.outer(self.alphaProp, 
                                                           self.alphaProp)
      self.sigmaSpecies = (omegaMtx / self.numSites) - np.outer(self.omegaProp,
                                                           self.omegaProp)
   
   # ...........................
   def _calculateDiversityStatistics(self):
      """
      @summary: Calculate the (beta) diversity statistics for this PAM
      """
      self.whittaker = float(self.numSpecies) / self.omegaProp.sum()
      self.lande = self.numSpecies - self.omegaProp.sum()
      self.legendre = self.omega.sum() - (float((self.omega**2).sum()
                                                            ) / self.numSites)
   
   # ...........................
   def _calculateMNTD(self, pdMtx):
      """
      @summary: Calculate mean nearest taxon distance for each site
      @param pdMtx: Phylogenetic distance matrix from each node to all others
      """
      mntd = []
      
      for site in self.pamData:
         sp = np.where(site == 1)[0]
         
         numSp = len(sp)
         nearestTotal = 0.0
         if numSp > 1:
            for i in sp:
               nearest = 99999
               for j in sp:
                  cmpVal = pdMtx.data[i, j]
                  if cmpVal > 0.0 and cmpVal < nearest:
                     nearest = cmpVal
               print nearest
               nearestTotal += nearest
            mntd.append(float(nearestTotal) / numSp)
         else:
            mntd.append(0.0)
      # Set the mntd attribute
      self.mntd = np.array(mntd).reshape((len(mntd), 1))
   
   # ...........................
   def _calculateTreeStats(self, pdMtx):
      """
      @summary: Calculate mean pairwise distance and Pearson's correlation 
                   coefficient for pair distance and pair sites shared
                   statistics using the tree
      @param pdMtx: Phylogenetic distance matrix from each node to all others
      @note: This is the covariance of taxon distance and sites shared by each
                pair of species in a cell
      """
      mpd = []
      pearson = []
      
      for site in self.pamData:
         sp = np.where(site == 1)[0].tolist()
         numSp = len(sp)
         numPairs = numSp * (numSp - 1) / 2
         
         pairSitesShared = []
         pairDistance = []
         
         if len(sp) > 1:
            while len(sp) > 1:
               i1 = sp.pop(0)
               for i2 in sp:
                  cmpVal = pdMtx.data[i1, i2]
                  pairDistance.append(cmpVal)
                  pairSitesShared.append(self.pamData[:,i1].dot(self.pamData[:,i2]))
         
         if numPairs >= 2:
            # Multiple denominator by 2 because we add pairs twice above
            mpd.append(float(sum(pairDistance)) / numPairs)
            
            # X - pair distance
            x = np.array(pairDistance)
            # Y - pair sites shared
            y = np.array(pairSitesShared)
            sumXY = np.sum(x*y)
            sumX = np.sum(x)
            sumY = np.sum(y)
            sumXsq = np.sum(x**2)
            sumYsq = np.sum(y**2)
            
            pearsonNumerator = sumXY - sumX*sumY / numPairs
            pearsonDenominator = sqrt((sumXsq - (sumX**2 / numPairs)) * (
                                                sumYsq - (sumY**2 / numPairs)))
            
            pearson.append(pearsonNumerator / pearsonDenominator)
         else:
            mpd.append(0.0)
            pearson.append(0.0)

      # Create numpy arrays
      numSites = mpd.shape[0]
      self.mpd = np.array(mpd).reshape((numSites, 1))
      self.pearson = np.array(pearson).reshape((numSites, 1))
   
