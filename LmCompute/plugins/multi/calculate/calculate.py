"""
@summary: Module containing functions to randomize a RAD PAM
@author: Lifemapper Team; lifemapper@ku.edu
@version: 4.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2016, University of Kansas Center for Research

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
from itertools import combinations
import json
import math
import numpy
import os
from osgeo import ogr
import urllib2

from LmCommon.common.lmconstants import JobStatus, DEFAULT_OGR_FORMAT

# .............................................................................
def calculate(matrix, covMatrix=False, Schluter=False, treeStats=False, treeData=None):
   """
   @summary: Calculate biodiversity indices and measures for a compressed matrix
   @param matrix: the compressed matrix to be calculated
   @param shapefile: location of the shapefile for joining with site-based 
                     statistics
   @param localIdIdx: index in the shapefile attributes of the unique 
                      site ID field
   @param sitesPresent: dictionary of site ID keys with boolean value indicating
                        presence of at least one species at the site 
   @param covMatrix: boolean indicating whether to create a covariance matrix
   @param Schluter: boolean indicating whether to compute the Schluter index
   @return: summary data
   """
   summaryData = {}
   sites = {}
   species = {}
   diversity = {}
   matrices = {}
   Schluters = {}

   try:
      alpha, omega, siteCount, speciesCount = _calculateMarginals(matrix)
      alphaprop, phi, phiavgprop = _calculateSiteVectors(matrix, alpha, omega, 
                                                         siteCount, speciesCount)
      omegaprop, psi, psiavgprop = _calculateSpeciesVectors(matrix, alpha, 
                                                            omega, siteCount, 
                                                            speciesCount)
      WhittakersBeta, LAdditiveBeta, LegendreBeta = _betaDiversityIndices(matrix, 
                                                       alpha, omega, omegaprop,
                                                       siteCount, speciesCount)
      SigmaSites = SigmaSpecies = Vsps = Vsites = None
      
      mntd = pearsons = avgTD = None
      
      if covMatrix:
         SigmaSites, SigmaSpecies = _covarianceMatrices(matrix, alpha, omega, 
                                                        siteCount, speciesCount)
      if Schluter:
         Vsps, Vsites = _SchluterCovariances(matrix, alpha, omega, 
                                             siteCount, speciesCount, 
                                             SigmaSites=SigmaSites, 
                                             SigmaSpecies=SigmaSpecies)
      print treeStats
      print treeData
      if treeStats and treeData is not None:
         # Get tree
         try:
            treeDict = json.loads(treeData)
            
            allClades, tipPathsDict = buildLeaves(treeDict)
            
            mntd = calculateMNTDPerSite(matrix, tipPathsDict, allClades)
            Sxy, pearsons, avgTD = calculateTDSSCorrelation(matrix, allClades, tipPathsDict)
         except Exception, e:
            print("Failed to compute tree stats: %s" % str(e))
         
      status = JobStatus.COMPLETE
   
   except Exception, e:
      print str(e)
      status = JobStatus.RAD_CALCULATE_ERROR

   else: 
      #TODO: Keys should be constants
      
      sites['speciesRichness-perSite'] = alpha         
      sites['MeanProportionalRangeSize'] = phiavgprop
      sites['ProportionalSpeciesDiversity'] = alphaprop
      sites['Per-siteRangeSizeofaLocality'] = phi
      
      # Tree stats
      sites['MNTD'] = mntd
      sites['PearsonsOfTDandSitesShared'] = pearsons
      sites['AverageTaxonDistance'] = avgTD
      
      species['RangeSize-perSpecies'] = omega
      species['MeanProportionalSpeciesDiversity']  = psiavgprop
      species['ProportionalRangeSize'] = omegaprop
      species['Range-richnessofaSpecies'] = psi 
      diversity['WhittakersBeta'] = WhittakersBeta
      diversity['LAdditiveBeta'] = LAdditiveBeta
      diversity['LegendreBeta'] = LegendreBeta
      matrices['SigmaSpecies'] = SigmaSpecies
      matrices['SigmaSites'] = SigmaSites
      Schluters['Sites-CompositionCovariance'] = Vsites
      Schluters['Species-RangesCovariance'] = Vsps
      summaryData['sites'] = sites
      summaryData['species'] = species
      summaryData['diversity'] = diversity
      summaryData['matrices'] = matrices
      summaryData['Schluter'] = Schluters
      
      
   # Return modified, existing shapefile (dlocation) 
   return status, summaryData

# .............................................................................
def _calculateMarginals(matrix):
   """
   @summary: calculates the marginal totals for a matrix
   N = siteCount, N1 = siteVector, S = speciesCount, S1 = speciesVector 
   """
   siteCount = float(matrix.shape[0])
   siteVector = numpy.ones(siteCount)
   speciesCount = float(matrix.shape[1])
   speciesVector = numpy.ones(speciesCount)
   # range size of each species
   omega = numpy.dot(siteVector, matrix)
   # species richness of each site
   alpha = numpy.dot(matrix, speciesVector)
   
   return alpha, omega, siteCount, speciesCount

# .............................................................................
def _calculateSiteVectors(matrix, alpha, omega, siteCount, speciesCount):
   """
   @summary: calculates site based vectors
   """
   # Proportional Species Diversity of each site
   alphaprop = alpha / speciesCount  # Y axis in sites scatter plot
   # Per-site range size of a locality
   # equivalent to N1 dot A 
   # where A = X dot X.T, but trying to avoid matrix matrix multiplication
   phi = numpy.dot(matrix, omega) 
   # phiprop is just used to calc phiavgprop, but could be returned?
   phiprop = phi / siteCount
   # Mean Proportional Range Size
   # X axis in sites scatter plot     
   phiavgprop = phiprop/alpha 
   
   return alphaprop, phi, phiavgprop

# .............................................................................
def _calculateSpeciesVectors(matrix, alpha, omega, siteCount, speciesCount):
   """
   @summary: calculates species based vectors
   """ 
   # Proportional Range Size of each species
   # this is the Y axis in species scatter plot
   omegaprop = omega/siteCount  
   # Range-richness of a species
   psi = numpy.dot(alpha, matrix)
   # psiprop is just used to calc psiavgprop, but could be returned?  
   psiprop = psi/speciesCount
   # Mean Proportional Species Diversity 
   psiavgprop = psiprop/omega # this is the X axis in species scatter plot
   
   return omegaprop, psi, psiavgprop

# ..............................................................................      
def _betaDiversityIndices(matrix, alpha, omega, omegaprop, siteCount, speciesCount):
   """
   @summary: calculates, Whittakers, Lande's additive, and 
   Legendre's beta diversity indices
   """
   ############# Whittaker's Beta ###########
   omegameanprop = omegaprop.sum() / speciesCount
   WhittakersBeta = 1.0/omegameanprop
   
   ########## Lande's Additive Beta #########
   LAdditiveBeta = speciesCount * (1-1.0 / WhittakersBeta)
   
   ########## Legendre's Beta ###############    
   LegendreBeta = omega.sum() - ((omega**2).sum() / siteCount)
   
   return WhittakersBeta, LAdditiveBeta, LegendreBeta

# .............................................................................
def _sharedSpeciesSitesShared(matrix):
   """
   @summary: calculates Omega (O) and Alpha (A) matrices, matrices containing
   the number of sites shared by species and number of shared species between
   sites, respectively 
   _A = alphaMtx; _O = omegaMtx
   """
   alphaMtx = numpy.dot(matrix, matrix.T)
   omegaMtx = numpy.dot(matrix.T, matrix)
   return alphaMtx, omegaMtx

# ............................................................................
def _averageCovariances(alphaprop, omegaprop, betaW, siteCount, speciesCount ,phi, psi):
   """
   @summary: calculated average covariances without having to use matrix/matrix products
   """
   # mean composition covariance
   meanCompositionCovariance = (1/float(speciesCount*siteCount) * phi) - ((betaW**-1)*alphaprop) 
   # mean range covariance
   meanRangeCovariance = (1/float(speciesCount*siteCount) * psi)- ((betaW**-1)*omegaprop)
   
   return meanCompositionCovariance, meanRangeCovariance

# .............................................................................
def _covarianceMatrices(matrix, alpha, omega, siteCount, speciesCount):
   """
   @summary: calculates the composition of sites and range of species 
   covariance matrices
   """
#    if self._A == None or self._O == None:
#       self._sharedSpeciesSitesShared()
   alphaMtx, omegaMtx = _sharedSpeciesSitesShared(matrix)
           
   # Matrix of covariance of composition of sites
   alphaprop = alpha / speciesCount
   SigmaSites = (alphaMtx / speciesCount) - numpy.outer(alphaprop, alphaprop.T)
    
   # Matrix of covariance of ranges of species
   omegaprop = omega / siteCount
   SigmaSpecies = (omegaMtx / siteCount) - numpy.outer(omegaprop, omegaprop.T)
    
   return SigmaSites, SigmaSpecies

# ..............................................................................
def _SchluterCovariances(matrix, alpha, omega, siteCount, speciesCount,
                         SigmaSites=None, SigmaSpecies=None):
   if SigmaSites == None or SigmaSpecies == None:
      SigmaSites, SigmaSpecies = _covarianceMatrices(matrix, alpha, omega, 
                                                     siteCount, speciesCount)
   speciesVector = numpy.ones(speciesCount)
   # Schluter species-ranges covariance 
#    Vsps = numpy.dot(self._S1,numpy.dot(self._SigmaSpecies,self._S1))/self._SigmaSpecies.trace()
   try:
      Vsps = numpy.dot(speciesVector, 
                 numpy.dot(SigmaSpecies, speciesVector)) / SigmaSpecies.trace()
   except Exception, e:
      raise Exception("Vsps: %s - %s - %s - (%s)" % (speciesVector.shape, SigmaSpecies.shape, speciesVector.shape, str(e)))

   # Schluter sites-composition covariance
#    self._Vsites = numpy.dot(self._N1,numpy.dot(self._SigmaSites,self._N1))/self._SigmaSites.trace()
   try:
      siteVector = numpy.ones(siteCount) 
      Vsites = numpy.dot(siteVector, 
                      numpy.dot(SigmaSites, siteVector)) / SigmaSites.trace()
   except Exception, e: 
      raise Exception("Vsites: %s - %s - %s - (%s)" % (siteVector.shape, SigmaSites.shape, siteVector.shape, str(e)))
    
   return Vsps, Vsites
         
# .............................................................................
def createShapefileFromSum(sitesDict, shapefile, localIdIdx, sitesPresent):
   """
   @summary: Fills in features of shapefile with site-based statistics
   @param sitesDict: Dictionary of site statistics
   @param shapefile: File name of the shapefile to modify
   @param localIdIdx: The index of the local id field in the shapefile
   @param sitesPresent: A dictionary where the keys are the ids of the sites in 
                           the shapefile and the value for each is an 
                           indication if it is present 
                              {1: True, 2: False, 3: True, ...}
   @note: sitesDict and fieldNames dictionaries have the same keys
   @todo: Make dictionary keys constants
   """
   success = True
   fieldNames = {'speciesRichness-perSite' : 'specrich',      
                 'MeanProportionalRangeSize': 'avgpropRaS',
                 'ProportionalSpeciesDiversity' : 'propspecDi',
                 'Per-siteRangeSizeofaLocality' : 'RaSLoc'}
   ogr.RegisterAll()
   drv = ogr.GetDriverByName(DEFAULT_OGR_FORMAT)
   try:
      shpDs = drv.Open(shapefile, True)
   except Exception, e:
      print('Invalid datasource %s' % shapefile, str(e))
      success = False
   shpLyr = shpDs.GetLayer(0)
   
   # for each statistic, create a field
   for key in sitesDict.keys():
      fldname = fieldNames[key]
      fldtype = ogr.OFTReal
      fldDefn = ogr.FieldDefn(fldname, fldtype)
      if shpLyr.CreateField(fldDefn) != 0:
         raise Exception('CreateField failed for %s in %s' 
                       % (fldname, shapefile))
         
   sortedSites = sorted([x[0] for x in sitesPresent.iteritems() if x[1]])
   print sortedSites
   try:
      currFeat = shpLyr.GetNextFeature()         
      # for each site/cell
      while currFeat is not None:
         siteId = currFeat.GetFieldAsInteger(localIdIdx)
         if sitesPresent[siteId]:
            # if present, fill in all statistic values
            for statname in sitesDict.keys():
               currVector = sitesDict[statname]
               currval = currVector[sortedSites.index(siteId)]
               print currval
               currFeat.SetField(fieldNames[statname], currval)
            shpLyr.SetFeature(currFeat)
            currFeat.Destroy()
         currFeat = shpLyr.GetNextFeature()
      shpDs.Destroy()
   except Exception, e:
      # Maybe just log exception and return everything else
      print('Failed to write shapefile %s with statistics (%s)' 
                      % (shapefile, str(e)))
      success = False
   else:
      print('Wrote shapefile %s with statistics' % shapefile)
   return success

# =============================================================================
# =                                Tree stats                                 =
# =============================================================================
# .............................................................................
def calculateTDSSCorrelation(mtx, allClades, tipPathsDict):
   """
   @summary: This is the covariance of taxon distance and sites shared by each
                pair of species in a cell
   @param mtx: A PAM as a numpy array
   @return: 3 numpy arrays
              Sxy - Covariance between taxon distance and sites shared between species (don't use)
              p - Pearson's correlation coefficient between taxon distance and sites shared
              avgTD - Average taxon distance between pairs of species in a site
   """
   mtxT = mtx.T
 
   def myCalc(row):
      presencePos = numpy.where(row==1.0)[0]
       
      # Only do the stat if there are two or more combinations
      if len(presencePos) > 2: # In other words, 3 presences
         sitesSharedPerSpeciesCombo = []
         taxDistPerSpeciesCombo = []
         comboCount = 0
          
#          # We can probably vectorize this
         for combo in combinations(presencePos, r=2):
            taxDist = getTaxonDistanceBtweenPair(combo[0], combo[1], allClades, 
                                                 tipPathsDict)
            if taxDist is not None:
               sitesShared = numpy.dot(mtxT[combo[0]], mtxT[combo[1]])
               taxDistPerSpeciesCombo.append(taxDist)
               sitesSharedPerSpeciesCombo.append(sitesShared)
               comboCount += 1
    
         if comboCount >= 2:  # test this
            avgSS = sum(sitesSharedPerSpeciesCombo)/float(comboCount)
            avgTD = sum(taxDistPerSpeciesCombo)/float(comboCount)
            # make array of avgs            
            SSAvgs = numpy.empty(comboCount);SSAvgs.fill(avgSS)
            TDAvgs = numpy.empty(comboCount);TDAvgs.fill(avgTD)
             
            ssArray = numpy.array(sitesSharedPerSpeciesCombo)
            tdArray = numpy.array(taxDistPerSpeciesCombo)
             
            # Deviation
            ssDev = ssArray-SSAvgs
            tdDev = tdArray-TDAvgs           
            # calculate variance
            ssVariance = sum(ssDev * ssDev)/float(comboCount)
            tdVariance = sum(tdDev * tdDev)/float(comboCount)
            # calculate std dev
            ssStdDev = math.sqrt(ssVariance)
            tdStdDev = math.sqrt(tdVariance)
            # calculate covariance
            Sxy = sum(ssDev * tdDev)/float(comboCount)
            # calculate Pearsons
            p = Sxy/(ssStdDev * tdStdDev)
             
            return Sxy, p, avgTD
         else:
            return 0.0, 0.0, 0.0
      else:
         return 0.0, 0.0, 0.0
 
   retMtx = numpy.apply_along_axis(myCalc, 1, mtx)
    
   return retMtx[:,0], retMtx[:,1], retMtx[:,2]

# .............................................................................
def buildLeaves(clade):
   """
   @summary: Builds two dictionaries 
   @param clade: The (sub)tree to build out
   @todo: Jeff, fill in the documentation a bit more.
   """
   
   allClades = {} # local id in tree
   tipPathsDict = {} # matrix index key
   
   # mx is matrix index
   
   def buildTip(clade):
      
      if 'pathId' in clade:
         allClades[clade["pathId"]] = dict((k,v) for k,v in clade.items() if k != "children")
      if 'children' in clade:
         for child in clade["children"]:
            buildTip(child)
      else:
         if "mx" in clade:
            tipPathsDict[int(clade['mx'])] = clade['path']

   buildTip(clade)
   return allClades, tipPathsDict

# .............................................................................
def getTaxonDistanceBtweenPair(mtrxIdx1, mtrxIdx2, allClades, tipPathsDict):
   """
   @summary: Gets the taxon distance between two leaves
   @param mtrxIdx1: The first taxon tip
   @param mtrxIdx2: The second taxon tip
   @param allClades: Dictionary of all clades
   @param tipPathsDict: Dictionary if tips
   """
   
   try:
      sps1PathStr = tipPathsDict[mtrxIdx1]
      sps2PathStr = tipPathsDict[mtrxIdx2]
   except:
      totalLen = None
   else:
      pl1  = sps1PathStr.split(',')
      pl2  = sps2PathStr.split(',')
      pL1  = map(int,pl1)
      pL2  = map(int,pl2)
      pS1  = set(pL1)
      pS2   = set(pL2)
      ancId  = max(set.intersection(pS1,pS2)) # greatest common ancestor pathId
      sp1Len = findLengthToId(pL1, ancId, allClades)
      sp2Len = findLengthToId(pL2, ancId, allClades)
      totalLen = sp1Len + sp2Len
   
   return totalLen

# .............................................................................
def findLengthToId(path, ancId, allClades):
   """
   @summary: Finds the length from a tip to an ancestor via its path string
   @param path: The path from the root to the tip (list of ids)
   @param ancId: common ancestor Id
   @param allClades: A dictionary of all clades
   @note: All nodes under a node (children) have larger ids
   """
   totLen = 0
   for pathId in path:
      if pathId > ancId:
         length = float(allClades[str(pathId)]["length"])
         totLen = totLen + length
      else:
         break
   return totLen   
   
# .............................................................................
def findNearest(matches, pathId, allClades):
   """
   @summary: Find the nearest taxon (via taxonomic distance) from a list of 
                matches (taxa in the same site)
   @param matches: A list of paths for taxa in the same site as the path in 
                      question
   @param pathId: The path to use to find distance
   @param allClades: A dictionary of all clades
   """
   
   #print matches," ",pathId
   if len(matches) > 1:
      # have to find the shortest one
      shortestList = []        
      for matchList in matches: # goes through each of the match lists
         compare = 0
         for matchId in matchList:
            
            if matchId > pathId:
               
               length = float(allClades[str(matchId)]["length"])
               compare = compare + length
            else:
               shortestList.append(compare)
               break
      shortest = min(shortestList)                                  
            
   # Jeff, I think this might be redundant.  You should be able to just use a list with 1
   elif len(matches) == 1:
      shortest = 0
      for matchId in matches[0]:
         if matchId > pathId:
            length = float(allClades[str(matchId)]["length"])
            shortest = shortest + length
         else:
            break
   return shortest

  
# .............................................................................
def calcMNTD(pathsInSite, allClades):
   """
   @summary: Calculate mean nearest taxon distance
   @param pathsInSite: list of path strings
   """
   pathList = []
   for path in pathsInSite:
      pl = path.split(',') 
      m  = map(int,pl)  # whole list, or everything minus itself pl[1:]
      pathList.append(m) # integers
   nearestTaxonLengths = []        
   for path in pathList:
      # builds a a searchIn list that excludes current 
      # path
      index = pathList.index(path)
      searchIn = list(pathList) 
      searchIn.pop(index)
      # end search in       
      # loop through pathids the focus path and find lists with a matching pathId
      # and append to matches
      matches = []
      for pathId in path[1:]:           
         for srchPth in searchIn:
            if pathId in srchPth[1:]:
               matches.append(srchPth)
         if len(matches) > 0:
            try:
               nearestLen = findNearest(matches,pathId, allClades)                 
               lengthToPathId = findLengthToId(path,pathId, allClades)
            except Exception, e:
               print str(e)
               return 0.0
            else:   
               nearestTaxonLengths.append(nearestLen+lengthToPathId)
               break
   totAllLengths = sum(nearestTaxonLengths)
   meanNearestTaxonDist = totAllLengths/float(len(nearestTaxonLengths))
   return meanNearestTaxonDist 

# .............................................................................
def calculateMNTDPerSite(mtx, tipPathsDict, allClades):
   """
   @summary: Calculate the mean nearest taxon distance per site
   @param mtx: A PAM object to use for calculations
   @param tipPathsDict: Dictionary of tree tips
   """
   # .........................................
   def calcMNTDForSite(site):
      """
      @summary: Calculates the mean nearest taxon distance for a particular site
      @param site: The particular site to calculate MNTD for
      @note: This function will be applied along the y-axis of the matrix
      """
      # Find a one-dimensional array of where presences are in matrix
      presencePositionInSite = numpy.where(site==1.0)[0]
      
      if len(presencePositionInSite) > 1:
         allPathsForSite = []
         for presencePos in presencePositionInSite:
            # Species in PAM are not in tree necessarily, and vice versa
            if presencePos in tipPathsDict:
               tipPath = tipPathsDict[presencePos]
               allPathsForSite.append(tipPath)
         if len(allPathsForSite) >= 2:
            mntd = calcMNTD(allPathsForSite, allClades)
            return mntd
      return 0.0
   
   return numpy.apply_along_axis(calcMNTDForSite, 1, mtx)
