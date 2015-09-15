"""
@summary: Module containing process runners to perform RAD calculations
@author: CJ Grady
@version: 3.0.0
@status: beta

@license: gpl2
@copyright: Copyright (C) 2014, University of Kansas Center for Research

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
# import csv
import glob
import numpy
import os
from StringIO import StringIO
import zipfile
 
from LmCommon.common.lmconstants import JobStatus, ProcessType
from LmCommon.common.lmXml import Element, SubElement, tostring

from LmCompute.common.layerManager import getAndStoreShapefile
from LmCompute.jobs.runners.pythonRunner import PythonRunner
from LmCompute.plugins.rad.calculate.calculate import calculate
from LmCompute.plugins.rad.common.matrixTools import getNumpyMatrixFromCSV

SCHLUTER_KEY = 'Schluter'
COMP_COV = 'Sites-CompositionCovariance'
RANGE_COV = 'Species-RangesCovariance'

DIVERSITY_KEY = 'diversity'
LADD_BETA = 'LAdditiveBeta'
LEGENDRE_BETA = 'LegendreBeta'
WHITTAKERS_BETA = 'WhittakersBeta'

MATRICES_KEY = 'matrices'
SIGMA_SITES = 'SigmaSites'
SIGMA_SPECIES = 'SigmaSpecies'

SITES_KEY = 'sites'
MEAN_PROP_RANGE_SIZE = 'MeanProportionalRangeSize'
PER_SITE_RANGE_SIZE = 'Per-siteRangeSizeofaLocality'
PROP_SPECIES_DIVERSITY = 'ProportionalSpeciesDiversity'
SPECIES_RICHNESS = 'speciesRichness-perSite'
# Tree stats
MNTD = 'MNTD'
PEARSON_TD_SS = 'PearsonsOfTDandSitesShared'
AVG_TD = 'AverageTaxonDistance'

SPECIES_KEY = 'species'
MEAN_PROP_SPECIES_DIVERSITY = 'MeanProportionalSpeciesDiversity'
PROP_RANGE_SIZE = 'ProportionalRangeSize'
RANGE_RICHNESS_SPECIES = 'Range-richnessofaSpecies'
RANGE_SIZE_PER_SPECIES = 'RangeSize-perSpecies'

# .............................................................................
def getStatisticArrayOrNone(value):
   if value is not None:
      statStr = StringIO()
      #numpy.save(value, statStr)
      numpy.savetxt(statStr, value)
      statStr.seek(0)
      return statStr.getvalue()
   else:
      return None

# .............................................................................
class CalculateRunner(PythonRunner):
   """
   @summary: RAD calculate job runner class
   """
   PROCESS_TYPE = ProcessType.RAD_CALCULATE
   
   # ...................................
   def _getFiles(self, shapefileName):
      if shapefileName is not None:
         return glob.iglob('%s*' % os.path.splitext(shapefileName)[0])
      else:
         return []

   # .......................................
   def _push(self):
      """
      @summary: Pushes the results of the job to the job server
      """
      #raise Exception, "_push Not implemented"
      if self.status < JobStatus.GENERAL_ERROR:
         self.status = JobStatus.COMPLETE
         component = "package"
         contentType = "application/octet-stream"
         
         el = Element('statistics')
         schluterEl = SubElement(el, 'Schluter')
         if self.summaryData[SCHLUTER_KEY][COMP_COV] is not None:
            SubElement(schluterEl, COMP_COV, 
                       value=self.summaryData[SCHLUTER_KEY][COMP_COV])
         
         if self.summaryData[SCHLUTER_KEY][RANGE_COV] is not None:
            SubElement(schluterEl, RANGE_COV, 
                       value=self.summaryData[SCHLUTER_KEY][RANGE_COV])
         
         divEl = SubElement(el, 'diversity')
         SubElement(divEl, LADD_BETA, 
                    value=self.summaryData[DIVERSITY_KEY][LADD_BETA])
         SubElement(divEl, LEGENDRE_BETA, 
                    value=self.summaryData[DIVERSITY_KEY][LEGENDRE_BETA])
         SubElement(divEl, WHITTAKERS_BETA,
                    value=self.summaryData[DIVERSITY_KEY][WHITTAKERS_BETA])
         
         print(self.summaryData)
         # Initialize zip file
         outStream = StringIO()
         with zipfile.ZipFile(outStream, 'w', compression=zipfile.ZIP_DEFLATED, 
                                 allowZip64=True) as zf:
            xmlString = StringIO(tostring(el))
            xmlString.seek(0)
            zf.writestr('statistics.xml', xmlString.getvalue())
            
            sigmaSitesVal = getStatisticArrayOrNone(
                                   self.summaryData[MATRICES_KEY][SIGMA_SITES])
            sigmaSpeciesVal = getStatisticArrayOrNone(
                                 self.summaryData[MATRICES_KEY][SIGMA_SPECIES])
            meanPropRangeSize = getStatisticArrayOrNone(
                             self.summaryData[SITES_KEY][MEAN_PROP_RANGE_SIZE])
            perSiteRangeSize = getStatisticArrayOrNone(
                              self.summaryData[SITES_KEY][PER_SITE_RANGE_SIZE])
            propSpecDiv = getStatisticArrayOrNone(
                           self.summaryData[SITES_KEY][PROP_SPECIES_DIVERSITY])
            specRich = getStatisticArrayOrNone(
                                 self.summaryData[SITES_KEY][SPECIES_RICHNESS])
            meanPropSpecDiv = getStatisticArrayOrNone(
                    self.summaryData[SPECIES_KEY][MEAN_PROP_SPECIES_DIVERSITY])
            propRangeSize = getStatisticArrayOrNone(
                                self.summaryData[SPECIES_KEY][PROP_RANGE_SIZE])
            rangeRich = getStatisticArrayOrNone(
                         self.summaryData[SPECIES_KEY][RANGE_RICHNESS_SPECIES])
            rangeSize = getStatisticArrayOrNone(
                         self.summaryData[SPECIES_KEY][RANGE_SIZE_PER_SPECIES])
            
            # Trees
            pearsonTdSs = getStatisticArrayOrNone(
                         self.summaryData[SITES_KEY][PEARSON_TD_SS])
            avgTd = getStatisticArrayOrNone(
                         self.summaryData[SITES_KEY][AVG_TD])
            mntd = getStatisticArrayOrNone(
                         self.summaryData[SITES_KEY][MNTD])

            if sigmaSitesVal is not None:
               zf.writestr('%s.npy' % SIGMA_SITES, sigmaSitesVal)
            if sigmaSpeciesVal is not None:
               zf.writestr('%s.npy' % SIGMA_SPECIES, sigmaSpeciesVal)
            if meanPropRangeSize is not None:
               zf.writestr('%s.npy' % MEAN_PROP_RANGE_SIZE, meanPropRangeSize)
            if perSiteRangeSize is not None:
               zf.writestr('%s.npy' % PER_SITE_RANGE_SIZE, perSiteRangeSize)
            if propSpecDiv is not None:
               zf.writestr('%s.npy' % PROP_SPECIES_DIVERSITY, propSpecDiv)
            if specRich is not None:
               zf.writestr('%s.npy' % SPECIES_RICHNESS, specRich)
            if meanPropSpecDiv is not None:
               zf.writestr('%s.npy' % MEAN_PROP_SPECIES_DIVERSITY, 
                                                               meanPropSpecDiv)
            if propRangeSize is not None:
               zf.writestr('%s.npy' % PROP_RANGE_SIZE, propRangeSize)
            if rangeRich is not None:
               zf.writestr('%s.npy' % RANGE_RICHNESS_SPECIES, rangeRich)
            if rangeSize is not None:
               zf.writestr('%s.npy' % RANGE_SIZE_PER_SPECIES, rangeSize)
               
            # Add tree stats
            if pearsonTdSs is not None:
               print("Adding pearsons!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
               zf.writestr('%s.npy' % PEARSON_TD_SS, pearsonTdSs)
            else:
               print("Pearsons was None")
            if avgTd is not None:
               zf.writestr('%s.npy' % AVG_TD, avgTd)
            if mntd is not None:
               zf.writestr('%s.npy' % MNTD, mntd)
               

            # Write log file
            zf.write(self.jobLogFile, os.path.split(self.jobLogFile)[1])

#TODO: Add this back in!
#             # Need to write shapegrid
#             if createShapefileFromSum(self.summaryData[SITES_KEY], self.shapegrid['dlocation'], self.shapegrid['localIdIdx'], self.sitesPresent):
#                self.log.debug("shapefile created")
#                # Write shapefile
#                for f in self._getFiles(self.shapegrid['dlocation']):
#                   if os.path.splitext(f)[1] in SHAPEFILE_EXTENSIONS:
#                      zf.write(f, "shapegrid%s" % os.path.splitext(f)[1])
#             else:
#                self.status = JobStatus.RAD_CALCULATE_FAILED_TO_CREATE_SHAPEFILE
#                raise Exception ("Failed to write statistics shapefile")


         outStream.seek(0)
         content = outStream.getvalue()
         
         self._update()
         try:
            self.env.postJob(self.PROCESS_TYPE, self.job.jobId, content, 
                                contentType, component)
         except Exception, e:
            try:
               self.log.debug(str(e))
            except: # Log not initialized
               pass
            self.status = JobStatus.PUSH_FAILED
            self._update()
      else:
         self._update()
         component = "error"
         content = None

   # ...................................
   def _doWork(self):
      self.status, self.summaryData = calculate(self.matrix, 
                                             covMatrix=self.doCovarianceMatrix, 
                                             Schluter=self.doSchluter, 
                                             treeStats=self.doTreeStats, 
                                             treeData=self.treeData)
   
   # ...................................
   def _processJobInput(self):
      self.log.debug("Start of process job input")
      sgUrl = self.job.shapegrid.url
      print self.job.matrix.url
      vectorPath = os.path.join(self.outputPath, 'vectorLayers')
       
      self.shapegrid = {
                   'dlocation' : getAndStoreShapefile(sgUrl, vectorPath),
                   'localIdIdx' : int(self.job.shapegrid.localIdIndex),
                   #'cellsideCount' : self.job.shapegrid.cellsides
                  }
      
      self.matrix = getNumpyMatrixFromCSV(csvUrl=self.job.matrix.url)
      
      self.sitesPresent = {}
      for site in self.job.sitesPresent.site:
         self.sitesPresent[int(site.key)] = bool(site.present.lower() == 'true')
   
      self.doSchluter = bool(self.job.doSchluter.lower() == 'true')
      self.doCovarianceMatrix = bool(self.job.doCovarianceMatrix.lower() == 'true')
      
      # Tree
      self.doTreeStats = bool(self.job.doTaxonDistance.lower() == 'true')
      try:
         self.treeData = self.job.treeData
      except Exception, e:
         print dir(self.job)
         print e
         print self.job.__dict__
         self.treeData = None
      
      print self.doSchluter, self.doCovarianceMatrix, self.doTreeStats, self.treeData
