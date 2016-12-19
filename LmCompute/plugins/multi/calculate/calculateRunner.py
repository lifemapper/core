"""
@summary: Module containing process runners to perform RAD calculations
@author: CJ Grady
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
# import csv
import glob
import numpy
import os
from StringIO import StringIO
import zipfile
 
from LmCommon.common.lmconstants import JobStatus, OutputFormat, ProcessType
from LmCommon.common.lmXml import Element, SubElement, tostring

from LmCompute.common.layerManager import LayerManager
from LmCompute.common.lmconstants import LayerFormat
from LmCompute.common.localconstants import JOB_DATA_PATH
from LmCompute.jobs.runners.pythonRunner import PythonRunner
from LmCompute.plugins.multi.calculate.calculate import calculate
from LmCompute.plugins.multi.common.matrixTools import getNumpyMatrixFromCSV

#SCHLUTER_KEY = 'Schluter'
#COMP_COV = 'Sites-CompositionCovariance'
#RANGE_COV = 'Species-RangesCovariance'

#DIVERSITY_KEY = 'diversity'
#LADD_BETA = 'LAdditiveBeta'
#LEGENDRE_BETA = 'LegendreBeta'
#WHITTAKERS_BETA = 'WhittakersBeta'

#MATRICES_KEY = 'matrices'
#SIGMA_SITES = 'SigmaSites'
#SIGMA_SPECIES = 'SigmaSpecies'

#SITES_KEY = 'sites'
#MEAN_PROP_RANGE_SIZE = 'MeanProportionalRangeSize'
#PER_SITE_RANGE_SIZE = 'Per-siteRangeSizeofaLocality'
#PROP_SPECIES_DIVERSITY = 'ProportionalSpeciesDiversity'
#SPECIES_RICHNESS = 'speciesRichness-perSite'
# Tree stats
#MNTD = 'MNTD'
#PEARSON_TD_SS = 'PearsonsOfTDandSitesShared'
#AVG_TD = 'AverageTaxonDistance'

#SPECIES_KEY = 'species'
#MEAN_PROP_SPECIES_DIVERSITY = 'MeanProportionalSpeciesDiversity'
#PROP_RANGE_SIZE = 'ProportionalRangeSize'
#RANGE_RICHNESS_SPECIES = 'Range-richnessofaSpecies'
#RANGE_SIZE_PER_SPECIES = 'RangeSize-perSpecies'

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
   #def _getFiles(self, shapefileName):
   #   if shapefileName is not None:
   #      return glob.iglob('%s*' % os.path.splitext(shapefileName)[0])
   #   else:
   #      return []

   # .......................................
   def _finishJob(self):
      """
      @summary: Move outputs we want to keep to the specified location
      @todo: Determine if anything else should be moved
      @todo: Should we take a name parameter?
      @todo: What should file names be?
      """
      # Options to keep:
      #  metrics
      
      if self.outDir is not None:
         fn = os.path.join(self.outDir, "{jobName}{ext}".format(
            jobName=self.jobName, ext=OutputFormat.PICKLE))
      else:
         fn = os.path.join(self.workDir, "{jobName}{ext}".format(
            jobName=self.jobName, ext=OutputFormat.PICKLE))
         
      pickle.dump(self.summaryData, fn)
         

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
      print self.job.matrix.url
      lyrMgr = LayerManager(JOB_DATA_PATH)
       
      sgLayerId = self.job.shapegrid.identifier
      sgUrl = self.job.shapegrid.shapegridUrl
      
      self.shapegrid = {
                        #TODO: Make sure this works correctly
                   'dlocation' : lyrMgr.getLayerFilename(sgLayerId, 
                                                         LayerFormat.SHAPE, 
                                                         layerUrl=sgUrl),
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
