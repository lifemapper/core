"""
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
import mx.DateTime
import os
from time import sleep

from LmCommon.common.apiquery import BisonAPI
from LmCommon.common.localconstants import ARCHIVE_USER
from LmCommon.common.lmconstants import BISON_BINOMIAL_REGEX, BISON_NAME_KEY, \
          BISON_TSN_FILTERS, GBIF_EXPORT_FIELDS, \
          GBIF_PROVIDER_FIELD, GBIF_TAXONKEY_FIELD, Instances, ONE_MONTH, \
          ProcessType

from LmDbServer.common.lmconstants import OCC_DUMP_FILE, \
               BISON_TSN_FILE, IDIGBIO_BINOMIAL_FILE, PROVIDER_DUMP_FILE
from LmDbServer.common.localconstants import DEFAULT_ALGORITHMS, \
                                      DEFAULT_MODEL_SCENARIO, \
                                      DEFAULT_PROJECTION_SCENARIOS, \
                                      SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, \
                                      SPECIES_EXP_DAY, DEFAULT_GRID_NAME
from LmDbServer.pipeline.pipeline import _Pipeline
from LmDbServer.pipeline.localworker import Infiller, Troubleshooter, \
                  ProcessRunner, GBIFChainer, BisonChainer, iDigBioChainer
from LmDbServer.populate.bioclimMeta import TAXONOMIC_SOURCE

from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import LOG_PATH
from LmServer.common.localconstants import DATASOURCE
from LmServer.db.scribe import Scribe
from LmServer.sdm.algorithm import Algorithm

# .............................................................................
class LMArchivePipeline(_Pipeline):
   """
   The pipeline asks for jobs and then runs them.
   @precondition: Must setAlgorithm and setModelScenario before running 
             start.  May addProjectionScenario one or more times to project
             model onto alternate scenarios (besides the original 
             model scenario).
   """
   def __init__(self, pipelineName, 
                algCodes, mdlScenarioCode, projScenarioCodes, 
                mdlMaskId=None, prjMaskId=None):
      """
      @summary Constructor for the pipeline class
      @param logger: Logger to use for the main thread
      @param processid: process id for this application
      """
      _Pipeline.__init__(self, pipelineName)
      self.algs = []
      self.modelScenario = None
      self.projScenarios = []
      self.modelMask = None
      self.projMask = None
      self.intersectGrid = None
      self._fillDefaultObjects(algCodes, mdlScenarioCode, projScenarioCodes, 
                               mdlMaskId, prjMaskId, DEFAULT_GRID_NAME)
   
# ...............................................
   def _fillDefaultObjects(self, algCodes, mdlScenarioCode, projScenarioCodes, 
                           mdlMaskId, prjMaskId, intersectGridName):
      for acode in algCodes:
         alg = Algorithm(acode)
         alg.fillWithDefaults()
         self.algs.append(alg)

      try:
         scribe = Scribe(self.log)
         success = scribe.openConnections()
         
         mscen = scribe.getScenario(mdlScenarioCode)
         if mscen is not None:
            self.modelScenario = mscen
            if mdlScenarioCode not in projScenarioCodes:
               self.projScenarios.append(self.modelScenario)
            for pcode in projScenarioCodes:
               scen = scribe.getScenario(pcode)
               if scen is not None:
                  self.projScenarios.append(scen)
               else:
                  raise LMError('Failed to retrieve scenario %s' % pcode)
         else:
            raise LMError('Failed to retrieve scenario %s' % mdlScenarioCode)
         
         self.modelMask = scribe.getLayer(mdlMaskId)
         self.projMask = scribe.getLayer(prjMaskId)
         self.intersectGrid = scribe.getShapeGrid(ARCHIVE_USER, 
                                                  shpname=intersectGridName)
      except Exception, e:
         if not isinstance(e, LMError):
            e = LMError(currargs=e.args, lineno=self.getLineno())
         raise e
      
      finally:
         scribe.closeConnections()

# ...............................................
   def _initWorkers(self):
      raise LMError('Must be implemented in data-specific subclass')

# .............................................................................
class GBIFPipeline(LMArchivePipeline):
   """
   The pipeline asks for jobs and then runs them.
   @precondition: Must setAlgorithm and setModelScenario before running 
             start.  May addProjectionScenario one or more times to project
             model onto alternate scenarios (besides the original 
             model scenario).
   """
   def __init__(self, pipelineName, 
                algCodes, mdlScenarioCode, projScenarioCodes, 
                mdlMaskId=None, prjMaskId=None, expDate=None):
      """
      @summary Constructor for the pipeline class
      @param logger: Logger to use for the main thread
      @param processid: process id for this application
      """
      LMArchivePipeline.__init__(self, pipelineName, algCodes, 
                                 mdlScenarioCode, projScenarioCodes, 
                                 mdlMaskId=mdlMaskId, prjMaskId=prjMaskId)
#       excludeList = self._readExcludeFile(MARINE_EXCLUDE_FILE)
      self._initWorkers(expDate)

# ...............................................
   def _readExcludeFile(self, filename):
      """
      Exclude file contains a list of marine names, one per line, each string 
      prefixed by 
      """
      excludeList = set()
      if filename is not None:
         f = open(filename, 'r')
         for line in f:
            elts = line.strip().split()
            for elt in elts:
               subelts = elt.split()
               if len(subelts) > 0 and subelts[0].isalpha():
                  excludeList.add(elt)
                  # Add just the first term (genus) also
                  excludeList.add(subelts[0])
         f.close()
      return excludeList 
   
# ...............................................
   def _initWorkers(self, expDate):
      taxname = TAXONOMIC_SOURCE[DATASOURCE]['name']
      self.workers = []
      updateInterval = ONE_MONTH
      gbifFldNames = []
      idxs = GBIF_EXPORT_FIELDS.keys()
      idxs.sort()
      for idx in idxs:
         gbifFldNames.append(GBIF_EXPORT_FIELDS[idx][0])

      try:
         self.workers.append(ProcessRunner(self.lock, self.name, updateInterval, 
                             processTypes=[ProcessType.SMTP],
                             threadSuffix='_email'))
         self.workers.append(GBIFChainer(self.lock, self.name, updateInterval, 
                             self.algs, self.modelScenario, self.projScenarios, 
                             OCC_DUMP_FILE, expDate, gbifFldNames, 
                             GBIF_TAXONKEY_FIELD, taxname,
                             providerKeyFile=PROVIDER_DUMP_FILE, 
                             providerKeyColname=GBIF_PROVIDER_FIELD,
                             mdlMask=self.modelMask, prjMask=self.projMask,
                             intersectGrid=self.intersectGrid))
         self.workers.append(Infiller(self.lock, self.name, updateInterval, 
                             self.algs, self.modelScenario, self.projScenarios, 
                             mdlMask=self.modelMask, prjMask=self.projMask,
                             intersectGrid=self.intersectGrid))
         self.workers.append(Troubleshooter(self.lock, self.name, updateInterval, 
                             archiveDataDeleteTime=None))
      except LMError, e:
         raise 
      except Exception, e:
         raise LMError('Failed to initialize Workers %s' % str(e))
      else:
         self.ready = True
      
# .............................................................................
class BisonPipeline(LMArchivePipeline):
   """
   The pipeline asks for jobs and then runs them.
   @precondition: Must setAlgorithm and setModelScenario before running 
             start.  May addProjectionScenario one or more times to project
             model onto alternate scenarios (besides the original 
             model scenario).
   """
   def __init__(self, pipelineName, algCodes, mdlScenarioCode, projScenarioCodes, 
                mdlMaskId=None, prjMaskId=None, expDate=None):
      """
      @summary Constructor for the pipeline class
      @param logger: Logger to use for the main thread
      @param processid: process id for this application
      """
      LMArchivePipeline.__init__(self, pipelineName, algCodes, 
                                 mdlScenarioCode, projScenarioCodes, 
                                 mdlMaskId=mdlMaskId, prjMaskId=prjMaskId)
      self._updateTSNFile(BISON_TSN_FILE, expDate)
      self._initWorkers(BISON_TSN_FILE, expDate)

# ...............................................
   def _updateTSNFile(self, filename, expDate):
      """
      If file does not exist or is older than expDate, create a new file. 
      """
      if filename is None or not os.path.exists(filename):
         self._recreateTSNFile(filename)
      else:
         ticktime = os.path.getmtime(filename)
         modtime = mx.DateTime.DateFromTicks(ticktime).mjd
         if modtime < expDate:
            self._recreateTSNFile(filename)
   
# ...............................................
   def _recreateTSNFile(self, filename):
      """
      Create a new file from BISON TSN query for binomials with > 20 points. 
      """
      bisonQuery = BisonAPI(qFilters={BISON_NAME_KEY: BISON_BINOMIAL_REGEX}, 
                            otherFilters=BISON_TSN_FILTERS)
      tsnList = bisonQuery.getBinomialTSNs()
      with open(filename, 'w') as f:
         for tsn, tsnCount in tsnList:
            f.write('%d, %d\n' % (int(tsn), int(tsnCount)))
      

# ...............................................
   def _initWorkers(self, tsnfilename, expDate):
      taxname = TAXONOMIC_SOURCE[DATASOURCE]['name']
      self.workers = []
      updateInterval = ONE_MONTH
      
      try:
         self.workers.append(BisonChainer(self.lock, self.name, updateInterval, 
                              self.algs, self.modelScenario, self.projScenarios, 
                              tsnfilename, expDate, taxname, 
                              mdlMask=self.modelMask, prjMask=self.projMask,
                              intersectGrid=self.intersectGrid))
         self.workers.append(Infiller(self.lock, self.name, updateInterval, 
                              self.algs, self.modelScenario, self.projScenarios, 
                              mdlMask=self.modelMask, prjMask=self.projMask,
                              intersectGrid=self.intersectGrid))
      except LMError, e:
         raise 
      except Exception, e:
         raise LMError('Failed to initialize Workers %s' % str(e))
      else:
         self.ready = True

# .............................................................................
class iDigBioPipeline(LMArchivePipeline):
   """
   The pipeline asks for jobs and then runs them.
   @precondition: Must setAlgorithm and setModelScenario before running 
             start.  May addProjectionScenario one or more times to project
             model onto alternate scenarios (besides the original 
             model scenario).
   """
   def __init__(self, pipelineName, algCodes, mdlScenarioCode, projScenarioCodes, 
                mdlMaskId=None, prjMaskId=None, expDate=None):
      """
      @summary Constructor for the pipeline class
      @param logger: Logger to use for the main thread
      @param processid: process id for this application
      """
      LMArchivePipeline.__init__(self, pipelineName, algCodes, 
                                 mdlScenarioCode, projScenarioCodes, 
                                 mdlMaskId=mdlMaskId, prjMaskId=prjMaskId)
      # Prep in rocks-lifemapper rpm install.
      #  in SPECIES directory:
      #   1) unzip dumped zip file
      #   2) list species directories into txt file with same basename
      self._initWorkers(IDIGBIO_BINOMIAL_FILE, expDate)

# ...............................................
   def _initWorkers(self, binomialfilename, expDate):
      self.workers = []
      updateInterval = ONE_MONTH
      
      try:
         self.workers.append(iDigBioChainer(self.lock, self.name, updateInterval, 
                              self.algs, self.modelScenario, self.projScenarios, 
                              binomialfilename, expDate, 
                              mdlMask=self.modelMask, prjMask=self.projMask,
                              intersectGrid=self.intersectGrid))
         self.workers.append(Infiller(self.lock, self.name, updateInterval, 
                              self.algs, self.modelScenario, self.projScenarios, 
                              mdlMask=self.modelMask, prjMask=self.projMask,
                              intersectGrid=self.intersectGrid))
      except LMError, e:
         raise 
      except Exception, e:
         raise LMError('Failed to initialize Workers %s' % str(e))
      else:
         self.ready = True


# .....................................................
"""
default scenarios and algorithms are set in Constants
 argv[0]: program name
 argv[1]: optional start line in occurrence datafile 
 i.e. python LmDbServer/pipeline/localpipeline.py 5012
"""
def _usage(startline):
   print('Usage:  python localpipeline.py <line number to start at in occurrence datafile>\n')
   print('        python localpipeline.py\n')
   print('\n')

if __name__ == '__main__':
   expdate = mx.DateTime.DateTime(SPECIES_EXP_YEAR, SPECIES_EXP_MONTH, 
                                  SPECIES_EXP_DAY)
   print('Begin Pipeline with algorithms %s, modelScenario %s, projectionScenarios %s' 
         % (str(DEFAULT_ALGORITHMS), DEFAULT_MODEL_SCENARIO, 
            str(DEFAULT_PROJECTION_SCENARIOS)))
   
   if DATASOURCE == Instances.BISON:
      p = BisonPipeline(DATASOURCE.lower(), 
                        DEFAULT_ALGORITHMS, DEFAULT_MODEL_SCENARIO, 
                        DEFAULT_PROJECTION_SCENARIOS, expDate=expdate.mjd)
   elif DATASOURCE == Instances.GBIF:
      p = GBIFPipeline(DATASOURCE.lower(), 
                       DEFAULT_ALGORITHMS, DEFAULT_MODEL_SCENARIO, 
                       DEFAULT_PROJECTION_SCENARIOS, expDate=expdate.mjd)
   elif DATASOURCE == Instances.IDIGBIO:
      p = iDigBioPipeline(DATASOURCE.lower(), 
                       DEFAULT_ALGORITHMS, DEFAULT_MODEL_SCENARIO, 
                       DEFAULT_PROJECTION_SCENARIOS, expDate=expdate.mjd)
   
   killfile = p.getKillfilename(DATASOURCE.lower())
   waitsec = 10
   print('           **************************') 
   print('IMPORTANT: Update expiration date (currently %s) and remove start.%s.txt for new data' 
         % (expdate.strftime(), DATASOURCE.lower()))
   print('           `touch %s` to kill and restart (waiting %d seconds)' 
         % (killfile, waitsec))
   print('           **************************') 
   sleep(waitsec)

   p.run()
      
   