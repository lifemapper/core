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
import json
import numpy
import os
from StringIO import StringIO
import tempfile
from zipfile import ZipFile

from LmCommon.common.lmAttObject import LmAttObj, LmAttList
from LmCommon.common.lmconstants import (InputDataType, JobStage, JobStatus, 
            ProcessType, RandomizeMethods,SHAPEFILE_EXTENSIONS)
from LmCommon.common.lmXml import Element, CDATA, fromstring, deserialize
                                        
from LmServer.base.job import _Job, _JobData
from LmServer.base.lmobj import LMError
from LmServer.common.lmconstants import JobFamily, ReferenceType
from LmServer.common.localconstants import WEBSERVICES_ROOT
from LmServer.rad.matrix import Matrix

# TODO: remove these when they become LmCompute plugins
#from LmServer.rad.radCalculations import calculate#, compress, swap, splotch

# .............................................................................
class RADBuildGridJob(_Job):
   """
   @summary: Job object used to build a new shapegrid
   """
   # ....................................
   software = ProcessType.RAD_BUILDGRID
   stage = JobStage.BUILD_SHAPEGRID

   # ....................................
   def __init__(self, shapegrid, cutoutWKT=None, computeId=None,  
                status=JobStatus.GENERAL, statusModTime=None, 
                priority=None, lastHeartbeat=None, createTime=None, jid=None,
                retryCount=None):
      """
      @summary: Build Shapegrid job constructor
      @param shapegrid: Shapegrid containing parameters for building
      @param cutoutWKT: (optional) Polygon in WKT to cut out
      @copydoc LmServer.base.job._Job::__init__()
      """
      jobData = RADBuildGridJobData(shapegrid, jid, ProcessType.RAD_BUILDGRID, 
                        shapegrid.getUserId(), cutoutWKT=cutoutWKT)
      _Job.__init__(self, jobData, shapegrid, ReferenceType.ShapeGrid, 
                    jobFamily=JobFamily.RAD, computeId=computeId, 
                    status=status, statusModTime=statusModTime, 
                    priority=priority, lastHeartbeat=lastHeartbeat, 
                    createTime=createTime, retryCount=retryCount)
   
   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      if component.lower() == 'package':
         
         sgDLocParts = os.path.split(self._outputObj.getDLocation())
         outDir = sgDLocParts[0]
         sgBase = os.path.splitext(sgDLocParts[1])[0] # Base name for shapegrid files 
         
         
         content = StringIO(content)
         content.seek(0)
         with ZipFile(content, 'r', allowZip64=True) as zf:
            
            zf.extractall(path=outDir)
            
            # We need to fix the filenames to match what we expect
            for zname in zf.namelist():
               name, ext = os.path.splitext(zname)
               if ext in SHAPEFILE_EXTENSIONS:
                  if name.find('shapegrid') >= 0:
                     oldName = os.path.join(outDir, zname)
                     newName = os.path.join(outDir, "%s%s" % (sgBase, ext))
                     os.rename(oldName, newName)

         status = JobStatus.COMPLETE
         self.update(status=status)
      else:
         raise LMError(["Do not know how to write content (%s) for BuildGrid job" % component])

# .............................................................................
#TODO: Finish this for arbitrary layersets or subsets !!
class RADNewIntersectJob(_Job):
   """
   @summary: Job object used to intersect a shapegrid with a set of layers
   @note: bktJob; Operates on a RADExperiment.orgLayerset and Bucket.shapegrid  
          to create the Bucket.fullPAM
   """
   # ....................................
   software = ProcessType.RAD_INTERSECT
   stage=JobStage.INTERSECT

   # ....................................
   def __init__(self, lyrset, shapegrid, outputPath, 
                computeId=None, status=JobStatus.GENERAL, 
                statusModTime=None, priority=None, lastHeartbeat=None, 
                createTime=None, jid=None, retryCount=None):
      """
      @summary: Intersect job constructor
      @param lyrset: MatrixLayerset containing PALayers or 
                     AncillaryLayers or envLayerset; a subset of
                     either of these may be targeted for intersection using 
                     LayerParameters
      @copydoc LmServer.base.job._Job::__init__()
      """
      self.outputPath = outputPath
      shapegrid.initSitesPresent()
      lyrset.initLayersPresent()
      inputType = lyrset.matrixType
      
   # ....................................
   def _readIntersectOutput(self, content):
      layerArrays = {}
      content = StringIO(content)
      content.seek(0)
      with ZipFile(content, 'r', allowZip64=True) as zf:
         try:
            xmlFile = zf.read('layerIndex.xml')
            allLayerOutput = deserialize(fromstring(xmlFile))
         except Exception, e:
            self.log.error("Unable to find or readlayerIndex.xml, %s" % str(e))
            raise e
         
         for lyrOut in allLayerOutput:
            try:
               intersectedLyr = StringIO(zf.read(lyrOut.filename))
               intersectedLyr.seek(0)
               pav = numpy.load(intersectedLyr)
               layerArrays[int(lyrOut.index)] = pav
            except Exception, e:
               self.log.error("Unable to read or load intersected layer %s (%s)" 
                              % (str(lyrOut.filename), str(e)))
               raise e
      return layerArrays      

   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      if component.lower() == 'pam':
         status = self.writeVectors(content)
         self.update(status=status)
      else:
         raise LMError(["Do not know how to write content (%s) for NEW intersect job" % component])

   # ....................................
   def writeVectors(self, content):
      """
      @summary: Writes out the RADBucket's pickeled PAM or GRIM file (numpy format)
      @postcondition: The PAM or GRIM is written to the filesystem
      """
      pass

# .............................................................................
class RADIntersectJob(_Job):
   """
   @summary: Job object used to intersect a shapegrid with a set of layers
   @note: bktJob; Operates on a RADExperiment.orgLayerset and Bucket.shapegrid  
          to create the Bucket.fullPAM
   """
   # ....................................
   software = ProcessType.RAD_INTERSECT
   stage=JobStage.INTERSECT

   # ....................................
   def __init__(self, radexp, doSpecies=True, computeId=None, 
                status=JobStatus.GENERAL, statusModTime=None, priority=None, 
                lastHeartbeat=None, createTime=None, jid=None, retryCount=None):
      """
      @summary: Intersect job constructor
      @param radexp: RADExperiment containing 
                     * a single RADBucket with the shapegrid to intersect  
                     * one or more MatrixLayersets, orgLayerset and/or 
                       envLayerset; either of these may be targeted for 
                       intersection using
      @param doSpecies: if True, intersect the PresenceAbsenceLayers in the 
                        radexp.orgLayerset; if False, intersect AncillaryLayers 
                        in the radexp.envLayerset 
      @copydoc LmServer.base.job._Job::__init__()
      """
      bucket = radexp.bucketList[0]
      self.bucketId = bucket.getId()
      self.outputPath = bucket.outputPath
      self.doSpecies = doSpecies

      if doSpecies:
         layerset = radexp.orgLayerset
         layersPresent = radexp.initOrgLayersPresent()
         sitesPresent = bucket.shapegrid.initSitesPresent()
         bucket.clearPresenceIndices()
         bucket.setSitesPresent(sitesPresent)
         bucket.setLayersPresent(layersPresent)
         inputType = InputDataType.USER_PRESENCE_ABSENCE
      else:
         raise LMError('Ancillary Intersection is not yet supported')
#          layerset = radexp.envLayerset
#          layersPresent = radexp.initEnvLayersPresent()
#          inputType = InputDataType.USER_ANCILLARY

      if status == JobStatus.INITIALIZE:
         # Update layerIndices (for entire experiment)
         radexp.writeLayerIndices()
#          # Initialize layersPresent and sitesPresent for this bucket
#          bucket.writePresenceIndices()

      jobData = RADIntersectJobData(layerset, bucket.shapegrid, jid, 
                                    radexp.getUserId(), bucket.metadataUrl, 
                                    radexp.metadataUrl, inputType=inputType)
      _Job.__init__(self, jobData, bucket, ReferenceType.Bucket, 
                       jobFamily=JobFamily.RAD, computeId=computeId, 
                       status=status, statusModTime=statusModTime, 
                       priority=priority, lastHeartbeat=lastHeartbeat, 
                       createTime=createTime, retryCount=retryCount)
#    
#    # ....................................
#    def run(self):
#       status, lyrArrays = intersect(self.dataObj['layerset'], 
#                                     self.dataObj['shapegrid'])
#       return [status, lyrArrays]
      
   # ....................................
   def _readIntersectOutput(self, content):
      layerArrays = {}
      content = StringIO(content)
      content.seek(0)
      with ZipFile(content, 'r', allowZip64=True) as zf:
         try:
            xmlFile = zf.read('layerIndex.xml')
            allLayerOutput = deserialize(fromstring(xmlFile))
         except Exception, e:
            self.log.error("Unable to find or readlayerIndex.xml, %s" % str(e))
            raise e
         
         for lyrOut in allLayerOutput:
            try:
               intersectedLyr = StringIO(zf.read(lyrOut.filename))
               intersectedLyr.seek(0)
               pav = numpy.load(intersectedLyr)
               layerArrays[int(lyrOut.index)] = pav
            except Exception, e:
               self.log.error("Unable to read or load intersected layer %s (%s)" 
                              % (str(lyrOut.filename), str(e)))
               raise e
      return layerArrays      

   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      if component.lower() == 'pam':
         status = self.writeMatrix(content)
         self.update(status=status)
      else:
         raise LMError(["Do not know how to write content (%s) for intersect job" % component])

   # ....................................
   def writeMatrix(self, content):
      """
      @summary: Writes out the RADBucket's pickeled PAM or GRIM file (numpy format)
      @postcondition: The PAM or GRIM is written to the filesystem
      """
      layerArrays = self._readIntersectOutput(content)
      
      print "Writing intersection for radBucket %s" % self._outputObj.getId()
      if len(layerArrays) > 0:
         idx1 = layerArrays.keys()[0]
         sitecount = len(layerArrays[idx1])
         
      if len(layerArrays) > 0:
         mtx = Matrix.initEmpty(sitecount, len(layerArrays))
         for mtxidx, lyrdata in layerArrays.iteritems():
            mtx.addColumn(lyrdata, mtxidx)
         if self.doSpecies:
            self._outputObj.setFullPAM(mtx)
         else:         
            self._outputObj.setFullGRIM(mtx)

      try:
         if self.doSpecies:
            # Can write each individual layer here too
            self._outputObj.writePam()
         else:            
            self._outputObj.writeGrim()
         
         status = JobStatus.COMPLETE
            
      except Exception, e:
         status = JobStatus.IO_MATRIX_WRITE_ERROR

      return status

# .............................................................................
class RADCompressJob(_Job):
   """
   @summary: Job object used to Compress a Bucket full (sparse) matrix of sites 
             intersected with layers
   @note: opsJob, rpsJob; 
          Operates on a:
             Bucket.fullPAM to create an Original PamSum._pam;
             Randomized PamSum._splotchPam to create its PamSum._pam 
   """
   # ....................................
   software = ProcessType.RAD_COMPRESS
   stage = JobStage.COMPRESS

   # ....................................
   def __init__(self, radexp, pamsum, computeId=None,  
                status=JobStatus.GENERAL, statusModTime=None, 
                priority=None, lastHeartbeat=None, createTime=None, jid=None,
                retryCount=None):
      """
      @summary: Compress job constructor
      @param radexp: RADExperiment containing the bucket of work, with 
                     fullPAM Matrix
      @param pamsum: PamSum to accept the compressed PAM
      @copydoc LmServer.base.job._Job::__init__()
      """
      self._bucket = radexp.bucketList[0]
      self._bucket.setPath()
      pamsum.outputPath = self._bucket.outputPath
      self.bucketId = self._bucket.getId()
      sitesPresent = self._bucket.getCleanSitesPresent()
      layersPresent = self._bucket.getCleanLayersPresent()      
      if pamsum.randomMethod == RandomizeMethods.NOT_RANDOM:
         objType = ReferenceType.OriginalPamSum
         # Does not exist until after Intersect has succeeded 
         # and status == PULL_REQUESTED
         if status == JobStatus.PULL_REQUESTED:
            self._bucket.readPAM()
         mtx = self._bucket.getFullPAM()
      else:
         objType = ReferenceType.RandomPamSum
         if status == JobStatus.PULL_REQUESTED:
            pamsum.readSplotchPAM()
         mtx = pamsum.getSplotchPAM()
      jobData = RADMatrixJobData(mtx, sitesPresent, layersPresent, jid, 
                                 ProcessType.RAD_COMPRESS, radexp.getUserId(),
                                 self._bucket.metadataUrl, radexp.metadataUrl)
      _Job.__init__(self, jobData, pamsum, objType, jobFamily=JobFamily.RAD, 
                    computeId=computeId, 
                    status=status, statusModTime=statusModTime, 
                    priority=priority, lastHeartbeat=lastHeartbeat, 
                    createTime=createTime, retryCount=retryCount)

#    # ....................................
#    def run(self):
#       status, cmpMtx, sitesPresent, layersPresent = \
#             compress(self.dataObj['matrix'], self.dataObj['sitesPresent'],
#                      self.dataObj['layersPresent'])
#       return [status, cmpMtx, sitesPresent, layersPresent]
   
   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      sitesPresent = {}
      layersPresent = {}
      
      if component.lower() == 'package':
         content = StringIO(content)
         content.seek(0)
         with ZipFile(content, 'r', allowZip64=True) as zf:
            # Sites present
            try:
               spXml = zf.read('sitesPresent.xml')
               spObj = deserialize(fromstring(spXml))
               # Process each site
               for site in spObj.site:
                  sitesPresent[int(site.id)] = bool(site.value.strip().lower() == 'true')
            except Exception, e:
               self.log.error("Unable to find or read sitesPresent.xml, %s" % str(e))
               raise e
            
            # Layers present
            try:
               lpXml = zf.read('layersPresent.xml')
               lpObj = deserialize(fromstring(lpXml))
               # Process each layer
               for lyr in lpObj.layer:
                  layersPresent[int(lyr.id)] = bool(lyr.value.strip().lower() == 'true')
            except Exception, e:
               self.log.error("Unable to find or read layersPresent.xml, %s" % str(e))
               raise e
            
            # Compressed matrix
            try:
               _, tmpFn = tempfile.mkstemp(suffix='.npy')
               with open(tmpFn, 'wb') as f:
                  f.write(zf.read('pam.npy'))
               mtx = numpy.load(tmpFn)
            except Exception, e:
               self.log.error("Unable to find or process pam.npy, %s" % str(e))
               raise e

         status = self.writeCompressData(mtx, sitesPresent, layersPresent)
         self.update(status=status)   
      
      else:
         raise LMError(["Do not know how to write content (%s) for splotch job" % component])

   # ....................................
   def writeCompressData(self, cmpMtx, sPrsnt, lPrsnt):
      """
      @summary: Writes out the RADBucket's pickled compressed PAM (numpy format)
      @postcondition: The compressed PAM is written to the filesystem
      """
      status = JobStatus.COMPLETE
      compressedPam = Matrix(cmpMtx, isCompressed=True)
      self._outputObj.setPam(compressedPam)
      rndMethod = self._outputObj.randomMethod
      
      if rndMethod == RandomizeMethods.NOT_RANDOM:
         self._bucket.clearPresenceIndices()
         self._bucket.setSitesPresent(sPrsnt)
         self._bucket.setLayersPresent(lPrsnt)
         
      elif rndMethod == RandomizeMethods.SPLOTCH:
         self._outputObj.clearSplotchSitesPresent()
         self._outputObj.setSplotchSitesPresent(sPrsnt)
      
      try:
         self._outputObj.writePam()            
      except Exception, e:
         self.log("Failed to write pam: %s" % str(e))
         status = JobStatus.IO_MATRIX_WRITE_ERROR
      else:
         status = JobStatus.COMPLETE
         
         try:
            if rndMethod == RandomizeMethods.NOT_RANDOM:
               self._bucket.writePresenceIndices()
               
            elif rndMethod == RandomizeMethods.SPLOTCH:
               self._outputObj.writeSplotchSites()
               
         except Exception, e:
            self.log("IO Indices write error: %s" % str(e))
            status = JobStatus.IO_INDICES_WRITE_ERROR
      
      return status
      
# .............................................................................
class RADSwapJob(_Job):
   """
   @summary: Job object used to randomize compressed Original PAM. 
   @note: rpsJob; Operates on the Original PamSum._pam to create a 
                  Randomized PamSum._pam 
   """
   # ....................................
   software = ProcessType.RAD_SWAP
   stage = JobStage.SWAP
   
   # ....................................
   def __init__(self, radexp, rndPamsum, numSwaps=50, computeId=None,  
                status=JobStatus.GENERAL, statusModTime=None, 
                priority=None, lastHeartbeat=None, createTime=None, jid=None,
                retryCount=None):
      """
      @summary: Randomize job constructor
      @param radexp: RADExperiment containing the bucket of work, with 
                     original PamSum with compressed PAM Matrix
      @param rndPamsum: empty PamSum to accept the swapped PAM
      @copydoc LmServer.base.job._Job::__init__()
      """
      bucket = radexp.bucketList[0]
      self.bucketId = bucket.getId()
      sitesPresent = bucket.getSitesPresent()
      layersPresent = bucket.getLayersPresent()
      mtx = None
      if status == JobStatus.PULL_REQUESTED:
         if bucket.pamSum is not None:
            # Read original PAM (compressed)
            bucket.pamSum.readPAM()
            mtx = bucket.pamSum.getPam()
         else:
            raise LMError('Original PamSum is not populated')
      
      # Overwrite numSwaps if parameter is on random PamSum
      if rndPamsum.getRandomParameter('numSwaps') is not None:
         numSwaps = rndPamsum.getRandomParameter('numSwaps')
      elif rndPamsum.getRandomParameter('iterations') is not None:
         numSwaps = rndPamsum.getRandomParameter('iterations')
      
      jobData = RADMatrixJobData(mtx, sitesPresent, layersPresent, 
                                 jid, ProcessType.RAD_SWAP, radexp.getUserId(),
                                 '%s/pamsums/original' % bucket.metadataUrl, 
                                 radexp.metadataUrl, numSwaps=numSwaps)
      _Job.__init__(self, jobData, rndPamsum, ReferenceType.RandomPamSum, 
                    jobFamily=JobFamily.RAD, computeId=computeId, 
                    status=status, statusModTime=statusModTime, 
                    priority=priority, lastHeartbeat=lastHeartbeat, 
                    createTime=createTime, retryCount=retryCount)
         
#    # ....................................
#    def run(self): 
#       status, rndMatrix, counter = swap(self.dataObj['matrix'], 
#                                         self.dataObj['sitesPresent'],
#                                         self.dataObj['layersPresent'], 
#                                         self.dataObj['iterations'])
#       return [status, rndMatrix, counter]

   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      if component.lower() == 'matrix':
         _, tmpFn = tempfile.mkstemp(suffix='.npy')
         with open(tmpFn, 'wb') as f:
            f.write(content)
         matrix = numpy.load(tmpFn)
         # CJ will add rndparameters in later
         #       rndparams = {'numberOfSwaps': counter, 
         #                    'numberOfIterations': self.dataObj['iterations']}
         swappedPam = Matrix(matrix, isCompressed=True)#, randomParameters=rndparams)
         self._outputObj.setPam(swappedPam)
         self._outputObj.writePam()        
         self.update(status=JobStatus.COMPLETE)          
      else:
         raise LMError(["Do not know how to write content (%s) for swap job" % component])

   # ....................................
   #def writeMatrix(self, rndMatrix):
   #   """
   #   @summary: Writes out a PamSum's randomized, pickeled PAM file (numpy 
   #             format)
   #   @postcondition: The PAM is written to the filesystem
   #   """
      #except Exception, e:
      #   status = JobStatus.IO_MATRIX_WRITE_ERROR
      #else:         
      #   status = JobStatus.COMPLETE
      #return status

# .............................................................................
class RADSplotchJob(_Job):
   """
   @summary: Job object used to randomize a PAM.  
   @note: rpsJob; Operates on a Bucket.fullPAM to create a Randomized PamSum._splotchPam
   """
   # ....................................
   software = ProcessType.RAD_SPLOTCH
   stage = JobStage.SPLOTCH

   # ....................................
   def __init__(self, radexp, rndPamsum, computeId=None, 
                status=JobStatus.GENERAL, statusModTime=None, 
                priority=None, lastHeartbeat=None, createTime=None, jid=None,
                retryCount=None):
      """
      @summary: Randomize job constructor
      @param radexp: RADExperiment containing the bucket of work, with 
                     fullPAM Matrix
      @param rndPamsum: empty PamSum to accept the Splotched PAM
      @copydoc LmServer.base.job._Job::__init__()
      """
      bucket = radexp.bucketList[0]
      self.bucketId = bucket.getId()
      sitesPresent = bucket.getCleanSitesPresent()
      layersPresent = bucket.getCleanLayersPresent()
      if status == JobStatus.PULL_REQUESTED:
         bucket.readPAM()
      mtx = bucket.getFullPAM()   
      jobData = RADMatrixJobData(mtx, sitesPresent, layersPresent, jid, 
                                 ProcessType.RAD_SPLOTCH, radexp.getUserId(),
                                 '%s/pamsums/original' % bucket.metadataUrl, radexp.metadataUrl,
                                 shapegrid=bucket.shapegrid)
      _Job.__init__(self, jobData, rndPamsum, ReferenceType.RandomPamSum, 
                    jobFamily=JobFamily.RAD, computeId=computeId, 
                    status=status, statusModTime=statusModTime, 
                    priority=priority, lastHeartbeat=lastHeartbeat, 
                    createTime=createTime, retryCount=retryCount)

#    # ....................................
#    def run(self): 
#       status, splotchedMtx = splotch(self.dataObj['matrix'], 
#                                      self.dataObj['shapegrid'], 
#                                      len(self.dataObj['sitesPresent']), 
#                                      self.dataObj['layersPresent'])
#       return [status, splotchedMtx]

   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      if component.lower() == 'matrix':
         _, tmpFn = tempfile.mkstemp(suffix='.npy')
         with open(tmpFn, 'wb') as f:
            f.write(content)
         matrix = numpy.load(tmpFn)
         
         splotchedPam = Matrix(matrix, isCompressed=False)
         self._outputObj.setSplotchPAM(splotchedPam)
         try:
            self._outputObj.writeSplotch()
         except Exception, e:
            status = JobStatus.IO_MATRIX_WRITE_ERROR
         else:         
            status = JobStatus.COMPLETE
         self.update(status=status)
      else:
         raise LMError(["Do not know how to write content (%s) for splotch job" % component])

# .............................................................................
class RADGradyJob(_Job):
   """
   @summary: Job object used to randomize compressed Original PAM. 
   @note: rpsJob; Operates on the Original PamSum._pam to create a 
                  Randomized PamSum._pam 
   """
   # ....................................
   software = ProcessType.RAD_GRADY
   stage = JobStage.GRADY_FILL
   
   # ....................................
   def __init__(self, radexp, rndPamsum, computeId=None,  
                status=JobStatus.GENERAL, statusModTime=None, 
                priority=None, lastHeartbeat=None, createTime=None, jid=None,
                retryCount=None):
      """
      @summary: Randomize job constructor
      @param radexp: RADExperiment containing the bucket of work, with 
                     original PamSum with compressed PAM Matrix
      @param rndPamsum: empty PamSum to accept the swapped PAM
      @copydoc LmServer.base.job._Job::__init__()
      """
      bucket = radexp.bucketList[0]
      self.bucketId = bucket.getId()
      sitesPresent = bucket.getSitesPresent()
      layersPresent = bucket.getLayersPresent()
      mtx = None
      if status == JobStatus.PULL_REQUESTED:
         if bucket.pamSum is not None:
            # Read original PAM (compressed)
            bucket.pamSum.readPAM()
            mtx = bucket.pamSum.getPam()
         else:
            raise LMError('Original PamSum is not populated')
      
      jobData = RADMatrixJobData(mtx, sitesPresent, layersPresent, 
                                 jid, ProcessType.RAD_GRADY, radexp.getUserId(),
                                 '%s/pamsums/original' % bucket.metadataUrl, 
                                 radexp.metadataUrl)
      _Job.__init__(self, jobData, rndPamsum, ReferenceType.RandomPamSum, 
                    jobFamily=JobFamily.RAD, computeId=computeId, 
                    status=status, statusModTime=statusModTime, 
                    priority=priority, lastHeartbeat=lastHeartbeat, 
                    createTime=createTime, retryCount=retryCount)
         
   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      if component.lower() == 'matrix':
         _, tmpFn = tempfile.mkstemp(suffix='.npy')
         with open(tmpFn, 'wb') as f:
            f.write(content)
         matrix = numpy.load(tmpFn)
         swappedPam = Matrix(matrix, isCompressed=True)
         self._outputObj.setPam(swappedPam)
         self._outputObj.writePam()        
         self.update(status=JobStatus.COMPLETE)          
      else:
         raise LMError(["Do not know how to write content (%s) for grady job" % component])

# .............................................................................
class RADCalculateJob(_Job):
   """
   @summary: Job object used to Calculate PamSum summary statistics and 
             indices from the Original or one Randomized PamSum._pam 
             (compressed PAM)
   @note: opsJob, rpsJob; Operates on an Original or Randomized PamSum._pam to 
             create its PamSum.sum
   """
   # ....................................
   software = ProcessType.RAD_CALCULATE
   stage = JobStage.CALCULATE

   # ....................................
   def __init__(self, radexp, pamsum, computeId=None, 
                status=JobStatus.GENERAL, statusModTime=None, priority=None,  
                lastHeartbeat=None, createTime=None, jid=None, retryCount=None):
      """
      @summary: RAD Calculate (Statistics for a compressed PAM) job constructor
      @param radexp: RADExperiment containing the bucket of work, with 
                     fullPAM Matrix
      @param pamsum: PamSum with compressed PAM to accept the calculated SUM;
                     this could be the original PamSum or a randomized one
      @copydoc LmServer.base.job._Job::__init__()
      """
      bucket = radexp.bucketList[0]
      self.bucketId = bucket.getId()
      sitesPresent = bucket.getSitesPresent()
      layersPresent = bucket.getLayersPresent()
      mtx = pamsum.getPam()
      if pamsum.randomMethod == RandomizeMethods.NOT_RANDOM:
         objType = ReferenceType.OriginalPamSum
      else:
         objType = ReferenceType.RandomPamSum
         if pamsum.randomMethod == RandomizeMethods.SPLOTCH:
            sitesPresent = pamsum.getSplotchSitesPresent()
            
      if os.path.exists(radexp.attrTreeDLocation):
         # Read tree and look for length
         treeData = open(radexp.attrTreeDLocation).read()
         try:
            tree = json.loads(treeData)
            l = tree['children'][0]['length'] # If the first child has a length, assume that they all do and do the statistics
            doTaxDist = True
         except Exception, e: # If there was a problem, assume that we can't do the stats
            from LmServer.common.log import LmPublicLogger
            log = LmPublicLogger()
            log.debug(str(e))
            log.debug(treeData)
            log.debug(str(tree))
            treeData = None
            doTaxDist = False
      else:
         treeData = None
         doTaxDist = False
            

      jobData = RADMatrixJobData(mtx, sitesPresent, layersPresent, jid, 
                                 ProcessType.RAD_CALCULATE, radexp.getUserId(),
                                 pamsum.metadataUrl, radexp.metadataUrl,
                                 shapegrid=bucket.shapegrid, treeData=treeData, 
                                 taxDist=doTaxDist, covMatrix=False, Schluter=False)
      _Job.__init__(self, jobData, pamsum, objType, jobFamily=JobFamily.RAD, 
                    computeId=computeId, 
                    status=status, statusModTime=statusModTime, 
                    priority=priority, lastHeartbeat=lastHeartbeat, 
                    createTime=createTime, retryCount=retryCount)
   
#    # ....................................
#    def run(self): 
#       status, summaryData, shapefile = calculate(self.dataObj['matrix'], 
#                                                  self.dataObj['shapegrid'],
#                                                  self.dataObj['sitesPresent'],
#                                                  covMatrix=self.dataObj['covMatrix'],
#                                                  Schluter=self.dataObj['Schluter'])
#       return [status, summaryData, shapefile]
#    
   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      
      # Add tree stats
      
      
      if component.lower() == 'package':
         # Get files from zip file
         cnt = StringIO()
         cnt.write(content)
         cnt.seek(0)
         with ZipFile(cnt, allowZip64=True) as zf:
            alpha = numpy.loadtxt(zf.open('speciesRichness-perSite.npy'))
            phiavgprop = numpy.loadtxt(zf.open('MeanProportionalRangeSize.npy'))
            alphaprop = numpy.loadtxt(zf.open('ProportionalSpeciesDiversity.npy'))
            phi = numpy.loadtxt(zf.open('Per-siteRangeSizeofaLocality.npy'))
            omega = numpy.loadtxt(zf.open('RangeSize-perSpecies.npy'))
            psiavgprop = numpy.loadtxt(zf.open('MeanProportionalSpeciesDiversity.npy'))
            omegaprop = numpy.loadtxt(zf.open('ProportionalRangeSize.npy'))
            psi = numpy.loadtxt(zf.open('Range-richnessofaSpecies.npy'))
            
            try:
               SigmaSites = numpy.loadtxt(zf.open('SigmaSites.npy'))
               SigmaSpecies = numpy.loadtxt(zf.open('SigmaSpecies.npy'))
            except:
               SigmaSites = None
               SigmaSpecies = None
            
            statsObj = deserialize(fromstring(zf.open('statistics.xml').read()))
            
            # Note: I had to use __getattribute__ because the attribute names 
            #    had '-' in them.  I should probably escape that character or 
            #    have another mechanism for getting those variables.
            
            # Try Schluter
            try:
               Vsites = float(statsObj.Schluter.__getattribute__('Species-RangesCovariance'))
               Vsps = float(statsObj.Schluter.__getattribute__('Sites-CompositionCovariance'))
            except:
               Vsites = None
               Vsps = None

            # Try diversity
            try:
               WhittakersBeta = float(statsObj.diversity.__getattribute__('WhittakersBeta'))
               LAdditiveBeta = float(statsObj.diversity.__getattribute__('LAdditiveBeta'))
               LegendreBeta = float(statsObj.diversity.__getattribute__('LegendreBeta'))
            except:
               WhittakersBeta = None
               LAdditiveBeta = None
               LegendreBeta = None
            
            # Try tree
            try:
               mntd = numpy.loadtxt(zf.open('MNTD.npy'))
               pearsonsTdSs = numpy.loadtxt(zf.open('PearsonsOfTDandSitesShared.npy'))
               avgTd = numpy.loadtxt(zf.open('AverageTaxonDistance.npy'))
            except:
               mntd = None
               pearsonsTdSs = None
               avgTd = None
         
         # Create summary data object
         summaryData = {}
         sites = {}
         species = {}
         diversity = {}
         matrices = {}
         Schluters = {}
         sites['speciesRichness-perSite'] = alpha         
         sites['MeanProportionalRangeSize'] = phiavgprop
         sites['ProportionalSpeciesDiversity'] = alphaprop
         sites['Per-siteRangeSizeofaLocality'] = phi
         # Tree stats
         sites['MNTD'] = mntd
         sites['PearsonsOfTDandSitesShared'] = pearsonsTdSs
         sites['AverageTaxonDistance'] = avgTd
         
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

         # Set the sum
         self._outputObj.setSum(summaryData)
         self._outputObj.writeSum(overwrite=True)
         self.update(status=JobStatus.COMPLETE)
         

#    # ....................................
#    def write(self, status, summaryData, zipShapedata):
#       """
#       @summary: Writes out a Pam's summary
#       @postcondition: The PAM is written to the filesystem
#       """
#       if status == JobStatus.COMPLETE:
#          self._outputObj.setSum(summaryData)
#          try:
#             self._outputObj.writeSum(overwrite=True)
#             status = JobStatus.COMPLETE
#          
#          except Exception, e:
#             status = JobStatus.IO_MATRIX_WRITE_ERROR
#                         
#          # TODO: add this back in
# #          try:
# #             self._outputObj.writeSummaryShapefileFromZipdata(zipShapedata, 
# #                                           self.dataObj['shapegrid']['epsgcode'])
# #          except Exception, e:
# #             print('Error writing summary shapefile')
#          
#       return status

# .............................................................................
# .............................................................................
class RADIntersectJobData(_JobData):
   """
   @note: Used for RAD Intersect; need layerset, shapegrid
   """
   # ....................................
   def __init__(self, layerset, shapegrid, jid, userid, 
                bucketUrl, experimentUrl, inputType=None):
      """
      @summary: Constructs a _JobData object for intersecting a layerset
      @param layerset: set of PresenceAbsence or Ancillary layers to be 
                       intersected
      @param shapegrid: Grid against which to intersect all layers 
      @param jid: JobId, primary key in the LMJob database table
      @param userid: User requesting this job
      @param bucketUrl: Metadata URL for the resulting object
      @param experimentUrl: Metadata URL for the parent experiment
      """
      layers = {}
      shapegridVals = {
                       'dlocation': shapegrid.getDLocation(),
                       'localIdIdx': shapegrid.getLocalIdIndex(),
                       'identifier': shapegrid.verify,
                       'shapegridUrl' : "%s/shapefile" % shapegrid.metadataUrl
                      }

      for lyr in layerset.layers:
         lyrVals = {'dlocation': lyr.getDLocation()}
         try:
            lyrVals['isRaster'] = lyr.gdalType is not None
            lyrVals['resolution'] = lyr.resolution
            lyrVals['identifier'] = lyr.verify
            lyrVals['layerUrl'] = "%s/GTiff" % lyr.metadataUrl
         except:
            lyrVals['isRaster'] = False
            lyrVals['identifier'] = lyr.verify
            lyrVals['layerUrl'] = "%s/shapefile" % lyr.metadataUrl
                        
         try:
            lyrVals['isOrganism'] = True
            lyrVals['attrPresence'] = lyr.attrPresence
            lyrVals['minPresence'] = lyr.minPresence
            lyrVals['maxPresence'] = lyr.maxPresence
            lyrVals['percentPresence'] = lyr.percentPresence
            lyrVals['attrAbsence'] = lyr.attrAbsence
            lyrVals['minAbsence'] = lyr.minAbsence
            lyrVals['maxAbsence'] = lyr.maxAbsence
            lyrVals['percentAbsence'] = lyr.percentAbsence
         except:
            try:
               lyrVals['isOrganism'] = False
               lyrVals['attrValue'] = lyr.attrValue
               lyrVals['weightedMean'] = lyr.weightedMean
               lyrVals['largestClass'] = lyr.largestClass
               lyrVals['minPercent'] = lyr.minPercent
            except:
               raise Exception('Must provide PA or Anc Layer')
         layers[lyr.getMatrixIndex()] = lyrVals
         
      obj = {'layerset': layers, 'shapegrid': shapegridVals}
      
      _JobData.__init__(self, obj, jid, ProcessType.RAD_INTERSECT, userid,
                        bucketUrl, experimentUrl, inputType=inputType)

# .............................................................................
class RADBuildGridJobData(_JobData):
   """
   @note: Uses ShapeGrid 
   """
   # ....................................
   def __init__(self, shapegrid, jid, processType, userId, cutoutWKT=None):
      """
      @summary: Constructs a _JobData object for building a shapegrid
      @param shapegrid: The shapegrid object to build
      @param jid: JobId, primary key in the LMJob database table
      @param processType: Type of calculation to be performed (software required)
      @param userId: User requesting this job
      @param cutoutWKT: (optional) Polygon (in Well-Known Text) to cut out
      
      @note: If the cutout is stored on the shapegrid, take out that parameter
      """
      self.minX = shapegrid.minX
      self.minY = shapegrid.minY
      self.maxX = shapegrid.maxX
      self.maxY = shapegrid.maxY
      self.cellSize = shapegrid.cellsize
      self.epsgCode = shapegrid.epsgcode()
      self.cellSides = shapegrid.cellsides
      self.siteId = shapegrid.siteId
      self.siteX = shapegrid.siteX
      self.siteY = shapegrid.siteY
      self.cutoutWKT = cutoutWKT
      # Add post processing element to post back to job server
      self.postProcessing = LmAttObj(name="postProcessing")
      self.postProcessing.post = LmAttObj(name="post")
      self.postProcessing.post.jobServer = "%s/jobs" % WEBSERVICES_ROOT
      _JobData.__init__(self, {}, jid, processType, userId, 
                        shapegrid.metadataUrl(), None)
      
# .............................................................................
class RADMatrixJobData(_JobData):
   """
   @note: Uses Full or Compressed PAM (Matrix)
   """
   # ....................................
   def __init__(self, matrix, sitesPresent, layersPresent, jid, 
                processtype, userid, pamsumUrl, experimentUrl,
                shapegrid=None, randomMethod=None, numSwaps=None, 
                treeData=None, taxDist=None, covMatrix=None, Schluter=None):
      """
      @summary: Constructs a _JobData object for working with a PAM
      @param matrix: Matrix to be operated upon
      @param sitesPresent: dictionary indicating which sites are present in a 
                           compressed PAM; sites are identified by Feature ID;
                           if uncompressed, all sites are intialized to True
      @param layersPresent: dictionary indicating which layers are present in a 
                            compressed PAM; layers are identified by their index  
                            in the matrix (matrixId); if uncompressed, all 
                             layers are intialized to True
      @param jid: JobId, primary key in the LMJob database table
      @param processtype: Type of calculation to be performed (software required)
      @param userid: User requesting this job
      @param pamsumUrl: metadata URL for the resulting object
      @param experimentUrl: metadata URL for the parent experiment
      @param shapegrid: shapegrid (used only in Splotch)
      @param numSwaps: number of successful swaps to perform (used only in Swap)
      @param treeData: Tree data for the experiment (only used in Calculate)
      @param taxDist: boolean indicating whether to create taxonomic distance 
                         stats (only used in Calculate)
      @param covMatrix: boolean indicating whether to create a covariance Matrix
                        (used only in Calculate)
      @param Schluter: boolean indicating whether to calculate Schluter index
                        (used only in Calculate)
      @todo: Remove matrix once everything is using csv
      """
      
      # Matrix
      self.matrix = LmAttObj(name='matrix')
      self.matrix.url = "%s/csv" % pamsumUrl

      # Shapegrid
      if processtype in [ProcessType.RAD_SPLOTCH, ProcessType.RAD_CALCULATE]:
         self.shapegrid = LmAttObj(name="shapegrid")
         self.shapegrid.shapegridUrl = "%s/shapefile" % shapegrid.metadataUrl
         self.shapegrid.identifier = shapegrid.verify
         self.shapegrid.localIdIndex = shapegrid.getLocalIdIndex()
         self.shapegrid.cellSides = shapegrid.cellsides
         
      # Sites present
      if processtype in [ProcessType.RAD_SPLOTCH, ProcessType.RAD_CALCULATE, ProcessType.RAD_COMPRESS]:
         self.sitesPresent = LmAttObj(name="sitesPresent")
         self.sitesPresent.sitesPresent = LmAttList(name="sitesPresent")
         for k in sorted(sitesPresent.keys()):
            self.sitesPresent.sitesPresent.append(
               LmAttObj(name="site", 
                        attrib={'key': str(k), 'present': str(sitesPresent[k])}))
      
      # Layers present
      if processtype in [ProcessType.RAD_SPLOTCH]:
         self.layersPresent = LmAttObj(name="layersPresent")
         self.layersPresent.layersPresent = LmAttList(name="layersPresent")
         for k in sorted(layersPresent.keys()):
            self.layersPresent.layersPresent.append(
               LmAttObj(name="layer",
                        attrib={'key': str(k), 'present': str(layersPresent[k])}))
      
      # Number of swaps
      if processtype == ProcessType.RAD_SWAP:
         self.numSwaps = numSwaps
      
      # Covariance Matrix
      # Schluter
      if processtype == ProcessType.RAD_CALCULATE:
         
         if treeData is not None:
            el = Element("treeData")
            el.append(CDATA(treeData))
            self.treeData = el
            
         self.doTaxonDistance = taxDist
         
         self.doCovarianceMatrix = covMatrix
         self.doSchluter = Schluter
         

      # Add post processing element to post back to job server
      self.postProcessing = LmAttObj(name="postProcessing")
      self.postProcessing.post = LmAttObj(name="post")
      self.postProcessing.post.jobServer = "%s/jobs" % WEBSERVICES_ROOT
      
      
      _JobData.__init__(self, {}, jid, processtype, userid, 
                        pamsumUrl, experimentUrl)

