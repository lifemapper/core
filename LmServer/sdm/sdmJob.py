"""
@summary: Module containing Ecological Niche Model job parent classes
@author: CJ Grady
@contact: cjgrady@ku.edu
@version: 0.1
@status: alpha

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

@todo: Evaluate if these classes can be instantiated or not.  Disallowing for
          now, but it is possible that this is not necessary since no 
          additional input is needed for the subclasses at this time.
"""
import os
from StringIO import StringIO
import subprocess
from zipfile import ZipFile

from LmCommon.common.lmconstants import (JobStatus, JobStage, ProcessType, 
                                        SHAPEFILE_EXTENSIONS, OutputFormat)

from LmServer.base.job import _Job, _JobData
from LmServer.base.lmobj import LMError, LMMissingDataError
from LmServer.common.lmconstants import JobFamily, ReferenceType
from LmServer.common.localconstants import POINT_COUNT_MAX, ARCHIVE_USER

# .............................................................................
class SDMOccurrenceJob(_Job):
   # ....................................
   stage=JobStage.OCCURRENCE
   # Models do not depend on prior calculations 
#    priorMDLStage = []
   # ....................................
   def __init__(self, occSet, processType=ProcessType.GBIF_TAXA_OCCURRENCE, 
                computeId=None, status=None, statusModTime=None, 
                priority=None, lastHeartbeat=None, createTime=None, 
                jid=None, retryCount=None):
      """
      @summary: Model job constructor
      @param model: Lifemapper model object that contains parameters for the
                       model to be generated.
      @copydoc LmServer.base.job._Job::__init__()
      """
      jobData = SDMOccurrenceJobData(occSet, jid, processType)
      _Job.__init__(self, jobData, occSet, ReferenceType.OccurrenceSet, 
                       jobFamily=JobFamily.SDM, computeId=computeId, 
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
         self.writePackage(content)
         
      # These methods are not currently used.  I don't remember if we want to transition to or from them
      #elif component.lower() == 'shapefile':
      #   self.writePoints(content)
      #elif component.lower() == 'subset':
      #   self.writePoints(content, subset=True)
      else:
         raise LMError(["Do not know how to write content (%s) for occurrence set job" % component])

   # ....................................
   def writePackage(self, pointData):
      """
      @summary: This function is a replacement for the old writePackage 
                   function.  We decided that we can trust the data coming back 
                   and just write it directly to its final location.  This also 
                   allows us to get around a bug in Python that is thrown for 
                   very large file sizes in zip files.
      """
      try:
         occ = self._outputObj
         dLocParts = os.path.split(occ.getDLocation())
         subsetDLocParts = os.path.split(occ.getDLocation(subset=True))
         
         # Get path parts
         outDir = dLocParts[0] # Where all of the files will be extracted
         ptBase = os.path.splitext(dLocParts[1])[0] # Gets the base name for the points
         subsetBase = os.path.splitext(subsetDLocParts[1])[0] # Gets the subset base
         
         z = ZipFile(StringIO(pointData), allowZip64=True)
         
         # Extract all files to output directory
         #   Note: We are doing this because we trust the source
         #          and Python 2.7.4 has better security for it
         z.extractall(path=outDir)
         
         # We need to fix the filenames to match what we expect
         for zname in z.namelist():
            name, ext = os.path.splitext(zname)
            if ext in SHAPEFILE_EXTENSIONS:
               if name.find('points') >= 0:
                  oldName = os.path.join(outDir, zname)
                  newName = os.path.join(outDir, "%s%s" % (ptBase, ext))
                  os.rename(oldName, newName)
               elif name.find('subset') >= 0:
                  oldName = os.path.join(outDir, zname)
                  newName = os.path.join(outDir, "%s%s" % (subsetBase, ext))
                  os.rename(oldName, newName)
      except LMError, e:
         self.update(status=JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR)
         raise e
      except Exception, e:
         self.update(status=JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR)
         raise LMError('Error writeShapefile (%s)' % str(e))
         
      # Build indices on shapefile for speed
      try:
         retcode = subprocess.call(["shptree", "%s" % occ.getDLocation()])
         if retcode != 0:
            print('Unable to create shapetree index on %s' % occ.getDLocation())
      except Exception, e:
         print('Unable to create shapetree index on %s: %s' 
               % (occ.getDLocation(), str(e)))

          

#    # ....................................
#    def writePackage(self, pointData, subset=False):
#       """
#       @summary: Writes the points for the occurrence set
#       @param pointData: The posted data content
#       @param pointDataType: The content type of the point data (shapefile / csv)
#       @note: Look into adding a failsafe by checking the number of points 
#                 uploaded
#       """
#       # TODO: change this to just copy shapefiles, read metadata from meta file
#       # Get shapefiles for entire dataset and subset if it exists
#       pointsStream = StringIO()
#       pointsZ = ZipFile(pointsStream, 'w', compression=ZIP_DEFLATED,
#                            allowZip64=True)      
#       subsetStream = None
#       subsetZ = None
#       z = ZipFile(StringIO(pointData), allowZip64=True)
#       
#       for zname in z.namelist():
#          name, ext = os.path.splitext(zname)
#          if ext in SHAPEFILE_EXTENSIONS:
#             if name.find('points') > -1:
#                # Add file to pointsStream
#                v = StringIO(z.read(zname))
#                v.seek(0)
#                pointsZ.writestr(zname, v.getvalue())
#             elif name.find('subset') > -1:
#                # or add file to subsetStream
#                # open subsetStream if it doesn't yet exist
#                if subsetStream is None:
#                   subsetStream = StringIO()
#                   subsetZ = ZipFile(subsetStream, 'w', 
#                                        compression=ZIP_DEFLATED, 
#                                        allowZip64=True)
#                v = StringIO(z.read(zname))
#                v.seek(0)
#                subsetZ.writestr(zname, v.getvalue())
#             else:
#                pass
#       pointsZ.close()
#       pointsStream.seek(0)            
#       if subsetStream is not None:
#          subsetZ.close()      
#          subsetStream.seek(0)
#          self.writePoints(subsetStream.getvalue(), subset=True)
#       # writePoints sets queryCount and dlocation (non-subset overwrites)
#       self.writePoints(pointsStream.getvalue(), subset=False)
#      
#    # ....................................
#    def writePoints(self, pointData, subset=False):
#       """
#       @summary: Writes the points for the occurrence set
#       @param pointData: The posted data content
#       @param pointDataType: The content type of the point data (shapefile / csv)
#       @note: Look into adding a failsafe by checking the number of points 
#                 uploaded
#       @note: This updates the dlocation and queryCount on the occ object
#       """
#       # TODO: change this to just copy files
#       import subprocess
#       occ = self._outputObj
#       
#       # This also sets the dlocation on occ object
#       occ.clearDLocation()
#       dlocation = occ.getDLocation(subset=subset)
#       
#       try:
#          occ.readFromUploadedData(pointData)
#       except LMError, e:
#          self.update(status=JobStatus.IO_OCCURRENCE_SET_READ_ERROR)
#          raise e
#       except Exception, e:
#          self.update(status=JobStatus.IO_OCCURRENCE_SET_READ_ERROR)
#          raise LMError('Error (readFromUploadedData) reading points (%s)' % str(e))
#       
#       try:
#          occ.writeShapefile(dlocation=dlocation, overwrite=True)
#          occ.queryCount = occ.count
#          #self.update(status=JobStatus.COMPLETE)
#       except LMError, e:
#          self.update(status=JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR)
#          raise e
#       except Exception, e:
#          self.update(status=JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR)
#          raise LMError('Error writeShapefile (%s)' % str(e))
#       
#       # Build indices on shapefile for speed
#       try:
#          retcode = subprocess.call(["shptree", "%s" % occ.getDLocation()])
#          if retcode != 0:
#             print('Unable to create shapetree index on %s' % occ.getDLocation())
#       except Exception, e:
#          print('Unable to create shapetree index on %s: %s' 
#                % (occ.getDLocation(), str(e)))
      
# .............................................................................
class SDMModelJob(_Job):
   # ....................................
   stage=JobStage.MODEL
   # Models do not depend on prior calculations 
#    priorMDLStage = []
   # ....................................
   def __init__(self, model, processType=ProcessType.OM_MODEL, 
                computeId=None, lastHeartbeat=None, createTime=None, 
                jid=None, retryCount=None):
      """
      @summary: Model job constructor
      @param model: Lifemapper model object that contains parameters for the
                       model to be generated.
      @copydoc LmServer.base.job._Job::__init__()
      """
      jobData = SDMModelJobData(model, jid, processType)
      _Job.__init__(self, jobData, model, ReferenceType.SDMModel, 
                       jobFamily=JobFamily.SDM, computeId=computeId, 
                       status=model.status, statusModTime=model.statusModTime, 
                       priority=model.priority, lastHeartbeat=lastHeartbeat, 
                       createTime=createTime, retryCount=retryCount)
   
   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      if component.lower() == 'model':
         self.writeRuleset(content)
      elif component.lower() == 'package':
         self.writePackage(content)
      else:
         raise LMError(["Do not know how to write content (%s) for model job" % component])

   # ....................................
   def writeRuleset(self, rulesetData):
      """
      @summary: Writes out the model ruleset
      @postcondition: The raw model is written to the filesystem
      @note: If file exists, it is first deleted.  This must be done explicitely,
             as if the owner is different, it will not be automatically 
             overwritten.
      """
      model = self.dataObj
      self._readyFilename(model.ruleset, overwrite=True)
      try:
         f = open(model.ruleset,"w")
         f.write(rulesetData)
         f.close()
      except Exception, e:
         self.update(status=JobStatus.IO_MODEL_OUTPUT_WRITE_ERROR)
         raise LMError ('Error writing file %s' % model.ruleset)   

   # ....................................
   def writePackage(self, packageData):
      """
      @summary: Writes out the model ruleset
      @postcondition: The raw model is written to the filesystem
      @note: If file exists, it is first deleted.  This must be done explicitely,
             as if the owner is different, it will not be automatically 
             overwritten.
      """
      model = self.dataObj
      fname = model.getModelStatisticsFilename()
      self._readyFilename(fname, overwrite=True)      
      try:
         f = open(fname, "w")
         f.write(packageData)
         f.close()
         self.update(status=JobStatus.COMPLETE)
      except Exception, e:
         self.update(status=JobStatus.IO_MODEL_OUTPUT_WRITE_ERROR)
         raise LMError('Error writing package %s (%s)' % (fname, str(e)))
         
      
# .............................................................................
class SDMProjectionJob(_Job):
   """
   @summary: SDMProjection parent class
   @note: Inherits from _Job
   @note: Use a subclass
   """
   # ....................................
   stage=JobStage.PROJECT
   jobFamily=JobFamily.SDM
   # Projections depend on completed model
   priorMDLStage = [JobStage.MODEL]
   # ....................................
   def __init__(self, projection, processType=ProcessType.OM_PROJECT, 
                computeId=None, lastHeartbeat=None, createTime=None, 
                jid=None, retryCount=None):
      """
      @summary: Projection job constructor
      @param projection: Lifemapper projection object containing parameters
                            for the projection to be generated.
      @copydoc LmServer.base.job._Job::__init__()
      """
      jobData = SDMProjectionJobData(projection, jid, processType)
      _Job.__init__(self, jobData, projection, ReferenceType.SDMProjection, 
                       jobFamily=JobFamily.SDM, computeId=computeId, 
                       status=projection.status, 
                       statusModTime=projection.statusModTime, 
                       priority=projection.priority, lastHeartbeat=lastHeartbeat, 
                       createTime=createTime, retryCount=retryCount)
      
   # ....................................
   def write(self, component, content, contentType):
      """
      @summary: Writes the content for the specified component
      @param component: The job component to write
      @param content: The content to write
      @param contentType: The mime-type of the content
      """
      if component.lower() == 'projection':
         if self.jobData._dataObj.getUserId() == 'cgwillis':
            self.writeZippedRaster(content)
         else:
            self.writeRaster(content, None)
      elif component.lower() == 'package':
         self.writePackage(content)
      else:
         raise LMError(["Do not know how to write content (%s) for model job" % component])

   # ....................................
   def writeRaster(self, projdata, srs):
      """
      @summary: Writes out the projection's raster file (tif)
      @postcondition: The projection is written to the filesystem
      """
      projection = self.jobData._dataObj
      # Converting ASCII grids to Tiffs on ComputeResource, prior to return
      ext = OutputFormat.GTIFF
      print "Writing projection for: %s" % self.jobData.jid
      try:
         projection.writeProjection(projdata, srs=srs, fileExtension=ext)
         
      except LMError, e:
         self.update(status=JobStatus.IO_PROJECTION_OUTPUT_WRITE_ERROR)
         raise 
      except Exception, e:
         self.update(status=JobStatus.IO_PROJECTION_OUTPUT_WRITE_ERROR)
         raise LMError(currargs='Error writing raster', prevargs=e.args, 
                       lineno=self.getLineno())
      
      else:
         # populate filename and raster info on projection
         projection.populateStats()
         # delete mapfile, it must be updated
         projection.clearLocalMapfile()
      del projdata
      
   # ....................................
   def writePackage(self, pkgdata):
      """
      @summary: Writes out the projection's raster file (tif)
      @postcondition: The projection is written to the filesystem
      """
      projection = self.jobData._dataObj
      try:
         projection.writePackage(pkgdata)
      except LMError, e:
         self.update(status=JobStatus.IO_PROJECTION_OUTPUT_WRITE_ERROR)
         raise 
      except Exception, e:
         self.update(status=JobStatus.IO_PROJECTION_OUTPUT_WRITE_ERROR)
         raise LMError(currargs='Error writing package', prevargs=e.args, 
                       lineno=self.getLineno())
      self.update(status=JobStatus.COMPLETE)
      del pkgdata
   
   # ...................................
   def writeZippedRaster(self, compressedRaster):
      projection = self.jobData._dataObj
      # Converting ASCII grids to Tiffs on ComputeResource, prior to return
      ext = OutputFormat.TAR_GZ
      print "Writing compressed projection for: %s" % self.jobData.jid
      try:
         # Get file location
         fname = projection.createLocalDLocation()+".gz"
         projection.setDLocation(dlocation=fname)
         # Write data
         with open(fname, 'wb') as outF:
            outF.write(compressedRaster)
         
      except LMError, e:
         self.update(status=JobStatus.IO_PROJECTION_OUTPUT_WRITE_ERROR)
         raise 
      except Exception, e:
         self.update(status=JobStatus.IO_PROJECTION_OUTPUT_WRITE_ERROR)
         raise LMError(currargs='Error writing raster', prevargs=e.args, 
                       lineno=self.getLineno())
      
      del compressedRaster
   
# .............................................................................
class SDMOccurrenceJobData(_JobData):
   """
   """
   # ....................................
   def __init__(self, occSet, jid, processtype):
      """
      @summary: SDM Occurrence job constructor
      @param occSet: Lifemapper OccurrenceSet object that contains parameters 
                     for the occurrenceSet to be populated.
      @copydoc LmServer.base.job._Job::__init__()
      """
      if processtype not in [ProcessType.USER_TAXA_OCCURRENCE, 
                             ProcessType.GBIF_TAXA_OCCURRENCE,
                             ProcessType.BISON_TAXA_OCCURRENCE, 
                             ProcessType.IDIGBIO_TAXA_OCCURRENCE]:
         raise LMError(currargs='Unsupported OccurrenceJob ProcessType {}'
                                 .format(processtype))
      
      rdloc = occSet.getRawDLocation()
      if rdloc is None:
         raise LMError('Missing raw data location')
         
      # check for local csv for User or GBIF data
      if (processtype in [ProcessType.USER_TAXA_OCCURRENCE, 
                          ProcessType.GBIF_TAXA_OCCURRENCE]):
         if os.path.exists(rdloc):
            # Add the delimited values so they can be sent to cluster
            with open(rdloc) as dFile:
               tmpStr = dFile.read()
               # Remove non-printable characters
               import string
               self.delimitedOccurrenceValues = ''.join(
                                 filter(lambda x: x in string.printable, tmpStr))
         else:
            raise LMError("Data location: %s, does not exist" % rdloc)
      
      obj = {'dlocation': rdloc,
             'count': occSet.queryCount}
      _JobData.__init__(self, obj, jid, processtype, 
                        occSet.getUserId(), occSet.metadataUrl, 
                        occSet.metadataUrl)

# .............................................................................
class SDMModelJobData(_JobData):
   """
   """
   # ....................................
   def __init__(self, model, jid, processtype):
      """
      @summary: SDMModel job constructor
      @param model: Lifemapper model object that contains parameters for the
                   model to be generated.
      @copydoc LmServer.base.job._Job::__init__()
      """
      pointLimit = None
      if (model.getUserId() == ARCHIVE_USER and 
          model.occurrenceSet.queryCount > POINT_COUNT_MAX):
         subset = True
      else:
         subset = False
         
      if model.status in (JobStatus.INITIALIZE, JobStatus.PULL_REQUESTED):
         try:
            model.occurrenceSet.readShapefile(subset=subset)
         except Exception, e:
            raise LMMissingDataError('Invalid occurrence data source %s for %s, (%s)' 
                     % (str(model.occurrenceSet.getDLocation(subset=subset)), 
                        str(model.occurrenceSet.getId()), str(e)))
      _JobData.__init__(self, model, jid, processtype, model.getUserId(),
                        model.metadataUrl, model.metadataUrl)

# .............................................................................
class SDMProjectionJobData(_JobData):
   # ....................................
   def __init__(self, projection, jid, processtype):
      """
      @summary: SDMProjection job constructor
      @param projection: Lifemapper projection object containing parameters
                            for the projection to be generated.
      @param jid: The job database id 
      @param processtype: Type of calculations to be performed on this object
      """
      _JobData.__init__(self, projection, jid, processtype, 
                        projection.getUserId(), projection.metadataUrl,
                        projection.getModel().metadataUrl)

