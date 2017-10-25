"""
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
from LmBackend.command.base import _LmCommand
from LmBackend.common.lmconstants import CMD_PYBIN, SINGLE_SPECIES_SCRIPTS_DIR

# .............................................................................
class BisonPointsCommand(_LmCommand):
   """
   @summary: This command builds a BISON occurrence set
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'bison_points.py'

   # ................................
   def __init__(self, pointsUrl, outFile, bigFile, maxPoints):
      """
      @summary: Construct the command object
      @param pointsUrl: A URL to query for occurrence data
      @param outFile: The file location to write the shapefile for models
      @param bigFile: The file location to write the full occurrence set
      @param maxPoints: The maximum number of points for modeling
      """
      _LmCommand.__init__(self)
      self.outputs.append(outFile)
      self.pointsUrl = pointsUrl
      self.outFile = outFile
      self.bigFile = bigFile
      self.maxPoints = maxPoints

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {} {}'.format(CMD_PYBIN, self.getScript(),
                  self.pointsUrl, self.outFile, self.bigFile, self.maxPoints)

# .............................................................................
class GbifPointsCommand(_LmCommand):
   """
   @summary: This command creates an occurrence set from GBIF points
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'gbif_points.py'

   # ................................
   def __init__(self, csvFile, pointCount, outFile, bigFile, maxPoints):
      """
      @summary: Construct the command object
      @param csvFile: File location of raw CSV to use for data
      @param pointCount: The number of records that should be in the CSV file
      @param outFile: The file location to write the modelable shapefile
      @param bigFile: The file location to write the full shapefile
      @param maxPoints: The maximum number of points to include in the 
                           modelable shapefile 
      """
      _LmCommand.__init__(self)
      self.inputs.append(csvFile)
      self.outputs.append(outFile)
      self.csvFile = csvFile
      self.pointCount = pointCount
      self.outFile = outFile
      self.bigFile = bigFile
      self.maxPoints = maxPoints

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return ''.format(CMD_PYBIN, self.getScript(),
               self.csvFile, self.pointCount, self.outFile, self.bigFile, 
               self.maxPoints)

# .............................................................................
class GrimRasterCommand(_LmCommand):
   """
   @summary: This command intersects a raster layer and a shapegrid for a GRIM
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'grim_raster.py'

   # ................................
   def __init__(self, shapegridFilename, rasterFilename, grimColFilename, 
                resolution, minPercent=None, ident=None):
      """
      @summary: Construct the command object
      @param shapegridFilename: The file location of the shapegrid to intersect
      @param rasterFilename: The file location of the raster file to intersect
                                with the shapegrid
      @param grimColFilename: The file location to write the GRIM column
      @param resolution: The resolution of the raster
      @param minPercent: If provided, use largest class method, otherwise use 
                            weighted mean
      @param ident: If included, use this for a label on the GRIM column 
      """
      _LmCommand.__init__(self)
      self.inputs.extend([shapegridFilename, rasterFilename])
      self.outputs.append(grimColFilename)
      
      self.sgFn = shapegridFilename
      self.rastFn = rasterFilename
      self.grimColFn = grimColFilename
      self.resolution = resolution
      self.minPercent = minPercent
      self.ident = ident

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      optArgs = ''
      if self.minPercent is not None:
         optArgs += ' -m {}'.format(self.minPercent)
      if self.ident is not None:
         optArgs += ' -i {}'.format(self.ident)
      return '{} {} {} {} {} {} {}'.format(CMD_PYBIN, self.getScript(),
            optArgs, self.sgFn, self.rastFn, self.grimColFn, self.resolution)

# .............................................................................
class IdigbioPointsCommand(_LmCommand):
   """
   @summary: This command creates an iDigBio occurrence set
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'idigbio_points.py'

   # ................................
   def __init__(self, taxonKey, outFile, bigFile, maxPoints):
      """
      @summary: Construct the command object
      @param taxonKey: The taxon key to use when querying iDigBio
      @param outFile: The file location to write the shapefile for modeling
      @param bigFile: The file location to write the full shapefile
      @param maxPoints: The maximum number of points to include in the model 
                           shapefile
      """
      _LmCommand.__init__(self)
      self.outputs.append(outFile)
      self.taxonKey = taxonKey
      self.outFile = outFile
      self.bigFile = bigFile
      self.maxPoints = maxPoints

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {} {}'.format(CMD_PYBIN, self.getScript(),
               self.taxonKey, self.outFile, self.bigFile, self.maxPoints)

# .............................................................................
class IntersectRasterCommand(_LmCommand):
   """
   @summary: This command intersects a raster layer and a shapegrid
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'intersect_raster.py'

   # ................................
   def __init__(self, shapegridFilename, rasterFilename, pavFilename, 
                resolution, minPresence, maxPresence, percentPresence, 
                squid=None):
      """
      @summary: Construct the command object
      @param shapegridFilename: The file location of the shapegrid to intersect
      @param rasterFilename: The file location of the raster file to intersect
                                with the shapegrid
      @param pavFilename: The file location to write the resulting PAV
      @param resolution: The resolution of the raster
      @param minPresence: The minimum value to be considered present
      @param maxPresence: The maximum value to be considered present
      @param percentPresence: The percent of a shapegrid feature that must be 
                                 present to be considered present
      @param squid: If included, use this for a label on the PAV 
      """
      _LmCommand.__init__(self)
      self.inputs.extend([shapegridFilename, rasterFilename])
      self.outputs.append(pavFilename)
      
      self.sgFn = shapegridFilename
      self.rastFn = rasterFilename
      self.pavFn = pavFilename
      self.resolution = resolution
      self.minPres = minPresence
      self.maxPres = maxPresence
      self.perPres = percentPresence
      self.squid = squid

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {} {} {} {} {} {}'.format(CMD_PYBIN, self.getScript(),
            ' --squid={}'.format(self.squid) if self.squid is not None else '',
            self.sgFn, self.rastFn, self.pavFn, self.resolution, self.minPres, 
            self.maxPres, self.perPres)

# .............................................................................
class IntersectVectorCommand(_LmCommand):
   """
   @summary: This command intersects a vector layer and a shapegrid
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'intersect_vector.py'

   # ................................
   def __init__(self, shapegridFilename, vectorFilename, pavFilename, 
                presenceAttrib, minPresence, maxPresence, percentPresence, 
                squid=None):
      """
      @summary: Construct the command object
      @param shapegridFilename: The file location of the shapegrid to intersect
      @param vectorFilename: The file location of the vector file to intersect
                                with the shapegrid
      @param pavFilename: The file location to write the resulting PAV
      @param presenceAttrib: The shapefile attribute to use for determining 
                                presence
      @param minPresence: The minimum value to be considered present
      @param maxPresence: The maximum value to be considered present
      @param percentPresence: The percent of a shapegrid feature that must be 
                                 present to be considered present
      @param squid: If included, use this for a label on the PAV 
      """
      _LmCommand.__init__(self)
      self.inputs.extend([shapegridFilename, vectorFilename])
      self.outputs.append(pavFilename)
      
      self.sgFn = shapegridFilename
      self.vectFn = vectorFilename
      self.pavFn = pavFilename
      self.presAttrib = presenceAttrib
      self.minPres = minPresence
      self.maxPres = maxPresence
      self.perPres = percentPresence
      self.squid = squid

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {} {} {} {} {} {}'.format(CMD_PYBIN, self.getScript(),
            ' --squid={}'.format(self.squid) if self.squid is not None else '',
            self.sgFn, self.vectFn, self.pavFn, self.presAttrib, self.minPres, 
            self.maxPres, self.perPres)

# .............................................................................
class SdmodelCommand(_LmCommand):
   """
   @summary: This command creates a species distribution model
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'sdmodel.py'

   # ................................
   def __init__(self, pType, jobName, pointsFilename, layersJsonFilename, 
                rulesetFilename, paramsJsonFilename, packageFilename=None,
                workDir=None, metricsFilename=None, logFilename=None, 
                statusFilename=None, maskFilename=None):
      """
      @summary: Construct the command object
      @param pType: This parameter indicates which type of model to create
      @param jobName: A name for this model
      @param pointsFilename: The file location of the occurrence shapefile to 
                                use for modeling
      @param layersJsonFilename: The file location of the JSON file containing
                                    climate layer information
      @param rulesetFilename: The file location of the output model rule set
      @param paramsJsonFilename: The file location of the JSON file with 
                                    algorithm parameters
      @param workDir: A work directory name (relative)
      @param metricsFilename: A file location to write metrics
      @param logFilename: A file location to write logging
      @param statusFilename: A file location to write the status of the 
                                projection
      @param packageFilename: A file location to write the projection package
      @param maskFilename: A file location of a mask layer to use for 
                              projecting 
      """
      _LmCommand.__init__(self)
      self.inputs.extend([pointsFilename, layersJsonFilename, 
                          paramsJsonFilename])
      self.outputs.append(rulesetFilename)

      self.pType = pType
      self.jobName = jobName
      self.pointsFn = pointsFilename
      self.lyrsFn = layersJsonFilename
      self.rsFn = rulesetFilename
      self.paramsFn = paramsJsonFilename
      
      self.optArgs = ''
      
      if workDir is not None:
         self.optArgs += ' -w {}'.format(workDir)
         
      if metricsFilename is not None:
         self.outputs.append(metricsFilename)
         self.optArgs += ' --metrics_file={}'.format(metricsFilename)
         
      if logFilename is not None:
         self.outputs.append(logFilename)
         self.optArgs += ' -l {}'.format(logFilename)
      
      if statusFilename is not None:
         self.outputs.append(statusFilename)
         self.optArgs += ' -s {}'.format(statusFilename)
      
      if packageFilename is not None:
         self.outputs.append(packageFilename)
         self.optArgs += ' -p {}'.format(packageFilename)
      
      if maskFilename is not None:
         self.inputs.append(maskFilename)
         self.optArgs += ' -m {}'.format(maskFilename)

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {} {} {} {} {}'.format(
         CMD_PYBIN, self.getScript(),
         self.optArgs, self.pType, self.jobName, self.pointsFn,
         self.lyrsFn, self.rsFn, self.paramsFn)

# .............................................................................
class SdmProjectCommand(_LmCommand):
   """
   @summary: This command creates a projection from a model rule set and a 
                package of climate layers
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'sdmproject.py'

   # ................................
   def __init__(self, pType, jobName, rulesetFilename, layersJsonFilename, 
                outputRasterFilename, algo=None, workDir=None, 
                metricsFilename=None, logFilename=None, statusFilename=None, 
                packageFilename=None, maskFilename=None):
      """
      @summary: Construct the command object
      @param pType: This parameter indicates which type of SDM projection to 
                       create
      @param jobName: A name for this projection
      @param rulesetFilename: The file location of the model rule set to be
                                 applied
      @param layersJsonFilename: The file location of the JSON file containing
                                    climate layer information
      @param outputRasterFilename: The file location to write the resulting 
                                      raster
      @param algo: Only for Maxent, a file location with algorithm parameters
      @param workDir: A work directory name (relative)
      @param metricsFilename: A file location to write metrics
      @param logFilename: A file location to write logging
      @param statusFilename: A file location to write the status of the 
                                projection
      @param packageFilename: A file location to write the projection package
      @param maskFilename: A file location of a mask layer to use for 
                              projecting 
      """
      _LmCommand.__init__(self)
      self.inputs.extend([rulesetFilename, layersJsonFilename])
      self.outputs.append(outputRasterFilename)
      
      self.pType = pType
      self.jobName = jobName
      self.rsFn = rulesetFilename
      self.lyrsFn = layersJsonFilename
      self.outFn = outputRasterFilename
      
      self.optArgs = ''
      
      if algo is not None:
         self.optArgs += ' -algo {}'.format(algo)
         self.inputs.append(algo)
      
      if workDir is not None:
         self.optArgs += ' -w {}'.format(workDir)
         
      if metricsFilename is not None:
         self.outputs.append(metricsFilename)
         self.optArgs += ' --metrics_file={}'.format(metricsFilename)
         
      if logFilename is not None:
         self.outputs.append(logFilename)
         self.optArgs += ' -l {}'.format(logFilename)
      
      if statusFilename is not None:
         self.outputs.append(statusFilename)
         self.optArgs += ' -s {}'.format(statusFilename)
      
      if packageFilename is not None:
         self.outputs.append(packageFilename)
         self.optArgs += ' -p {}'.format(packageFilename)
      
      if maskFilename is not None:
         self.inputs.append(maskFilename)
         self.optArgs += ' -m {}'.format(maskFilename)
      
   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {} {} {} {}'.format(CMD_PYBIN, self.getScript(),
               self.optArgs, self.pType, self.jobName, self.rsFn, 
               self.lyrsFn, self.outFn)

# .............................................................................
class UserPointsCommand(_LmCommand):
   """
   @summary: This command will create an occurrence set from user point data
   """
   relDir = SINGLE_SPECIES_SCRIPTS_DIR
   scriptName = 'user_points.py'

   # ................................
   def __init__(self, inCsvFilename, metadataFilename, outFilename, 
                bigFilename, maxPoints):
      """
      @summary: Construct the command object
      @param inCsvFilename: The file path to the raw user CSV
      @param metadataFilename: Path to a JSON file of occurrence metadata
      @param outFilename: The file location to write the modeling shapefile
      @param bigFilename: The file location to write the full shapefile
      @param maxPoints: The maximum number of points for the model shapefile
      """
      _LmCommand.__init__(self)
      self.inputs.extend([inCsvFilename, metadataFilename])
      
      # Only add the modelable shapefile since we don't know if the other will
      #    exist
      self.outputs.append(outFilename)
      
      self.inCsvFn = inCsvFilename
      self.metaFn = metadataFilename
      self.outFn = outFilename
      self.bigFn = bigFilename
      self.maxPoints = maxPoints

   # ................................
   def getCommand(self):
      """
      @summary: Get the raw command to run on the system
      """
      return '{} {} {} {} {} {} {}'.format(CMD_PYBIN, self.getScript(), 
            self.inCsvFn, self.metaFn, self.outFn, self.bigFn, self.maxPoints)
