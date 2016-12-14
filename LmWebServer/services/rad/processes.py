"""
@summary: Module containing processes for RAD services
@author: CJ Grady
@version: 2.0
@status: alpha

@license: gpl2
@copyright: Copyright (C) 2015, University of Kansas Center for Research

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
from mx.DateTime import gmt
from types import ListType

from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.common.lmconstants import (JobStage, JobStatus, RandomizeMethods,
                                    HTTPStatus)
from LmServer.base.layer import Raster, Vector
from LmServer.base.lmobj import LMError, LmHTTPError
from LmServer.common.localconstants import (WEBSERVICES_ROOT, DEFAULT_EPSG, 
                                            DEFAULT_MAPUNITS)
from LmServer.common.log import LmPublicLogger
from LmServer.db.scribe import Scribe
from LmServer.rad.anclayer import AncillaryRaster, AncillaryVector
from LmServer.rad.palayer import PresenceAbsenceRaster, PresenceAbsenceVector
from LmServer.rad.radbucket import RADBucket
from LmServer.rad.shapegrid import ShapeGrid

from LmWebServer.base.servicesBaseClass import WPSService

MAX_CELLS = 400000

# =============================================================================
class AddAncLayerProcess(WPSService):
   """
   @summary: Adds an ancillary layer to an experiment
   """
   identifier = "addanclayer"
   title = "Add Ancillary Layer"
   version = "1.0"
   abstract = "This process will add an ancillary layer to an experiment"
   inputParameters = [
                      {
                       "minOccurs" : "1",
                       "maxOccurs" : "unbounded",
                       "identifier" : "ancLayer",
                       "title" : "Ancillary layer to add",
                       "reference" : "%s/schemas/radServiceRequest.xsd#ancLayerType" % WEBSERVICES_ROOT,
                       "paramType" : "ancLayerType",
                       "defaultValue" : None
                      },
                     ]
   outputParameters = [
                      ]

   # ...................................
   def execute(self):
      log = LmPublicLogger()
      obj = deserialize(fromstring(self.body))
      lyrs = []
      
      scribe = Scribe(log)
      scribe.openConnections()

      if obj.DataInputs is not None:
         inputs = obj.DataInputs.Input
         if not isinstance(inputs, ListType):
            inputs = [inputs]
         for input in inputs:
            if input.Identifier == "ancLayer":
               ancLyr = input.Data.ComplexData
               lyrId = ancLyr.layerId
               
               try:
                  attrValue = ancLyr.parameters.attrValue
               except:
                  attrValue = None
               
               weightedMean = False
               largestClass = False
               
               try:
                  calculateMethod = ancLyr.parameters.calculateMethod
                  if calculateMethod.lower() == "weightedMean":
                     weightedMean = True
                  else:
                     largestClass = True
               except:
                  weightedMean = True
                  
               try:
                  minPercent = ancLyr.parameters.minPercent
               except:
                  minPercent = None
                               
               log.debug("AttrValue: %s" % attrValue)
               log.debug("WeightedMean: %s" % weightedMean)
               log.debug("LargestClass: %s" % largestClass)
               log.debug("MinPercent: %s" % minPercent)
               
               if lyrId is not None:
                  baseLyr = scribe.getRADLayer(lyrid=lyrId)
                  if baseLyr is None:
                     raise LMError("Layer %s does not exist" % str(lyrId))
                  elif isinstance(baseLyr, Vector):
                     lyr = AncillaryVector(name=baseLyr.name, 
                                           title=baseLyr.title, 
                                           bbox=baseLyr.bbox, 
                                           startDate=baseLyr.startDate, 
                                           endDate=baseLyr.endDate, 
                                           mapunits=baseLyr.mapUnits, 
                                           resolution=baseLyr.resolution, 
                                           epsgcode=baseLyr.epsgcode,
                                           ogrType=baseLyr.ogrType, 
                                           dataFormat=baseLyr.dataFormat,  
                                           valAttribute=baseLyr.getValAttribute(), 
                                           description=baseLyr.description, 
                                           attrValue=attrValue,
                                           weightedMean=weightedMean,
                                           largestClass=largestClass,
                                           minPercent=minPercent,
                                           ancUserId=self.user,
                                           lyrId=lyrId,
                                           lyrUserId=baseLyr.user)
                  elif isinstance(baseLyr, Raster):
                     lyr = AncillaryRaster(baseLyr.name, 
                                           title=baseLyr.title, 
                                           bbox=baseLyr.bbox, 
                                           startDate=baseLyr.startDate, 
                                           endDate=baseLyr.endDate, 
                                           mapunits=baseLyr.mapUnits, 
                                           resolution=baseLyr.resolution, 
                                           epsgcode=baseLyr.epsgcode,
                                           gdalType=baseLyr.gdalType,
                                           dataFormat=baseLyr.dataFormat,  
                                           description=baseLyr.description, 
                                           attrValue=attrValue,
                                           weightedMean=weightedMean,
                                           largestClass=largestClass,
                                           minPercent=minPercent,
                                           ancUserId=self.user,
                                           lyrId=lyrId,
                                           lyrUserId=baseLyr.user)
      
      expId = int(self.parameters["experimentid"])

      exp = scribe.getRADExperiment(self.user, expid=expId)
      scribe.insertAncillaryLayer(lyr, exp)
      
      scribe.closeConnections()
      
      timestamp = gmt()
      return self._executeResponse(JobStatus.COMPLETE, "ProcessSucceeded", timestamp)
   
# =============================================================================
class AddBucketProcess(WPSService):
   """
   @summary: Adds a bucket to an experiment
   """
   identifier = "addbucket"
   title = "Add a bucket to an experiment"
   version = "1.0"
   abstract = "This process will add a bucket to an experiment"
   inputParameters = [
                      {
                       "minOccurs" : "1",
                       "maxOccurs" : "unbounded",
                       "identifier" : "bucket",
                       "title" : "Bucket to add",
                       "reference" : "%s/schemas/radServiceRequest.xsd#bucketType" % WEBSERVICES_ROOT,
                       "paramType" : "bucketType",
                       "defaultValue" : None
                      },
                     ]
   outputParameters = [
                       {
                        "identifier" : "bucketId",
                        "title" : "The id of the bucket",
                        "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                        "paramType" : "integer"
                       },
                      ]

   # ...................................
   def execute(self):
      obj = deserialize(fromstring(self.body))
      log = LmPublicLogger()
      scribe = Scribe(log)
      scribe.openConnections()

      if obj.DataInputs is not None:
         inputs = obj.DataInputs.Input
         inputs = [obj.DataInputs.Input] if not isinstance(inputs, ListType) else inputs
         for input in inputs:
            if input.Identifier == "bucket":
               # Look to see if shapegrid id was provided or full definition
               
               # Full definition
               
               bucketEl = input.Data.ComplexData
               kws = []
               try:
                  sgId = bucketEl.shapegridId
                  expId = int(self.parameters["experimentid"])
                  exp = scribe.getRADExperiment(self.user, expid=expId)
                  shapegrid = scribe.getShapeGrid(self.user, shpid=sgId)
                  bucket = RADBucket(shapegrid, keywords=kws, 
                                     status=JobStatus.GENERAL, 
                                     stage=JobStage.GENERAL, userId=self.user,
                                     parentMetadataUrl=exp.metadataUrl)
                  b = scribe.insertBucket(bucket, exp)
               except:
                  name = bucketEl.shapegrid.name
                  cellShape = bucketEl.shapegrid.cellShape
                  cellSize = float(bucketEl.shapegrid.cellSize)
                  mapUnits = bucketEl.shapegrid.mapUnits
                  epsgCode = bucketEl.shapegrid.epsgCode
                  bounds = bucketEl.shapegrid.bounds
                  try:
                     left, bottom, right, top = bounds
                  except:
                     left, bottom, right, top = bounds.strip().split(',')

                  numCells = (float(top)-float(bottom))*(float(right)-float(left))/float(cellSize)**2
                  if numCells > MAX_CELLS:
                     raise LmHTTPError(HTTPStatus.BAD_REQUEST,
                        "Too many cells for shapegrid, Max: %s, Requested: %s"\
                           % (MAX_CELLS, numCells))
                  
                  try:
                     cutout = bucketEl.shapegrid.cutout.strip()
                  except:
                     cutout = None
   
                  if cellShape.lower() == "hexagon":
                     cellSides = 6
                  elif cellShape.lower() == "square":
                     cellSides = 4
                  else:
                     raise LMError("Undefined shape")
            
                  expId = int(self.parameters["experimentid"])
                  exp = scribe.getRADExperiment(self.user, expid=expId)
                  shapegrid = ShapeGrid(name, cellSides, cellSize, mapUnits, 
                                        epsgCode, bounds, userId=self.user,
                                        size=numCells)
                  neworexistingSG = scribe.insertShapeGrid(shapegrid, cutout=cutout)
                  neworexistingSG.buildShape(cutout=cutout)
                  bucket = RADBucket(neworexistingSG, keywords=kws, 
                                     status=JobStatus.GENERAL, 
                                     stage=JobStage.GENERAL, userId=self.user,
                                     parentMetadataUrl=exp.metadataUrl)
                  b = scribe.insertBucket(bucket, exp)

      scribe.closeConnections()
      
      timestamp = gmt()
      return self._executeResponse(JobStatus.COMPLETE, "ProcessSucceeded", timestamp, outputs={"bucketId" : b.id})

# =============================================================================
class AddPALayerProcess(WPSService):
   """
   @summary: Adds a presence absence layer to an experiment
   """
   identifier = "addpalayer"
   title = "Add PA Layer"
   version = "1.0"
   abstract = "This process will add a presence / absence layer to an experiment"
   inputParameters = [
                      {
                       "minOccurs" : "1",
                       "maxOccurs" : "unbounded",
                       "identifier" : "paLayer",
                       "title" : "Presence absence layer to add",
                       "reference" : "%s/schemas/radServiceRequest.xsd#paLayerType" % WEBSERVICES_ROOT,
                       "paramType" : "paLayerType",
                       "defaultValue" : None
                      },
                     ]
   outputParameters = [
                      ]

   # ...................................
   def execute(self):
      obj = deserialize(fromstring(self.body))
      lyrs = []
      
      scribe = Scribe(LmPublicLogger())
      scribe.openConnections()

      if obj.DataInputs is not None:
         inputs = obj.DataInputs.Input
         inputs = [obj.DataInputs.Input] if not isinstance(inputs, ListType) else inputs
         for input in inputs:
            if input.Identifier == "paLayer":
               paLyr = input.Data.ComplexData
               lyrId = paLyr.layerId
               
               try:
                  attrPresence = paLyr.parameters.attrPresence
               except:
                  attrPresence = None
               try:
                  minPresence = paLyr.parameters.minPresence
               except:
                  minPresence = None
               try:
                  maxPresence = paLyr.parameters.maxPresence
               except:
                  maxPresence = None
               try:
                  percentPresence = paLyr.parameters.percentPresence
               except:
                  percentPresence = None
               try:
                  attrAbsence = paLyr.parameters.attrAbsence
               except:
                  attrAbsence = None
               try:
                  minAbsence = paLyr.parameters.minAbsence
               except:
                  minAbsence = None
               try:
                  maxAbsence = paLyr.parameters.maxAbsence
               except:
                  maxAbsence = None
               try:
                  percentAbsence = paLyr.parameters.percentAbsence
               except:
                  percentAbsence = None
                  
               if lyrId is not None:
                  baseLyr = scribe.getRADLayer(lyrid=lyrId)
                  if baseLyr is None:
                     raise LMError("Layer %s does not exist" % str(lyrId))
                  elif isinstance(baseLyr, Vector):
                     lyr = PresenceAbsenceVector(baseLyr.name, title=baseLyr.title, 
                              bbox=baseLyr.bbox, startDate=baseLyr.startDate, 
                              endDate=baseLyr.endDate, mapunits=baseLyr.mapUnits, 
                              resolution=baseLyr.resolution, 
                              keywords=baseLyr.keywords, epsgcode=baseLyr.epsgcode,  
                              ogrType=baseLyr.ogrType, dataFormat=baseLyr.dataFormat,  
                              #valAttribute=baseLyr.getValAttribute(), 
                              description=baseLyr.description, 
                              attrPresence=attrPresence, minPresence=minPresence, 
                              maxPresence=maxPresence, 
                              percentPresence=percentPresence, 
                              attrAbsence=attrAbsence, minAbsence=minAbsence, 
                              maxAbsence=maxAbsence, percentAbsence=percentAbsence, 
                              paUserId=self.user, lyrId=lyrId, lyrUserId=baseLyr.user)
                  elif isinstance(baseLyr, Raster):
                     lyr = PresenceAbsenceRaster(baseLyr.name, title=baseLyr.title, 
                              bbox=baseLyr.bbox, startDate=baseLyr.startDate, 
                              endDate=baseLyr.endDate, mapunits=baseLyr.mapUnits, 
                              resolution=baseLyr.resolution, 
                              keywords=baseLyr.keywords, epsgcode=baseLyr.epsgcode, 
                              gdalType=baseLyr.gdalType, 
                              dataFormat=baseLyr.dataFormat, 
                              description=baseLyr.description, 
                              attrPresence=attrPresence, minPresence=minPresence, 
                              maxPresence=maxPresence, 
                              percentPresence=percentPresence, 
                              attrAbsence=attrAbsence, minAbsence=minAbsence, 
                              maxAbsence=maxAbsence, percentAbsence=percentAbsence, 
                              paUserId=self.user, lyrId=lyrId, lyrUserId=baseLyr.user)
      
      expId = int(self.parameters["experimentid"])

      exp = scribe.getRADExperiment(self.user, expid=expId)
      scribe.insertPresenceAbsenceLayer(lyr, exp, rollback=True)
      
      scribe.closeConnections()
      
      timestamp = gmt()
      return self._executeResponse(JobStatus.COMPLETE, "ProcessSucceeded", timestamp)
   
# =============================================================================
class AddTreeProcess(WPSService):
   """
   @summary: Adds a tree to an experiment
   """
   identifier = "addtree"
   title = "Adds a tree to an experiment"
   version = "1.0"
   abstract = "This process will add a tree to an experiment"
   inputParameters = [
                      {
                       "minOccurs" : "1",
                       "maxOccurs" : "1",
                       "identifier" : "jsonTree",
                       "title" : "Tree data in Lifemapper QGIS plugin JSON format",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#string",
                       "paramType" : "string",
                       "defaultValue" : None
                      }
                     ]
   outputParameters = [
                      ]

   # ...................................
   def execute(self):
      obj = deserialize(fromstring(self.body))
      lyrs = []
      
      scribe = Scribe(LmPublicLogger())
      scribe.openConnections()

      if obj.DataInputs is not None:
         inputs = obj.DataInputs.Input
         inputs = [obj.DataInputs.Input] if not isinstance(inputs, ListType) else inputs
         for input in inputs:
            if input.Identifier == "jsonTree":
               jTree = input.Data.LiteralData.strip()

      scribe = Scribe(LmPublicLogger())
      scribe.openConnections()
      
      expId = int(self.parameters["experimentid"])

      exp = scribe.getRADExperiment(self.user, expid=expId)
      
      if exp is None:
         raise LmHTTPError(HTTPStatus.NOT_FOUND, 
                           "Experiment %s was not found for this user" % expId)
      
      # Save data here
      exp.writeAttributeTree(jTree)
      scribe.updateRADExperiment(exp)
      
      scribe.closeConnections()
      
      timestamp = gmt()
      return self._executeResponse(JobStatus.COMPLETE, "ProcessSucceeded", timestamp)
   
# =============================================================================
class IntersectProcess(WPSService):
   """
   @summary: Intersects a shapegrid with a list of layers
   """
   identifier = "intersect"
   title = "Intersect layers"
   version = "1.0"
   abstract = "This process will intersect a bucket's shapegrid with the layers in the containing experiment."
   inputParameters = [
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "experimentId",
                       "title" : "Id of the RAD experiment",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "bucketId",
                       "title" : "Id of the RAD bucket",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                     ]
   outputParameters = [
                      ]

   # ...................................
   def execute(self):
      obj = deserialize(fromstring(self.body))
      bucketId = None
      experimentId = None
      
      if obj.DataInputs is not None:
         for input in obj.DataInputs.Input:
            if input.Identifier == "bucketId":
               bucketId = input.Data.LiteralData
            elif input.Identifier == "experimentId":
               experimentId = input.Data.LiteralData

      # Look for object IDs
      if self.parameters.has_key("experimentid"):
         experimentId = int(self.parameters["experimentid"])
      if self.parameters.has_key("bucketid"):
         bucketId = int(self.parameters["bucketid"])
      
      logger = LmPublicLogger()
      logger.debug("Bucket id: %s" % bucketId)
      logger.debug("Experiment id: %s" % experimentId)
      logger.debug("Parameters: %s" % self.parameters)
      scribe = Scribe(LmPublicLogger())
      scribe.openConnections()
      ret = scribe.initRADIntersectPlus(self.user, bucketId)
#      if not isinstance(ret, ListType):
#         ret = [ret]
#      for j in ret:
#         if j._dataObj.id == bucketId:
#            s1 = j.post()
#            s2 = scribe.updateJob(j)
#      scribe.closeConnections()
      
      timestamp = gmt()
      return self._executeResponse(JobStatus.INITIALIZE, "Process Accepted", timestamp)
   

# =============================================================================
class RandomizeProcess(WPSService):
   """
   @summary: Creates a randomized PamSum for a bucket
   """
   identifier = "randomize"
   title = "Randomize a bucket's PamSum"
   version = "1.0"
   abstract = "This process will randomize the bucket's PamSum using the method selected."
   inputParameters = [
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "experimentId",
                       "title" : "Id of the RAD experiment",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "bucketId",
                       "title" : "Id of the Bucket",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "randomizeMethod",
                       "title" : "The method used to randomize the PamSum",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#String",
                       "paramType" : "String",
                       "defaultValue" : "Swap"
                      },
                      # TODO: Remove iterations
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "iterations",
                       "title" : "The number of iterations to use with the swap algorithm",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : 10000
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "numSwaps",
                       "title" : "The number of successful swaps to use with the swap algorithm",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : 10000
                      }
                     ]
   outputParameters = [
                      ]

   # ...................................
   def execute(self):
      obj = deserialize(fromstring(self.body))
      bucketId = None
      method = "swap"
      numSwaps = 10000
      
      if obj.DataInputs is not None:
         for input in obj.DataInputs.Input:
            if input.Identifier == "bucketId":
               bucketId = input.Data.LiteralData
            elif input.Identifier == "experimentId":
               experimentId = input.Data.LiteralData
            elif input.Identifier == "randomizeMethod":
               method = input.Data.LiteralData.strip().lower()

      # Look for parameters
      if self.parameters.has_key("experimentid"):
         experimentId = int(self.parameters["experimentid"])
      if self.parameters.has_key("bucketid"):
         bucketId = int(self.parameters["bucketid"])
      if self.parameters.has_key("randomizemethod"):
         method = self.parameters["randomizemethod"].lower()
      # TODO: Remove
      if self.parameters.has_key("iterations"):
         numSwaps = int(self.parameters["iterations"])
      if self.parameters.has_key("numswaps"):
         numSwaps = int(self.parameters["numswaps"])
      
      if method not in ["swap", "splotch", "grady"]:
         method = "swap"
      
      logger = LmPublicLogger()
      logger.debug("Bucket id: %s" % bucketId)
      logger.debug("Experiment id: %s" % experimentId)
      logger.debug("Method: %s" % method)
      scribe = Scribe(logger)
      scribe.openConnections()
      #scribe = Scribe(logger)
      #scribe.openConnections()
 
      #bucket = scribe.getRADBucket(bucketId)
      if method == "swap":
         alg = RandomizeMethods.SWAP
      elif method == "grady":
         alg = RandomizeMethods.GRADY
      else:
         alg = RandomizeMethods.SPLOTCH
         
      ret = scribe.initRADRandomizePlus(self.user, bucketId, method=alg, 
                                    numSwaps=numSwaps)

#      if not isinstance(ret, ListType):
#         ret = [ret]
#      for j in ret:
#         if j._dataObj.id == bucketId:
#            s1 = j.post()
#            s2 = scribe.updateJob(j)
      scribe.closeConnections()
      #scribe.closeConnections()
      
      timestamp = gmt()
      return self._executeResponse(JobStatus.INITIALIZE, "Process Accepted", timestamp)
   

# =============================================================================
class BuildGridProcess(WPSService):
   """
   @summary: Creates a ShapeGrid
   """
   identifier = "buildgrid"
   title = "Create a ShapeGrid"
   version = "1.0"
   abstract = "This process will create a ShapeGrid for any of a user's RAD buckets."
   inputParameters = [
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "name",
                       "title" : "Name of the ShapeGrid",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#String",
                       "paramType" : "String",
                       "defaultValue" : None
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "cellShape",
                       "title" : "Shape of grid cells",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#String",
                       "paramType" : "String",
                       "defaultValue" : 'square'
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "cellSize",
                       "title" : "The resolution (in mapunits) of grid cells",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#String",
                       "paramType" : "number",
                       "defaultValue" : 1
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "mapUnits",
                       "title" : "The units used for measurement in the ShapeGrid",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#String",
                       "paramType" : "String",
                       "defaultValue" : DEFAULT_MAPUNITS
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "epsgCode",
                       "title" : "The number of successful swaps to use with the swap algorithm",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#integer",
                       "paramType" : "integer",
                       "defaultValue" : DEFAULT_EPSG
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "bbox",
                       "title" : "The minX, minY, maxX, maxY of the ShapeGrid",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#String",
                       "paramType" : "String",
                       "defaultValue" : '-180,-90,180,90'
                      },
                      {
                       "minOccurs" : "0",
                       "maxOccurs" : "1",
                       "identifier" : "cutout",
                       "title" : "A polygon in well-known text (WKT) defining areas to be removed from the ShapeGrid",
                       "reference" : "http://www.w3.org/TR/xmlschema-2/#String",
                       "paramType" : "String",
                       "defaultValue" : None
                      },

                     ]
   outputParameters = [
                      ]

   # ...................................
   def execute(self):
      obj = deserialize(fromstring(self.body))
      cellSides = 4
      cellSize = 1
      bbox = '-180,-90,180,90'
      mapUnits = DEFAULT_MAPUNITS
      epsg = DEFAULT_EPSG
      cutout = None
      # POST data
      if obj.DataInputs is not None:
         for input in obj.DataInputs.Input:
            if input.Identifier == "name":
               name = input.Data.LiteralData.strip()
            elif input.Identifier == "cellShape":
               cellShape = input.Data.LiteralData.strip().lower()
            elif input.Identifier == "cellSize":
               cellSize = input.Data.LiteralData
            elif input.Identifier == "mapUnits":
               mapUnits = input.Data.LiteralData
            elif input.Identifier == "epsgCode":
               epsg = input.Data.LiteralData.strip().lower()
            elif input.Identifier == "bbox":
               bbox = input.Data.LiteralData.strip().lower()
            elif input.Identifier == "cutout":
               cutout = input.Data.LiteralData.strip().lower()
      # URL parameters
      if self.parameters.has_key("name"):
         name = self.parameters["name"]
      if self.parameters.has_key("cellshape"):
         cellShape = int(self.parameters["cellshape"])
      if self.parameters.has_key("cellsize"):
         cellSize = self.parameters["cellsize"].lower()
      if self.parameters.has_key("mapunits"):
         mapUnits = self.parameters["mapunits"].lower()
      if self.parameters.has_key("epsgcode"):
         epsgCode = int(self.parameters["epsgcode"])
      if self.parameters.has_key("bbox"):
         bbox = self.parameters["bbox"]
      if self.parameters.has_key("cutout"):
         cutout = self.parameters["cutout"].lower()
      
      if cellShape == "hexagon":
         cellSides = 6
      elif cellShape == "square":
         cellSides = 4
      else:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Undefined cell shape")
      
      shpgrid = ShapeGrid(name, cellSides, cellSize, mapUnits, epsgCode, bbox)
      logger = LmPublicLogger()
      logger.debug("Shapegrid name: %s" % name)
      scribe = Scribe(logger)
      scribe.openConnections()
 
      ret = scribe.initRADBuildGrid(self.user, shpgrid, cutoutWKT=cutout)
      scribe.closeConnections()
      
      timestamp = gmt()
      return self._executeResponse(JobStatus.INITIALIZE, "Process Accepted", timestamp)
   
