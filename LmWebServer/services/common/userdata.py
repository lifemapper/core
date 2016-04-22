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
import re
from types import ListType
import traceback
import urllib2

from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.common.lmconstants import (DEFAULT_EPSG, DEFAULT_POST_USER, 
               JobStatus, JobStage, HTTPStatus, LM_NAMESPACE)

from LmServer.base.layer import Raster, Vector
from LmServer.base.lmobj import LmHTTPError, LMError
from LmServer.base.utilities import (getMjdTimeFromISO8601, getUrlParameter, 
               getXmlListFromTree, getXmlValueFromTree)
from LmServer.common.lmconstants import Priority, LMServiceModule
from LmServer.common.localconstants import ARCHIVE_USER
from LmServer.db.scribe import Scribe
from LmServer.rad.anclayer import AncillaryRaster, AncillaryVector
from LmServer.rad.palayer import PresenceAbsenceRaster, PresenceAbsenceVector
from LmServer.rad.radbucket import RADBucket
from LmServer.rad.radexperiment import RADExperiment
from LmServer.rad.shapegrid import ShapeGrid
from LmServer.sdm.algorithm import Algorithm
from LmServer.sdm.envlayer import EnvironmentalLayer, EnvironmentalType
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.scenario import Scenario
from LmServer.sdm.sdmexperiment import SDMExperiment
from LmServer.sdm.sdmJob import SDMModelJob, SDMProjectionJob

# TODO: Always throw HTTP errors


#TODO: Move this to constants
MAX_CELLS = 400000

# .............................................................................
def evalBool(value):
   """
   @summary: Evaluate if the string representation of a value is true or false
   @note: This is necessary because str('False') evaluates to True
   """
   if value is None:
      return None
   elif isinstance(value, bool):
      return value
   elif value.lower() == 'false':
      return False
   else: 
      try:
         if int(value) == 0:
            return False
      except: # Will fail if value is not an integer
         pass
      # Anything other than some form of False or 0
      return True

# Try creating a class to process a request and have the attributes found in 
#   the url parameters or the body xml as attributes
# .............................................................................
class RequestParameters(object):
   """
   @summary: This class allows the query of parameter values in either the 
                request URL parameters or the body of the request if it is XML
   """
   # .....................................
   def __init__(self, body, parameters):
      try:
         self.body = fromstring(body)
      except:
         self.body = None
      self.parameters = dict(
                       [(k.lower(), parameters[k]) for k in parameters.keys()])
   
   # .....................................
   def getParameter(self, name, castFunc=lambda x: x, parents=[], returnList=False):
      """
      @summary: Gets a parameter from either the request body XML or URL 
                   parameters
      @param name: The name of the parameter to look for
      @param castFunc: (optional) Casts the value to a specific type
      @param parents: (optional) XML path to the parameter
      """
      if self.parameters.has_key(name.lower()):
         val = getUrlParameter(name.lower(), self.parameters)
         if returnList:
            # Theck was necessary because strings were being split
            if isinstance(val, list):
               val = [castFunc(v) for v in val]
            else:
               val = [castFunc(val)]
         else:
            if val is not None:
               val = castFunc(val)
      else:
         if self.body is None:
            if returnList:
               return []
            else:
               return None
         else:
            try:
               #pth = ["%s" % (LM_NAMESPACE, p) for p in parents]
               pth = parents
               pth.append(name)
               #pth.append("%s" % (LM_NAMESPACE, name))
               if returnList:
                  val = getXmlListFromTree(self.body, pth)
                  val = [castFunc(v) for v in val]
               else:
                  val = getXmlValueFromTree(self.body, pth)
                  if val is not None:
                     val = castFunc(val)
            except Exception, e:
               raise e
      return val

# .............................................................................
class DataPoster(object):
   # ...............................
   def __init__(self, userId, logger):
      """
      """
      self.log = logger
      if userId == ARCHIVE_USER:
         userId = DEFAULT_POST_USER
      self.userId = userId
      self.scribe = Scribe(self.log)
   
   # ...............................
   def __enter__(self):
      self.scribe.openConnections()
      return self
   
   # ...............................
   def __exit__(self, eType, eValue, eTraceback):
      try:
         self.scribe.closeConnections()
      except:
         pass
      
      if eType is not None:
         self.log.error("%s, %s, %s" % (str(eType), str(eValue), str(traceback.format_tb(eTraceback))))
         raise eValue

   # ...............................
   def open(self):
      """
      @summary: Opens the scribe connection
      @note: You do not need to call this if using the 'with' statement
      """
      self.scribe.openConnections()
   
   # ...............................
   def close(self):
      """
      @summary: Closes the scribe connection
      @note: You do not need to call this if using the 'with' statement
      """
      self.scribe.closeConnections()
     
   # ...............................

   def getRADBucketFromXml(self, bucketEl):
      """
      @summary: Builds a RADBucket object from an LM Element Tree object
      @param bucketEl: An ElementTree element with bucket parameters (lmRad:BucketType)
      """
      sgId = getXmlValueFromTree(bucketEl, "shapegridId")
      kws = getXmlValueFromTree(bucketEl, "keywords")
      if sgId is not None:
         shapegrid = self.scribe.getShapeGrid(self.userId, sgId)
      else:
         sgEl = bucketEl.find("{%s}shapegrid" % LM_NAMESPACE)
         name = getXmlValueFromTree(sgEl, "name")
         cellShape = getXmlValueFromTree(sgEl, "cellShape")
         cellSize = float(getXmlValueFromTree(sgEl, "cellSize"))
         mapUnits = getXmlValueFromTree(sgEl, "mapUnits")
         epsgCode = getXmlValueFromTree(sgEl, "epsgCode")
         bounds = getXmlValueFromTree(sgEl, "bounds")
         bbox = sgEl.find("{%s}bbox" % LM_NAMESPACE)
         if bbox is not None:
            minX = getXmlValueFromTree(bbox, "minX")
            minY = getXmlValueFromTree(bbox, "minY")
            maxX = getXmlValueFromTree(bbox, "maxX")
            maxY = getXmlValueFromTree(bbox, "maxY")
         if bounds is not None:
            boundsString = "%s" % bounds.strip()
         else:
            boundsString = "%s, %s, %s, %s" % (minX, minY, maxX, maxY)
         
         try:
            left, bottom, right, top = boundsString
         except:
            left, bottom, right, top = boundsString.strip().split(',')

         numCells = (float(top)-float(bottom))*(float(right)-float(left))/float(cellSize)**2
         if numCells > MAX_CELLS:
            raise LmHTTPError(HTTPStatus.BAD_REQUEST,
               "Too many cells for shapegrid, Max: %s, Requested: %s"\
                  % (MAX_CELLS, numCells))

         if cellShape.lower() == "hexagon":
            cellSides = 6
         elif cellShape.lower() == "square":
            cellSides = 4
         else:
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Undefined shape")
         
         shapegrid = ShapeGrid(name, cellSides, cellSize, mapUnits, 
                epsgCode, boundsString, userId=self.userId)
         neworexistingSG = self.scribe.insertShapeGrid(shapegrid)
         neworexistingSG.buildShape()
      
      # Need metadataUrl or parentMetadataUrl here!
      bucket = RADBucket(neworexistingSG, keywords=kws, 
                         stage=JobStage.GENERAL, 
                         status=JobStatus.INITIALIZE, userId=self.userId)
      return bucket

   # ............................................
   def getRADExperimentFromXml(self, expEl):
      """
      @summary: Builds a RADExperiment object from an ElementTree element
      @param expEl: ElementTree element containing experiment information 
                       (lmRad:experimentType)
      """
      try:
         name = getXmlValueFromTree(expEl, "name")
         epsgCode = int(getXmlValueFromTree(expEl, "epsgCode"))
         email = getXmlValueFromTree(expEl, "email")
         desc = getXmlValueFromTree(expEl, "description")
         exp = RADExperiment(self.userId, name, epsgCode, email=email,
                             description=desc)
         bucketsEl = expEl.find("{%s}buckets" % LM_NAMESPACE)
         if bucketsEl is not None:
            buckets = bucketsEl.findall("{%s}bucket" % LM_NAMESPACE)
            for bucketEl in buckets:
               exp.bucketList.append(self.getRADBucketFromXml(bucketEl))
         
         envLSEl = expEl.find("{%s}envLayerSet" % LM_NAMESPACE)
         if envLSEl is not None:
            exp.setEnvLayerset(self.getRADEnvLayerSetFromXml(envLSEl))
            
         paLSEl = expEl.find("{%s}paLayerSet" % LM_NAMESPACE)
         if paLSEl is not None:
            exp.setOrgLayerset(self.getRADPaLayerSetFromXml(paLSEl))
         
         return exp
      except Exception, e:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Required parameter is missing %s" % str(e))
      

   # ............................................
   def getRADLayerFromXml(self, lyrEl):
      """
      @summary: Gets RAD layer data from an ElementTree element
      @param lyrEl: ElementTree element (lmRad:layerType)
      """
      name = getXmlValueFromTree(lyrEl, "name")
      title = getXmlValueFromTree(lyrEl, "title")

      bounds = getXmlValueFromTree(lyrEl, "bounds")
      bbox = lyrEl.find("{%s}bbox" % LM_NAMESPACE)
      if bbox is not None:
         minX = getXmlValueFromTree(bbox, "minX")
         minY = getXmlValueFromTree(bbox, "minY")
         maxX = getXmlValueFromTree(bbox, "maxX")
         maxY = getXmlValueFromTree(bbox, "maxY")
      if bounds is not None:
         boundsStr = "(%s)" % bounds.strip()
      else:
         boundsStr = "(%s, %s, %s, %s)" % (minX, minY, maxX, maxY)
      startDate = getXmlValueFromTree(lyrEl, "startDate")
      endDate = getXmlValueFromTree(lyrEl, "endDate")
      mapUnits = getXmlValueFromTree(lyrEl, "mapUnits")
      resolution = getXmlValueFromTree(lyrEl, "resolution")
      kws = getXmlValueFromTree(lyrEl, "keywords")
      epsgCode = getXmlValueFromTree(lyrEl, "epsgCode")
      description = getXmlValueFromTree(lyrEl, "description")
      url = getXmlValueFromTree(lyrEl, "layerUrl")
      
      return name, title, boundsStr, startDate, endDate, mapUnits, resolution,\
                kws, epsgCode, description, url
   
   # ............................................
   def getRADRasterLayerFromXml(self, lyrEl):
      """
      @summary: Gets RAD Raster layer data from an ElementTree element
      @param lyrEl: ElementTree element (lmRad:rasterLayer)
      """
      name, title, boundsStr, startDate, endDate, mapUnits, resolution, \
               kws, epsgCode, description, url = self.getRADLayerFromXml(lyrEl)   
      gdalType = getXmlValueFromTree(lyrEl, "gdalType")
      dataFormat = getXmlValueFromTree(lyrEl, "dataFormat")
      return name, title, boundsStr, startDate, endDate, mapUnits, resolution,\
                kws, epsgCode, description, url, gdalType, dataFormat
                
   # ............................................
   def getRADVectorLayerFromXml(self, lyrEl):
      """
      @summary: Gets RAD Vector layer data from an ElementTree element
      @param lyrEl: ElementTree element (lmRad:vectorLayer)
      """
      name, title, boundsStr, startDate, endDate, mapUnits, resolution, \
               kws, epsgCode, description, url = self.getRADLayerFromXml(lyrEl)   
      ogrType = getXmlValueFromTree(lyrEl, "ogrType")
      dataFormat = getXmlValueFromTree(lyrEl, "dataFormat")
      valAttr = getXmlValueFromTree(lyrEl, "valueAttribute")
      return name, title, boundsStr, startDate, endDate, mapUnits, resolution,\
                kws, epsgCode, description, url, ogrType, dataFormat, valAttr
   
   
   # ............................................
   def getRADEnvLayerSetFromXml(self, lsEl):
      """
      @summary: Builds an environmental layer set from XML
      """
      lyrs = []
      for lyrEl in lsEl.findall("{%s}envLayer" % LM_NAMESPACE):
         lyrId = lyrEl.find("{%s}layerId" % LM_NAMESPACE)
         vector = lyrEl.find("{%s}vector" % LM_NAMESPACE)
         raster = lyrEl.find("{%s}raster" % LM_NAMESPACE)
         
         # process parameters
         paramsEl = lyrEl.find("{%s}parameters" % LM_NAMESPACE)
         attrValue = getXmlValueFromTree(paramsEl, "attrValue")
         calcMeth = getXmlValueFromTree(paramsEl, "calculateMethod")
         minPercent = getXmlValueFromTree(paramsEl, "minPercent")
         desc = getXmlValueFromTree(paramsEl, "description")
         
         wm = lc = False
         if calcMeth == "weightedMean":
            wm = True
         elif calcMeth == "largestClass":
            lc = True
         
         if lyrId is not None:
            baseLyr = self.scribe.getRADLayer(lyrid=lyrId)
            if isinstance(baseLyr, Vector):
               lyr = AncillaryVector(baseLyr.name, title=baseLyr.title, 
                        bbox=baseLyr.bbox, startDate=baseLyr.startDate,
                        endDate=baseLyr.endDate, mapunits=baseLyr.mapUnits,
                        resolution=baseLyr.resolution, 
                        epsgcode=baseLyr.epsgcode, ogrType=baseLyr.ogrType,
                        dataFormat=baseLyr.dataFormat, 
                        valAttribute=baseLyr.getValAttribute(),
                        description=desc, attrValue=attrValue,
                        weightedMean=wm, largestClass=lc, 
                        minPercent=minPercent, ancUserId=self.userId, 
                        lyrId=lyrId)
            elif isinstance(baseLyr, Raster):
               lyr = AncillaryRaster(baseLyr.name, title=baseLyr.title, 
                        bbox=baseLyr.bbox, startDate=baseLyr.startDate,
                        endDate=baseLyr.endDate, mapunits=baseLyr.mapUnits,
                        resolution=baseLyr.resolution, 
                        epsgcode=baseLyr.epsgcode, gdalType=baseLyr.gdalType,
                        dataFormat=baseLyr.dataFormat, 
                        valAttribute=baseLyr.getValAttribute(),
                        description=desc, attrValue=attrValue,
                        weightedMean=wm, largestClass=lc, 
                        minPercent=minPercent, ancUserId=self.userId, 
                        lyrId=lyrId)
            else:
               raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Unknown layer type")
         elif raster is not None:
            name, title, boundsStr, startDate, endDate, mapUnits, resolution, \
               kws, epsgCode, description, url, gdalType, dataFormat = \
               self.getRADRasterLayerFromXml(raster)
            lyr = AncillaryRaster(name, title=title, bbox=boundsStr, 
                     startDate=startDate, endDate=endDate, mapunits=mapUnits, 
                     resolution=resolution, epsgcode=epsgCode, 
                     gdalType=gdalType, dataFormat=dataFormat, 
                     description=description, attrValue=attrValue, 
                     weightedMean=wm, largestClass=lc, minPercent=minPercent)
         elif vector is not None:
            name, title, boundsStr, startDate, endDate, mapUnits, resolution, \
               kws, epsgCode, description, url, ogrType, dataFormat, valAttr =\
               self.getRADVectorLayerFromXml(raster)
            lyr = AncillaryVector(name=name, title=title, bbox=boundsStr, 
                     startDate=startDate, endDate=endDate, mapunits=mapUnits, 
                     resolution=resolution, epsgcode=epsgCode, ogrType=ogrType, 
                     dataFormat=dataFormat, valAttribute=valAttr, 
                     description=description, attrValue=attrValue, 
                     weightedMean=wm, largestClass=lc, minPercent=minPercent)
         else:
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="No layer data provided")

         lyrs.append(lyr)
      return lyrs
         
   # ............................................
   def getRADPaLayerSetFromXml(self, lsEl):
      """
      @summary: Builds a presence / absence layer set from XML
      """
      lyrs = []
      for lyrEl in lsEl.findall("{%s}paLayer" % LM_NAMESPACE):
         #lyrId = lyrEl.find("{%s}layerId" % LM_NAMESPACE)
         lyrId = getXmlValueFromTree(lyrEl, "layerId")
         vector = lyrEl.find("{%s}vector" % LM_NAMESPACE)
         raster = lyrEl.find("{%s}raster" % LM_NAMESPACE)
         
         # process parameters
         paramsEl = lyrEl.find("{%s}parameters" % LM_NAMESPACE)
         
         attrPresence = getXmlValueFromTree(paramsEl, "attrPresence")
         minPresence = getXmlValueFromTree(paramsEl, "minPresence")
         maxPresence = getXmlValueFromTree(paramsEl, "maxPresence")
         percentPresence = getXmlValueFromTree(paramsEl, "percentPresence")
         attrAbsence = getXmlValueFromTree(paramsEl, "attrAbsence")
         minAbsence = getXmlValueFromTree(paramsEl, "minAbsence")
         maxAbsence = getXmlValueFromTree(paramsEl, "maxAbsence")
         percentAbsence = getXmlValueFromTree(paramsEl, "percentAbsence")
         
         if lyrId is not None:
            baseLyr = self.scribe.getRADLayer(lyrid=lyrId)
            if baseLyr is None:
               raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Layer %s does not exist" % str(lyrId))
            elif isinstance(baseLyr, Vector):
               lyr = PresenceAbsenceVector(baseLyr.name, title=baseLyr.title, 
                        bbox=baseLyr.bbox, startDate=baseLyr.startDate, 
                        endDate=baseLyr.endDate, mapunits=baseLyr.mapUnits, 
                        resolution=baseLyr.resolution, 
                        keywords=baseLyr.keywords, epsgcode=baseLyr.epsgcode,  
                        ogrType=baseLyr.ogrType, dataFormat=baseLyr.dataFormat,  
                        valAttribute=baseLyr.getValAttribute(), 
                        description=baseLyr.description, 
                        attrPresence=attrPresence, minPresence=minPresence, 
                        maxPresence=maxPresence, 
                        percentPresence=percentPresence, 
                        attrAbsence=attrAbsence, minAbsence=minAbsence, 
                        maxAbsence=maxAbsence, percentAbsence=percentAbsence, 
                        paUserId=self.userId, lyrId=lyrId)
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
                        paUserId=self.userId, lyrId=lyrId)
            else:
               raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                                 msg="Unknown layer type")
               
         elif raster is not None:
            name, title, boundsStr, startDate, endDate, mapUnits, resolution, \
               kws, epsgCode, description, url, gdalType, dataFormat = \
               self.getRADRasterLayerFromXml(raster)
            lyr = PresenceAbsenceRaster(name, title=title, bbox=boundsStr, 
                     startDate=startDate, endDate=endDate, mapunits=mapUnits, 
                     resolution=resolution, keywords=kws, epsgcode=epsgCode, 
                     gdalType=gdalType, dataFormat=dataFormat, 
                     description=description, attrPresence=attrPresence, 
                     minPresence=minPresence, maxPresence=maxPresence, 
                     percentPresence=percentPresence, attrAbsence=attrAbsence, 
                     minAbsence=minAbsence, maxAbsence=maxAbsence, 
                     percentAbsence=percentAbsence, paUserId=self.userId)
         elif vector is not None:
            name, title, boundsStr, startDate, endDate, mapUnits, resolution, \
               kws, epsgCode, description, url, ogrType, dataFormat, valAttr =\
               self.getRADVectorLayerFromXml(raster)
            lyr = PresenceAbsenceVector(name, title=title, bbox=boundsStr, 
                     startDate=startDate, endDate=endDate, mapunits=mapUnits, 
                     resolution=resolution, keywords=kws, epsgcode=epsgCode, 
                     ogrType=ogrType, dataFormat=dataFormat, 
                     valAttribute=valAttr, description=description, 
                     attrPresence=attrPresence, minPresence=minPresence, 
                     maxPresence=maxPresence, percentPresence=percentPresence, 
                     attrAbsence=attrAbsence, minAbsence=minAbsence, 
                     maxAbsence=maxAbsence, percentAbsence=percentAbsence, 
                     paUserId=self.userId)
         else:
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, "No layer data provided")

         lyrs.append(lyr)
      return lyrs
   
   # ............................................
   def postRADAncLayer(self, parameters, body):
      """
      @summary: Process input parameters and add a new ancillary layer to an experiment
      @param parameters: Dictionary of URL parameters
      @param body: Payload of the request (body)
      """
      # Create an empty list that will hold Anc layer parameter tuples
      ancLayerParams = []
      
      try: # Try XML first
         obj = deserialize(fromstring(body))
         
         if obj.DataInputs is not None:
            inputs = obj.DataInputs.Input
            inputs = [obj.DataInputs.Input] if not isinstance(inputs, ListType) else inputs
            # Loop through each entry
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

                  # Add entry to ancLayerParams list
                  ancLayerParams.append(
                     (lyrId, attrValue, weightedMean, largestClass, minPercent)
                  )
         
      except: # Fall back to query parameters
         # Get list of layer ids
         layerIds = getUrlParameter("layerId", parameters)
         attrValue = getUrlParameter("attrValue", parameters)
         weightedMean = getUrlParameter("weightedMean", parameters)
         largestClass = getUrlParameter("largestClass", parameters)
         minPercent = getUrlParameter("minPercent", parameters)
         
         if weightedMean is None:
            weightedMean = False
         else:
            weightedMean = bool(int(weightedMean))
            
         if largestClass is None:
            largestClass = False
         else:
            largestClass = bool(int(largestClass))
         
         # Make layerIds a list if it isn't
         if not isinstance(layerIds, ListType):
            layerIds = [layerIds]
         
         # Loop through layer ids list
         for lyrId in layerIds:
            # Add entry to paLayerParams list
            ancLayerParams.append(
               (lyrId, attrValue, weightedMean, largestClass, minPercent)
            )

      # Get the experiment object
      expId = int(self.parameters["experimentid"])
      exp = self.scribe.getRADExperiment(self.userId, expid=expId)
      
      # Loop through Anc layer parameter tuples
      for lyrId, attrValue, weightedMean, largestClass, minPercent \
            in ancLayerParams:
         if lyrId is not None:
            baseLyr = self.scribe.getRADLayer(lyrid=lyrId)
            if baseLyr is None:
               raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                                 msg="RAD layer %s was not found" % lyrId)
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
                                     ancUserId=self.userId,
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
                                     ancUserId=self.userId,
                                     lyrId=lyrId,
                                     lyrUserId=baseLyr.user)
            
            # Add the ancillary layer
            ancLyr = self.scribe.insertAncillaryLayer(lyr, exp)
         else:
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                              msg="Must specify a layer to use")
      # Return the last ancillary layer
      # TODO: Evaluate what we want to return here
      return ancLyr
   
   # ............................................
   def postRADBucket(self, parameters, body):
      """
      @summary: Process input parameters and post a new RAD bucket
      @param parameters: Dictionary of url parameters
      @param body: Payload of the request (body)
      """
      if body is not None:
         reqBody = fromstring(body)
         expId = getXmlValueFromTree(reqBody, "experimentId")
         bucketEl = reqBody.find("{%s}bucket" % LM_NAMESPACE)
         bucket = self.getRADBucketFromXml(bucketEl)
         exp = self.scribe.getRADExperiment(self.userId, expid=expId)
         bucket.setParentMetadataUrl(exp.metadataUrl)
         self.scribe.insertBucket(bucket, exp)
         return bucket
      else:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, "Need to provide XML for posting bucket at this time")
   
   
   # ............................................
   def postRADExperiment(self, parameters, body):
      """
      @summary: Process input parameters and post a new RAD experiment
      @param parameters: Dictionary of url parameters
      @param body: Payload of the request (body)
      """
      if body is not None:
         reqBody = fromstring(body)
         expEl = reqBody.find("{%s}experiment" % LM_NAMESPACE)
         exp = self.getRADExperimentFromXml(expEl)

      else:
         name = getUrlParameter("name", parameters)
         epsgCode = getUrlParameter("epsgCode", parameters)
         email = getUrlParameter("email", parameters)
         desc = getUrlParameter("description", parameters)
         exp = RADExperiment(self.userId, name, epsgCode, email=email, 
                                                              description=desc)
         shapegridId = getUrlParameter("shapegridId", parameters)
         if shapegridId is not None:
            shpGrd = self.scribe.getShapeGrid(self.userId, shapegridId)
            bkt = RADBucket(shpGrd)
            exp.addBucket(bkt)
      
      exp = self.scribe.insertRADExperiment(exp)
      
      #exp.setId(expId)
      for bucket in exp.bucketList:
         bucket.setParentMetadataUrl(exp.metadataUrl)
         bucketId = self.scribe.insertBucket(bucket, exp)

      return exp
   
   # ............................................
   def postRADLayer(self, parameters, body):
      """
      @summary: Posts a new RAD layer
      """
      layerContent = None
      try:
         layerEl = fromstring(body)
         name, title, boundsStr, startDate, endDate, mapUnits, resolution,\
                keywords, epsgCode, description, layerUrl = self.getRADLayerFromXml(layerEl)
         # Just get Raster or Vector, then read with GDAL or OGR
         isRaster = getXmlValueFromTree(layerEl, "raster")
         isVector = getXmlValueFromTree(layerEl, "vector")
         dataFormat = getXmlValueFromTree(layerEl, "dataFormat")
         valueAttribute = getXmlValueFromTree(layerEl, "valueAttribute")
      except:
         name = getUrlParameter("name", parameters)
         title = getUrlParameter("title", parameters)
         boundsStr = getUrlParameter("bbox", parameters)
         startDate = getUrlParameter("startDate", parameters)
         endDate = getUrlParameter("endDate", parameters)
         mapUnits = getUrlParameter("mapUnits", parameters)
         resolution = getUrlParameter("resolution", parameters)
         keywords = getUrlParameter("keyword", parameters)
         epsgCode = getUrlParameter("epsgCode", parameters)
         description = getUrlParameter("description", parameters)
         layerUrl = getUrlParameter("layerUrl", parameters)
         isRaster = getUrlParameter("raster", parameters)
         isVector = getUrlParameter("vector", parameters)
         dataFormat = getUrlParameter("dataFormat", parameters)
         valueAttribute = getUrlParameter("valueAttribute", parameters)
         
         if body is not None and len(body.strip()) > 1:
            layerContent = body
      
      if layerContent is None:
         if layerUrl is not None:
            layerContent = urllib2.urlopen(layerUrl).read()
         else:
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                              msg="Layer content not specified in post body or layerUrl parameter")

      if epsgCode is None:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Please provide an EPSG code for the layer")
      if mapUnits is None:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Please provide the map units for the layer")
      
      if isVector:
         lyr = Vector(name=name, title=title, bbox=boundsStr, 
                      startDate=startDate, endDate=endDate, mapunits=mapUnits, 
                      resolution=resolution, epsgcode=epsgCode, 
                      ogrFormat=dataFormat, 
                      valAttribute=valueAttribute, keywords=keywords, 
                      description=description, lyrUserId=self.userId,
                      moduleType=LMServiceModule.RAD)
      elif isRaster:
         lyr = Raster(name=name, title=title, bbox=boundsStr, 
                      gdalFormat=dataFormat, 
                      startDate=startDate, endDate=endDate, mapunits=mapUnits, 
                      resolution=resolution, epsgcode=epsgCode,
                      keywords=keywords, description=description, 
                      lyrUserId=self.userId, moduleType=LMServiceModule.RAD)
      else:
         raise LmHTTPError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, 
                           msg="Cannot determine type of layer")

      lyr.readFromUploadedData(layerContent)
      lyrTempFile = lyr.getDLocation()
      # scribe function deletes temp data after successful insert/write
      updatedLyr, isNewLyr = self.scribe.insertRADLayer(lyr, 
                                                        lyrTempFile=lyrTempFile)
      return updatedLyr
   
   # ............................................
   def postRADPALayer(self, parameters, body):
      """
      @summary: Posts a new RAD PA layer for an experiment
      @param parameters: A dictionary of URL parameters
      @param body: The body of the request
      """
      # Create an empty list that will hold PA layer parameter tuples
      paLayerParams = []
      
      try: # Try XML first
         obj = deserialize(fromstring(body))
         
         if obj.DataInputs is not None:
            inputs = obj.DataInputs.Input
            inputs = [obj.DataInputs.Input] if not isinstance(inputs, ListType) else inputs
            # Loop through each entry
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
                  
                  # Add entry to paLayerParams list
                  paLayerParams.append(
                     (lyrId, attrPresence, minPresence, maxPresence, 
                      percentPresence, attrAbsence, minAbsence, maxAbsence, 
                      percentAbsence)
                  )
         
      except: # Fall back to query parameters
         # Get list of layer ids
         layerIds = getUrlParameter("layerId", parameters)
         attrPresence = getUrlParameter("attrPresence", parameters)
         minPresence = getUrlParameter("minPresence", parameters)
         maxPresence = getUrlParameter("maxPresence", parameters)
         percentPresence = getUrlParameter("percentPresence", parameters)
         attrAbsence = getUrlParameter("attrAbsence", parameters)
         minAbsence = getUrlParameter("minAbsence", parameters)
         maxAbsence = getUrlParameter("maxAbsence", parameters)
         percentAbsence = getUrlParameter("percentAbsence", parameters)
      
         # Make layerIds a list if it isn't
         if not isinstance(layerIds, ListType):
            layerIds = [layerIds]
         
         # Loop through layer ids list
         for lyrId in layerIds:
            # Add entry to paLayerParams list
            paLayerParams.append(
               (lyrId, attrPresence, minPresence, maxPresence, 
                percentPresence, attrAbsence, minAbsence, maxAbsence, 
                percentAbsence)
            )

      # Get the experiment object
      expId = int(self.parameters["experimentid"])
      exp = self.scribe.getRADExperiment(self.userId, expid=expId)
      
      # Loop through PA layer parameter tuples
      for lyrId, attrPresence, minPresence, maxPresence, percentPresence, \
          attrAbsence, minAbsence, maxAbsence, percentAbsence in paLayerParams:
         if lyrId is not None:
            baseLyr = self.scribe.getRADLayer(lyrid=lyrId)
            if baseLyr is None:
               raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                                 msg="RAD layer %s was not found" % lyrId)
            elif isinstance(baseLyr, Vector):
               lyr = PresenceAbsenceVector(baseLyr.name, title=baseLyr.title, 
                        bbox=baseLyr.bbox, startDate=baseLyr.startDate, 
                        endDate=baseLyr.endDate, mapunits=baseLyr.mapUnits, 
                        resolution=baseLyr.resolution, 
                        keywords=baseLyr.keywords, epsgcode=baseLyr.epsgcode,  
                        ogrType=baseLyr.ogrType, dataFormat=baseLyr.dataFormat,  
                        valAttribute=baseLyr.getValAttribute(), 
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
            
            # Add the presence absence layer
            paLyr = self.scribe.insertPresenceAbsenceLayer(lyr, exp, 
                                                           rollback=True)
         else:
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                              msg="Must specify a layer to use")
      # Return the last PA layer
      # TODO: Evaluate what we want to return here
      return paLyr

   # ............................................
   def postRADShapegrid(self, parameters, body):
      """
      @todo: This should move to LmWebServer.services.rad.processes
      @summary: Posts a new RAD shapefile from the parameters provided
      @param parameters: A dictionary of URL parameters
      @param body: The body of the request
      """
      if body is not None:
         shp = deserialize(fromstring(body)).shapegrid
         name = shp.name
         cellShape = shp.cellShape
         cellSize = float(shp.cellSize)
         mapUnits = shp.mapUnits
         epsgCode = shp.epsgCode      

         bounds = shp.bounds
         try:
            cutout = shp.cutout.strip()
         except:
            cutout = None
      else:
         # TODO: please add size parameter!
         name = getUrlParameter("name", parameters)
         cellShape = getUrlParameter("cellShape", parameters)
         cellSize = getUrlParameter("cellSize", parameters)
         mapUnits = getUrlParameter("mapUnits", parameters)
         epsgCode = getUrlParameter("epsgCode", parameters)
         bounds = getUrlParameter("bbox", parameters)
         cutout = getUrlParameter("cutout", parameters)
      
      if cellShape.lower() == "hexagon":
         cellSides = 6
      elif cellShape.lower() == "square":
         cellSides = 4
      else:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Undefined cell shape")
      
      shapegrid = ShapeGrid(name, cellSides, cellSize, mapUnits, epsgCode, 
                           bounds, userId=self.userId)
      neworexistingSG = self.scribe.insertShapeGrid(shapegrid, cutout=cutout)
      #TODO: Enable job
      #sgJob = self.scribe.initRADBuildGrid(self.userId, shapegrid)
      return neworexistingSG

   # ............................................
   def _postSDMExperiment(self, algo, occSetId, mdlScnId, prjScnIds, name, 
                       description, email, mdlMaskId=None, prjMaskId=None):
      """
      @summary: Posts an experiment from parameters (used for REST and WPS)
      """
      if mdlScnId is None or algo is None or occSetId is None:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Required parameter was None")         
      
      mdlScen = self.scribe.getScenario(mdlScnId)

      pMask = None
      mMask = None
      
      if mdlMaskId is not None:
         mMask = self.scribe.getLayer(mdlMaskId)
      if prjMaskId is not None:
         pMask = self.scribe.getLayer(prjMaskId)
      occ = self.scribe.getOccurrenceSet(occSetId)
      
      if occ.epsgcode != mdlScen.epsgcode:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
            msg="Occurrence set %s EPSG (%s) does not match scenario %s EPSG (%s)" \
             % (occ.getId(), occ.epsgcode, mdlScen.getId(), mdlScen.epsgcode))

      prjScens = []
      # CJG - 06/22/2015
      #   The Maxent replicates parameter changes the output of Maxent models
      #      so that there are multiple .lambdas files.  We are set up to 
      #      handle that situation so we will just provide them in the 
      #      experiment package and will not allow projections
      if not (algo.code == 'ATT_MAXENT' and int(algo.parameters['replicates']) > 1):
         for psid in prjScnIds:
            prjScn = self.scribe.getScenario(psid)
            prjScens.append(prjScn)
            if occ.epsgcode != mdlScen.epsgcode:
               raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                 msg="Occurrence set %s EPSG (%s) does not match scenario %s EPSG (%s)" \
                   % (occ.getId(), occ.epsgcode, prjScn.getId(), prjScn.epsgcode))
      else:
         if len(prjScnIds) > 0:
            self.log.debug("Removed projection scenarios.")
            try:
               import cherrypy
               msg = "Algorithm / parameter combination caused projection scenarios to be removed"
               cherrypy.response.headers['LM-message'] = msg
            except Exception, e:
               self.log.debug("Failed to add message to headers: %s" % str(e))
      # if occ = ARCHIVE_USER or (occ = anon and user != anon), copy to new occ 
      if (occ.getUserId() == ARCHIVE_USER or 
          (occ.getUserId() == DEFAULT_POST_USER and 
           self.userId != DEFAULT_POST_USER)):
         
         newOcc = OccurrenceLayer(occ.displayName, name=occ.name,
                                  epsgcode=occ.epsgcode, bbox=occ.bbox,
                                  userId=self.userId, status=JobStatus.COMPLETE)
         newOcc.readShapefile(dlocation=occ.getDLocation())
         occId = self.scribe.insertOccurrenceSet(newOcc)
         newOcc.writeShapefile()         
         # Set occ to the newOcc b/ occ initializes the job
         occ = newOcc

      jobs = self.scribe.initSDMModelProjectionJobs(occ, mdlScen, prjScens, 
                                  algo, self.userId, Priority.REQUESTED, 
                                  mdlMask=mMask, prjMask=pMask, email=email, 
                                  name=name, description=description)
      mdl = None
      prjs = []
      for job in jobs:
         if isinstance(job, SDMModelJob):
            mdl = job.dataObj
         elif isinstance(job, SDMProjectionJob):
            prjs.append(job.dataObj)
      
      exp = SDMExperiment(mdl, prjs)


#       experiments = [self.scribe.initSDMExperiment(occSet, mdlScen, prjScens, 
#                         algo, self.userId, Priority.REQUESTED, mdlMask=mMask, 
#                         prjMask=pMask, email=email, name=name, 
#                         description=description) for occSet in occSets]
      
      return exp
      
   # ............................................
   def postSDMExperimentRest(self, parameters, body):
      """
      @summary: Inserts a new experiment via a REST web service HTTP POST
      @param parameters: URL parameters
      @param body: The request body
      """
      reqParams = RequestParameters(body, parameters)
      mdlScnId = reqParams.getParameter("modelScenario", castFunc=int, 
                                      parents=["experiment"], returnList=False)
      mdlMaskId = reqParams.getParameter("modelMask", castFunc=int, 
                                      parents=["experiment"], returnList=False)
      prjScnIds = reqParams.getParameter("projectionScenario", castFunc=int, 
                                       parents=["experiment"], returnList=True)
      prjMaskId = reqParams.getParameter("projectionMask", castFunc=int, 
                                      parents=["experiment"], returnList=False)
      email = reqParams.getParameter("email", castFunc=str, 
                                     parents=["experiment"], returnList=False)
      name = reqParams.getParameter("name", castFunc=str, 
                                    parents=["experiment"], returnList=False)
      desc = reqParams.getParameter("description", castFunc=str, 
                                    parents=["experiment"], returnList=False)
      occSetId = reqParams.getParameter("occurrenceSetId", castFunc=int, 
                                      parents=["experiment"], returnList=False)
      
      algoCode = reqParams.getParameter("algorithmCode", castFunc=str, 
                                        parents=["experiment", "algorithm"], 
                                        returnList=False).upper()
      algo = Algorithm(algoCode)
      algo.fillWithDefaults()
      for x in algo.parameters.keys():
         val = reqParams.getParameter(x, 
                             parents=["experiment", "algorithm", "parameters"], 
                             returnList=False)
         if val is not None:
            if val.find('.') >= 0 or val.lower().find('e') >= 0:
               val = float(val)
            else:
               if re.match(r'\d+', val):
                  val = int(val)
            algo.setParameter(x, val)
      
      return self._postSDMExperiment(algo, occSetId, mdlScnId, prjScnIds, name,
                         desc, email, mdlMaskId=mdlMaskId, prjMaskId=prjMaskId)
   
   # ............................................
   def postSDMExperimentWPS(self, sdmExp):
      """
      @summary: Posts an SDM Experiment from an object retrieved from a WPS 
                   service
      @param sdmExp: SDM Experiment object deserialized from XML post
      @return: List of experiment ids
      @rtype: [integer]
      """
      algoRaw = None
      mdlScnId = None
      occSetId = None
      mdlMaskId = None
      prjMaskId = None
      email = None
      prjScnIds = []
      
      for input in sdmExp.DataInputs.Input:
         if input.Identifier == "algorithm":
            algoRaw = input.Data.ComplexData
         elif input.Identifier == "projectionScenarioId":
            prjScnIds.append(int(input.Data.LiteralData))
         elif input.Identifier == "modelScenarioId":
            mdlScnId = int(input.Data.LiteralData)
         elif input.Identifier == "occurrenceSetId":
            occSetId = int(input.Data.LiteralData)
         elif input.Identifier == "modelMaskId":
            mdlMaskId = int(input.Data.LiteralData)
         elif input.Identifier == "projectionMaskId":
            prjMaskId = int(input.Data.LiteralData)
         elif input.Identifier == "email":
            email = input.Data.LiteralData.strip()
         elif input.Identifier == "name":
            name = input.Data.LiteralData.strip()
         elif input.Identifier == "description":
            desc = input.Data.ListeralData.strip()
            
      if mdlScnId is None or algoRaw is None or occSetId is None:
         raise Exception, "Required parameter was None"         
      
      algoCode = algoRaw.algorithmCode
      alg = Algorithm(algoCode)
      alg.fillWithDefaults()
      for x in alg.parameters.keys():
         try:
            val = algoRaw.parameters.__getattribute__(x)
            if val.find('.') >= 0:
               val = float(val)
            else:
               if re.match(r'\d+', val):
                  val = int(val)
            alg.setParameter(x, val)
         except Exception, e:
            pass

      return self._postSDMExperiment(alg, occSetId, mdlScnId, prjScnIds, name,
                         desc, email, mdlMaskId=mdlMaskId, prjMaskId=prjMaskId)
   
   # ............................................
   def postSDMLayerTypeCode(self, parameters, body):
      """
      @summary: Posts a new layer type code to the database
      @param parameters: Dictionary of url parameters
      @param body: The payload of a request
      """
      modTime = None
      userId = self.userId
      envTypeId = None
      
      reqParams = RequestParameters(body, parameters)
      envType = reqParams.getParameter("code", castFunc=str, 
                                       parents=["typeCode"], returnList=False)
      title = reqParams.getParameter("title", castFunc=str,
                                     parents=["typeCode"], returnList=False)
      description = reqParams.getParameter("description", castFunc=str,
                                        parents=["typeCode"], returnList=False)
      kws = reqParams.getParameter("keyword", castFunc=str,
                             parents=["typeCode", "keywords"], returnList=True)

      et = EnvironmentalType(envType, title, description, userId, 
                                modTime=modTime, keywords=kws,
                                environmentalTypeId=envTypeId)
         
      self.scribe.getOrInsertLayerTypeCode(et)
      return et
      
   # ............................................
   def postSDMLayer(self, parameters, body):
      """
      @summary: Posts an environmental layer to the database
      @param parameters: Dictionary of url parameters
      @param body: The payload of a request
      """
      reqParams = RequestParameters(body, parameters)
      name = reqParams.getParameter("name", parents=["layer"])
      title = reqParams.getParameter("title", parents=["layer"])
      valUnits = reqParams.getParameter("valUnits", parents=["layer"])
      startDate = reqParams.getParameter("startDate", parents=["layer"])
      endDate = reqParams.getParameter("endDate", parents=["layer"])
      units = reqParams.getParameter("units", parents=["layer"])
      resolution = reqParams.getParameter("resolution", parents=["layer"])
      epsgCode = reqParams.getParameter("epsgCode", parents=["layer"], 
                                        castFunc=int)
      keywords = reqParams.getParameter("keyword", parents=["layer"], 
                                        returnList=True)
      envLayerType = reqParams.getParameter("envLayerType", parents=["layer"])
      envLayerTypeId = reqParams.getParameter("envLayerTypeId", 
                                              parents=["layer"])
      description = reqParams.getParameter("description", parents=["layer"])
      dataFormat = reqParams.getParameter("dataFormat", parents=["layer"])
      layerUrl = reqParams.getParameter("layerUrl", parents=["layer"])
      isCategorical = reqParams.getParameter("isCategorical", 
                                             parents=["layer"], castFunc=evalBool)

      if body is not None:
         rstCont = body
      elif layerUrl is not None:
         rstCont = urllib2.urlopen(layerUrl).read()
      else:
         rstCont = reqParams.getParameter("layerFile")
      
      #Convert start date and end date
      startDate = getMjdTimeFromISO8601(startDate)
      endDate = getMjdTimeFromISO8601(endDate)
      
      if epsgCode is None:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                           msg="Please provide an EPSG code for the layer")
      if units is None:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                       msg="Please provide the map units (units)for the layer")
      
      
      if rstCont is not None:
         lyr = EnvironmentalLayer(name, title=title, valUnits=valUnits,
                        startDate=startDate, endDate=endDate, 
                        mapunits=units, resolution=resolution, 
                        epsgcode=epsgCode, keywords=keywords,
                        layerType=envLayerType,layerTypeId=envLayerTypeId, 
                        description=description, userId=self.userId,
                        gdalFormat=dataFormat, isCategorical=isCategorical)

         existLyrs = self.scribe.getEnvLayersByNameUserEpsg(lyr.name, 
                                                lyr.getUserId(), lyr.epsgcode)
         
         if len(existLyrs) == 0:
            try:
               lyr.writeLayer(srcData=rstCont)
               lyr = self.scribe.insertLayer(lyr)
            except LMError, e:
               try:
                  self.scribe.deleteEnvLayer(lyr)
               except:
                  pass
               raise e
         else:
            raise LmHTTPError(HTTPStatus.CONFLICT, 
                   msg='Layer with name %s, user %s, epsg %d exists with id %d'
                       % (existLyrs[0].name, existLyrs[0].getUserId(), 
                          existLyrs[0].epsgcode, existLyrs[0].getId()))
         return lyr
      else:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                           msg="Error: Raster content missing")
   
   # ............................................
   def postSDMOccurrenceSet(self, parameters, body):
      """
      @summary: Posts an occurrence set to the database
      @param postRequest: XML post request
      @todo: Do inputs need to be validated?
      """
      reqParams = RequestParameters(body, parameters)
      
      self.log.debug(str(reqParams.parameters))
      
      uploadedType = ""
      tmp = reqParams.getParameter("pointsType")
      if tmp is not None:
         uploadedType = tmp.lower()
         if uploadedType not in ('shapefile', 'csv'):
            raise LmHTTPError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, 
                              msg="Points format: %s is not supported." % uploadedType)
      
      displayName = reqParams.getParameter("displayName")
      
      self.log.debug(reqParams.getParameter('displayname'))
      
      if displayName is None:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, msg="Must supply a display name when posting an occurrence set")
      
      # Check to see if points were posted in payload or if url parameter
      if body is None:
         body = reqParams.getParameter("points")
      if body is None: # if body is still None
         body = parameters.get('request').file.read()
         #body = reqParams.getParameter("request")

      epsgInput = reqParams.getParameter("epsgCode")
      
      # Get the default EPSG code for the user.  If it was not supplied, this will be used
      if self.userId == "changeThinking":
         epsgCode = 2163
      else:
         epsgCode = DEFAULT_EPSG

      if epsgInput is not None:
         try:
            epsgCode = int(epsgInput)
         except Exception, e:
            self.log.error("Trying to get the epsg code entered by the user: %s" % str(epsgInput))
            self.log.error(str(e))
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                           msg="EPSG code entered was invalid: %s" % epsgInput)
      
      name = reqParams.getParameter("name")
      
      occ = OccurrenceLayer(displayName, name=name, userId=self.userId, 
                            epsgcode=epsgCode)
      
      if uploadedType.lower() == 'csv':
         # CSV files in DOS format need to have \r removed
         body = body.replace('\r\n', '\n')
         body = body.replace('\r', '\n')
      
      occ.readFromUploadedData(body, uploadedType=uploadedType)
      tmplocation = occ.getDLocation()
      occ.clearDLocation()
      occ.setDLocation()
      occId = self.scribe.insertOccurrenceSet(occ)         
      success = occ.writeShapefile(overwrite=True)
      occ.deleteData(dlocation=tmplocation, isTemp=True)
      
      if success:
         occ.updateStatus(JobStatus.COMPLETE, queryCount=occ.count)
         success = self.scribe.updateOccState(occ)

      if success:
         return occ
      else:
         try:
            occ.updateStatus(JobStatus.IO_OCCURRENCE_SET_WRITE_ERROR)
            self.scribe.updateOccState(occ)
         except:
            pass
         
         msg = 'Error inserting, writing, or updating occurrenceset'
         self.log.error(msg)
         raise LmHTTPError(HTTPStatus.INTERNAL_SERVER_ERROR, 
                           msg="%s.\nparameters:%s\nbody:%s" % (msg, 
                                                str(parameters), str(body)))

   # ............................................
   def postSDMScenario(self, parameters, body):
      """
      @summary: Posts a scenario to the database
      @param parameters: Dictionary of url parameters
      @param body: The payload of a request
      """
      reqParams = RequestParameters(body, parameters)
      
      code = reqParams.getParameter("code", parents=["scenario"])
      title = reqParams.getParameter("title", parents=["scenario"])
      author = reqParams.getParameter("author", parents=["scenario"])
      description = reqParams.getParameter("description", parents=["scenario"])
      startDate = reqParams.getParameter("startDate", parents=["scenario"], 
                                         castFunc=getMjdTimeFromISO8601)
      endDate = reqParams.getParameter("endDate", parents=["scenario"], 
                                       castFunc=getMjdTimeFromISO8601)
      units = reqParams.getParameter("units", parents=["scenario"])
      res = reqParams.getParameter("resolution", parents=["scenario"])
      keywords = reqParams.getParameter("keyword", 
                                        parents=["scenario", "keywords"], 
                                        returnList=True)
      epsgCode = reqParams.getParameter("epsgCode", parents=["scenario"])
      layerIds = reqParams.getParameter("layer", 
                                        parents=["scenario", "layers"], 
                                        returnList=True)
      if code is None:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST,
                           msg="Must provide a code for the new scenario")
      if layerIds is None:
         raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                        msg="Must specify at least one layer for the scenario")
      
      existScn = self.scribe.getScenario(code)
      if existScn is not None:
         raise LmHTTPError(HTTPStatus.CONFLICT, 
                           msg="A scenario already exists (for some user) with code: %s, please use a different code" % code)
      
      layers = []
      for lyrId in layerIds:
         lyr = self.scribe.getLayer(int(lyrId))
         if lyr is None:
            raise LmHTTPError(HTTPStatus.NOT_FOUND, 
                              msg="Error: Layer %s does not exist" % lyrId)
         
         # Check to make sure layers fit the scenario
         # Check EPSG
         if lyr.epsgcode != int(epsgCode):
            raise LmHTTPError(HTTPStatus.BAD_REQUEST, 
                              msg="EPSG code of layer %s does not match the scenario.  %s not equal to %s" % (lyrId, lyr.epsgcode, epsgCode))
         
         # Check user
         if lyr.getUserId() != self.userId and lyr.getUserId() != DEFAULT_POST_USER and lyr.getUserId() != ARCHIVE_USER:
            raise LmHTTPError(HTTPStatus.FORBIDDEN, msg="Permission denied")
         
         layers.append(lyr)

      scn = Scenario(code, title=title, author=author, description=description,
                startdt=startDate, enddt=endDate, units=units, res=res, 
                keywords=keywords, epsgcode=epsgCode, layers=layers, 
                userId=self.userId)

      scnId, _ = self.scribe.insertScenario(scn)
      return scn
