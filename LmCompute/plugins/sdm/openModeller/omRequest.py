"""
@summary: Module used to create openModeller requests
@author: CJ Grady
@contact: cjgrady@ku.edu
@version: 3.0.0
@status: beta
@note: For openModeller version 1.3.0
@note: Possibly backwards compatible

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
from types import ListType

from LmCommon.common.lmXml import Element, SubElement, fromstring, \
                                        tostring

from LmCompute.common.layerManager import LayerManager

from LmCompute.plugins.sdm.openModeller.localconstants import DEFAULT_FILE_TYPE

# .............................................................................
class OmRequest(object):
   """
   @summary: Base class for openModeller requests
   """
   # .................................
   def __init__(self):
      if self.__class__ == OmRequest:
         raise Exception, "OmRequest base class should not be used directly."
      
   # .................................
   def generate(self):
      raise Exception, "generate method must be overridden by a subclass"

# .............................................................................
class OmModelRequest(OmRequest):
   """
   @summary: Class for generating openModeller model requests
   """
   
   # .................................
   def __init__(self, model, dataDir):
      """
      @summary: openModeller model request constructor
      @param model: The model object to use
      @param dataDir: The directory to store layers in
      @todo: Take options and statistic options as inputs
      """
      self.options = [
                  #("OccurrencesFilter", "SpatiallyUnique"), # Ignore duplicate points (same coordinates)
                  #("OccurrencesFilter", "EnvironmentallyUnique") # Ignore duplicate points (same environment values)
                ]
      self.statOpts = {
                    "ConfusionMatrix" : {
                                           "Threshold" : "0.5"
                                        },
                    "RocCurve" :        {
                                           "Resolution" : "15",
                                           "BackgroundPoints" : "10000",
                                           "MaxOmission" : "1.0"
                                        }
                 }

      self.lyrs = []
      
      lyrMgr = LayerManager(dataDir)
      for lyrUrl in model.layers:
         self.lyrs.append(lyrMgr.getLayerFilename(lyrUrl))

      try:
         self.mask = lyrMgr.getLayerFilename(model.mask)
      except:
         if len(self.lyrs) > 0:
            self.mask = self.lyrs[0]
         else:
            self.mask = ""
      lyrMgr.close()
      self.model = model
      
      if model.algorithm.parameter is not None:
         if isinstance(model.algorithm.parameter, ListType):
            self.algoParams = model.algorithm.parameter
         else:
            self.algoParams = [model.algorithm.parameter]
      else:
         self.algoParams = []
      
   # .................................
   def generate(self):
      """
      @summary: Generates a model request string by building an XML tree and 
                   then serializing it
      """
      # Parent Element
      reqEl = Element("ModelParameters")
      
      # Sampler Element
      samplerEl = SubElement(reqEl, "Sampler")
      envEl = SubElement(samplerEl, "Environment", 
                                  attrib={"NumLayers": str(len(self.lyrs))})
      for lyr in self.lyrs:
         SubElement(envEl, "Map", attrib={"Id": lyr, "IsCategorical": "0"})
      SubElement(envEl, "Mask", attrib={"Id": self.mask})
      
      presenceEl = SubElement(samplerEl, "Presence", 
                              attrib={"Label": self.model.points.displayName})
      SubElement(presenceEl, "CoordinateSystem", value=self.model.points.wkt)
      for pt in self.model.points.point:
         SubElement(presenceEl, "Point", attrib={"Id": pt.id, 
                                                  "X": pt.x, 
                                                  "Y": pt.y})

      # Algorithm Element
      algoEl = SubElement(reqEl, "Algorithm", 
                          attrib={"Id": self.model.algorithm.code})
      algoParamsEl = SubElement(algoEl, "Parameters")
      for param in self.algoParams:
         SubElement(algoParamsEl, "Parameter", 
                    attrib={"Id": param.id, "Value": param.value})
      
      # Options Element
      optionsEl = SubElement(reqEl, "Options")
      for name, value in self.options:
         SubElement(optionsEl, name, value=value)

      # Statistics Element      
      statsEl = SubElement(reqEl, "Statistics")
      SubElement(statsEl, "ConfusionMatrix", attrib={
                   "Threshold": self.statOpts['ConfusionMatrix']['Threshold']})
      SubElement(statsEl, "RocCurve", attrib={
           "Resolution": self.statOpts['RocCurve']['Resolution'], 
           "BackgroundPoints": self.statOpts['RocCurve']['BackgroundPoints'],
           "MaxOmission": self.statOpts['RocCurve']['MaxOmission']})

      return tostring(reqEl)

# .............................................................................
class OmProjectionRequest(OmRequest):
   """
   @summary: Class for generating openModeller projection requests
   """
   
   # .................................
   def __init__(self, projection, dataDir):
      """
      @summary: Constructor for OmProjectionRequest class
      @param projection: The projection object to use
      @param dataDir: The directory to store layers in
      """
      self.lyrs = []
      
      lyrMgr = LayerManager(dataDir)
      for lyrUrl in projection.layers:
         self.lyrs.append(lyrMgr.getLayerFilename(lyrUrl))

      try:
         self.mask = lyrMgr.getLayerFilename(projection.mask)
      except:
         if len(self.lyrs) > 0:
            self.mask = self.lyrs[0]
         else:
            self.mask = ""
      lyrMgr.close()
      
      self.algorithmSection = projection.algorithm
      self.layers = self.lyrs
      self.fileType = DEFAULT_FILE_TYPE
      self.templateLayer = self.mask
      
   # .................................
   def generate(self):
      """
      @summary: Generates a projection request string by generating an XML tree
                   and then serializing it
      """
      reqEl = Element("ProjectionParameters")
      # Append algorithm section
      reqEl.append(fromstring(self.algorithmSection))
      
      # Environment section
      envEl = SubElement(reqEl, "Environment",  
                         attrib={"NumLayers": str(len(self.layers))})
      for lyr in self.layers:
         SubElement(envEl, "Map", attrib={"Id": lyr, "IsCategorical": "0"})
      SubElement(envEl, "Mask", attrib={"Id": self.mask})
      
      # OutputParameters Element
      opEl = SubElement(reqEl, "OutputParameters",  
                                         attrib={"FileType": self.fileType})
      SubElement(opEl, "TemplateLayer", attrib={"Id": self.templateLayer})
      
      return tostring(reqEl)

