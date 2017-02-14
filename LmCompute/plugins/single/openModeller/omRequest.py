"""
@summary: Module used to create openModeller requests
@author: CJ Grady
@contact: cjgrady@ku.edu
@version: 4.0.0
@status: beta
@note: For openModeller version 1.3.0
@note: Possibly backwards compatible

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
from LmCommon.common.lmXml import (Element, SubElement, fromstring, tostring)

from LmCompute.plugins.single.openModeller.constants import DEFAULT_FILE_TYPE 

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
   def __init__(self, points, pointsLabel, coordSys, layerFns, algoJson, 
                maskFn=None):
      """
      @summary: openModeller model request constructor
      @param points: A list of point (id, x, y) tuples
      @param pointsLabel: A label for these points (taxon name)
      @param coordSys: WKT representing the coordinate system of the points
      @param layerFns: A list of layer file names
      @param algoJson: A JSON dictionary of algorithm parameters
      @param maskFn: A mask file name (could be None)
      @todo: Take options and statistic options as inputs
      @todo: Constants
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

      self.points = points
      self.pointsLabel = pointsLabel
      self.csWKT = coordSys
      self.layerFns = layerFns
      self.algoCode = algoJson['algorithmCode']
      self.algoParams = algoJson['parameters']
      self.maskFn = maskFn
      
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
                                 attrib={"NumLayers": str(len(self.layerFns))})
      for lyrFn in self.layerFns:
         SubElement(envEl, "Map", attrib={"Id": lyrFn, "IsCategorical": "0"})
      if self.maskFn is not None:
         SubElement(envEl, "Mask", attrib={"Id": self.maskFn})
      
      presenceEl = SubElement(samplerEl, "Presence", 
                                          attrib={"Label": self.pointsLabel})
      SubElement(presenceEl, "CoordinateSystem", value=self.csWKT)
      
      for ptId, x, y in self.points:
         SubElement(presenceEl, "Point", attrib={"Id": ptId, "X": x, "Y": y})

      # Algorithm Element
      algoEl = SubElement(reqEl, "Algorithm", attrib={"Id": self.algoCode})
      
      algoParamsEl = SubElement(algoEl, "Parameters")
      for param in self.algoParams:
         SubElement(algoParamsEl, "Parameter", 
                    attrib={"Id": param["name"], "Value": param["value"]})
      
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
   def __init__(self, rulesetFn, layerFns, maskFn=None):
      """
      @summary: Constructor for OmProjectionRequest class
      @param rulesetFn: A ruleset file generated by a model
      @param layerFns: A list of layers to project the ruleset on to
      @param maskFn: An optional mask layer for the projection
      """
      self.layerFns = layerFns
      self.maskFn = maskFn
      
      # Get the algorithm section out of the ruleset
      with open(rulesetFn) as rulesetIn:
         ruleset = rulesetIn.read()
         mdlEl = fromstring(ruleset)
         # Find the algorithm element, and pull it out
         self.algEl = mdlEl.find("Algorithm")
      
   # .................................
   def generate(self):
      """
      @summary: Generates a projection request string by generating an XML tree
                   and then serializing it
      """
      reqEl = Element("ProjectionParameters")
      # Append algorithm section
      reqEl.append(self.algEl)
      
      # Environment section
      envEl = SubElement(reqEl, "Environment",  
                         attrib={"NumLayers": str(len(self.layerFns))})
      for lyrFn in self.layerFns:
         SubElement(envEl, "Map", attrib={"Id": lyrFn, "IsCategorical": "0"})
      
      if self.maskFn is None:
         self.maskFn = self.layerFns[0]

      SubElement(envEl, "Mask", attrib={"Id": self.maskFn})
      
      # OutputParameters Element
      opEl = SubElement(reqEl, "OutputParameters",  
                                         attrib={"FileType": DEFAULT_FILE_TYPE})
      SubElement(opEl, "TemplateLayer", attrib={"Id": self.maskFn})
      
      return tostring(reqEl)

