"""
@summary: Module containing the FormatterFactory class that will select the 
             Formatter object to return
@author: CJ Grady
@contact: cjgrady@ku.edu
@status: alpha
@version: 1.0
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

@todo: Move constant dictionaries to constants module
"""
from LmCommon.common.lmconstants import JobStatus, HTTPStatus

from LmServer.base.lmobj import LmHTTPError

from LmWebServer.common.lmconstants import ASCIIGRID_INTERFACE, \
      ASCII_OLD_INTERFACE, ATOM_INTERFACE, CSV_INTERFACE, DEFAULT_INTERFACE, \
      GEOTIFF_INTERFACE, HTML_INTERFACE, INDICES_INTERFACE, \
      JSON_INTERFACE, KML_INTERFACE, MODEL_INTERFACE, OGC_INTERFACE, \
      PACKAGE_INTERFACE, PRESENCE_INTERFACE, PROV_INTERFACE, RAW_INTERFACE, \
      SHAPEFILE_INTERFACE, STATISTICS_INTERFACE, STATUS_INTERFACE, \
      TIFF_OLD_INTERFACE, XML_INTERFACE

from LmWebServer.formatters.asciiFormatter import AsciiGridFormatter
from LmWebServer.formatters.atomFormatter import AtomFormatter
from LmWebServer.formatters.csvFormatter import CsvFormatter
#from LmWebServer.formatters.emlFormatter import EmlFormatter
from LmWebServer.formatters.fileFormatter import FileFormatter
from LmWebServer.formatters.jsonFormatter import JsonFormatter
from LmWebServer.formatters.kmlFormatter import KmlFormatter
from LmWebServer.formatters.provFormatter import ProvFormatter
from LmWebServer.formatters.shapefileFormatter import ShapefileFormatter
from LmWebServer.formatters.statisticsFormatter import StatisticsFormatter
from LmWebServer.formatters.statusFormatter import StatusFormatter
from LmWebServer.formatters.tiffFormatter import TiffFormatter
from LmWebServer.formatters.xmlFormatter import StyledXmlFormatter, XmlFormatter

try:
   from LmWebServer.formatters.ogcFormatter import OgcFormatter
   enableOGC = True
except: # Failed import, disable OGC
   enableOGC = False



# Establish a dictionary of formatters - name: Constructor
formats = {
           ASCIIGRID_INTERFACE : AsciiGridFormatter,
           ASCII_OLD_INTERFACE : AsciiGridFormatter,
           ATOM_INTERFACE : AtomFormatter,
           CSV_INTERFACE : CsvFormatter,
           #EML_INTERFACE : EmlFormatter,
           GEOTIFF_INTERFACE : TiffFormatter,
           HTML_INTERFACE : StyledXmlFormatter,
           INDICES_INTERFACE : FileFormatter,
           JSON_INTERFACE : JsonFormatter,
           KML_INTERFACE : KmlFormatter,
           MODEL_INTERFACE : FileFormatter,
           PACKAGE_INTERFACE : FileFormatter,
           PRESENCE_INTERFACE : FileFormatter,
           PROV_INTERFACE : ProvFormatter,
           RAW_INTERFACE : FileFormatter,
           SHAPEFILE_INTERFACE : ShapefileFormatter,
           STATISTICS_INTERFACE : StatisticsFormatter,
           STATUS_INTERFACE : StatusFormatter,
           TIFF_OLD_INTERFACE : TiffFormatter,
           XML_INTERFACE : XmlFormatter,
          }

interfaces = {
              "LmAttList" : [ATOM_INTERFACE, HTML_INTERFACE, 
                             JSON_INTERFACE, XML_INTERFACE],
              "SDMExperiment" : [ATOM_INTERFACE, HTML_INTERFACE, #EML_INTERFACE,  
                  JSON_INTERFACE, KML_INTERFACE, MODEL_INTERFACE, 
                  PACKAGE_INTERFACE, PROV_INTERFACE, STATUS_INTERFACE, 
                  XML_INTERFACE],
              "EnvironmentalLayer" : [ASCIIGRID_INTERFACE, ASCII_OLD_INTERFACE, 
                  ATOM_INTERFACE, GEOTIFF_INTERFACE, #EML_INTERFACE, 
                  HTML_INTERFACE, JSON_INTERFACE, KML_INTERFACE, 
                  PROV_INTERFACE, RAW_INTERFACE, TIFF_OLD_INTERFACE, 
                  XML_INTERFACE],
              "OccurrenceLayer" : [ATOM_INTERFACE, CSV_INTERFACE, #EML_INTERFACE, 
                  HTML_INTERFACE, JSON_INTERFACE, KML_INTERFACE, 
                  PROV_INTERFACE, SHAPEFILE_INTERFACE, XML_INTERFACE],
              "PamSum" : [ATOM_INTERFACE, CSV_INTERFACE, HTML_INTERFACE,
                          JSON_INTERFACE, SHAPEFILE_INTERFACE, XML_INTERFACE],
              "SDMProjection" : [ASCIIGRID_INTERFACE, ASCII_OLD_INTERFACE, 
                  ATOM_INTERFACE, GEOTIFF_INTERFACE, #EML_INTERFACE,  
                  HTML_INTERFACE, JSON_INTERFACE, KML_INTERFACE,  
                  PACKAGE_INTERFACE, PROV_INTERFACE, RAW_INTERFACE, 
                  STATUS_INTERFACE, TIFF_OLD_INTERFACE, XML_INTERFACE],
              "Scenario" : [ATOM_INTERFACE, HTML_INTERFACE, #EML_INTERFACE,  
                  JSON_INTERFACE, XML_INTERFACE],
              "Raster" : [ASCIIGRID_INTERFACE, ASCII_OLD_INTERFACE, 
                  ATOM_INTERFACE, GEOTIFF_INTERFACE, #EML_INTERFACE, 
                  HTML_INTERFACE, JSON_INTERFACE, KML_INTERFACE, RAW_INTERFACE, 
                  TIFF_OLD_INTERFACE, XML_INTERFACE],
              "Vector" : [ATOM_INTERFACE, CSV_INTERFACE, #EML_INTERFACE, 
                  HTML_INTERFACE, JSON_INTERFACE, SHAPEFILE_INTERFACE, 
                  XML_INTERFACE],
              "RADBucket" : [ATOM_INTERFACE, HTML_INTERFACE, #EML_INTERFACE,  
                  JSON_INTERFACE, PRESENCE_INTERFACE, SHAPEFILE_INTERFACE, 
                  XML_INTERFACE],
              "RADExperiment" : [ATOM_INTERFACE, HTML_INTERFACE, #EML_INTERFACE,  
                  INDICES_INTERFACE, JSON_INTERFACE, XML_INTERFACE]
             }
completeInterfaces = [ASCIIGRID_INTERFACE, ASCII_OLD_INTERFACE, CSV_INTERFACE, #EML_INTERFACE, 
     GEOTIFF_INTERFACE, KML_INTERFACE, MODEL_INTERFACE, 
     PACKAGE_INTERFACE, PRESENCE_INTERFACE, PROV_INTERFACE, RAW_INTERFACE, 
     SHAPEFILE_INTERFACE, TIFF_OLD_INTERFACE]

if enableOGC:
   formats[OGC_INTERFACE] = OgcFormatter
   completeInterfaces.append(OGC_INTERFACE)
   interfaces["EnvironmentalLayer"].append(OGC_INTERFACE)
   interfaces["OccurrenceLayer"].append(OGC_INTERFACE)
   interfaces["SDMProjection"].append(OGC_INTERFACE)
   



# .............................................................................
class FormatterFactory(object):
   """
   @summary: Returns a Factory object subclass based on the object and format 
                requested.  It may also consider the parameters given to the 
                constructor.
   """
   # ..................................
   def __init__(self, obj, format=DEFAULT_INTERFACE, parameters={}):
      """
      @summary: Constructor
      @param obj: The object to format
      @param format: (optional) The format to convert the object to
      @param parameters: A dictionary of parameters (such as url parameters) 
                            that may be useful for the Formatter
      """
      self.obj = obj
      self.format = format.lower()
      if not formats.has_key(self.format):
         self.format = DEFAULT_INTERFACE.lower()
      self.parameters = parameters
      self.parameters["lmformat"] = self.format
      typeName = str(obj.__class__).split("'")[1].split('.')[-1]
      if interfaces.has_key(typeName):
         self.parameters["interfaces"] = interfaces[typeName]
         try:
            status = self.obj.status
         except:
            try:
               status = self.obj.model.status
            except:
               status = JobStatus.COMPLETE
         # Remove interfaces that are only present upon completion
         if status != JobStatus.COMPLETE:
            for i in completeInterfaces:
               if i in self.parameters["interfaces"]:
                  self.parameters["interfaces"].remove(i)
   
   # ..................................
   def doGetFormatter(self):
      """
      @summary: Returns a Formatter object subclass from the factory
      @return: A Formatter object subclass
      """
      fmtObj = self._getFormatter()
      return fmtObj

   # ..................................
   def _getFormatter(self):
      """
      @summary: Determines and constructs a Formatter object to be returned
      @rtype: Formatter subclass
      @return: A Formatter object subclass
      """
      if formats.has_key(self.format):
         fmtObj = formats[self.format](self.obj, parameters=self.parameters)
         return fmtObj
      else:
         raise LmHTTPError(HTTPStatus.UNSUPPORTED_MEDIA_TYPE, 
                           msg="Format: %s is not available" % self.format)
   
