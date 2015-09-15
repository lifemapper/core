"""
@summary: Module containing functions to format Lifemapper SDM Scenarios into
             EML formatted ElementTree elements
@author: CJ Grady
@version: 
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
from LmCommon.common.lmXml import Element, fromstring, PI, QName, \
                                  register_namespace, setDefaultNamespace, \
                                  SubElement, tostring

from LmServer.base.utilities import formatTimeYear, getPackageId

from LmWebServer.common.lmconstants import XSI_NAMESPACE
from LmWebServer.lmEml.contact import addContactElement
from LmWebServer.lmEml.customUnits import processCustomUnits
from LmWebServer.lmEml.emlConstants import EML_KNOWN_FEATURES, EML_NAMESPACE, \
                                           EML_SCHEMA_LOCATION, EML_SYSTEM
from LmWebServer.lmEml.serviceRaster import addServiceRaster

# .............................................................................
def buildSdmScenarioPackage(scn):
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
   
   register_namespace('', EML_NAMESPACE)
   setDefaultNamespace(EML_NAMESPACE)   
   el = Element("eml", attrib={
                          "packageId" : getPackageId(scn, separator='.'),
                          "system" : EML_SYSTEM,
                          QName(XSI_NAMESPACE, "schemaLocation") : \
                                "%s\n%s" % (EML_NAMESPACE, EML_SCHEMA_LOCATION)
                              })

   
   dsEl = SubElement(el, "dataset", 
                         attrib={"id" : getPackageId(scn, separator='.')})
   SubElement(dsEl, "title", value=scn.title)
   SubElement(SubElement(dsEl, "creator"), 
              "organizationName", 
              value="Lifemapper")
   
   if len(scn.keywords) > 0:
      kwsEl = SubElement(dsEl, "keywordSet")
      for kw in scn.keywords:
         SubElement(kwsEl, "keyword", value=kw)
   
   # Coverages
   coverageEl = SubElement(dsEl, "coverage")

   # Geographic Coverage
   geoCovEl = SubElement(coverageEl, "geographicCoverage")
   SubElement(geoCovEl, "geographicDescription", 
                                value="Bounding Box (EPSG:%s) - %s" % (
                                                  scn.epsgcode, str(scn.bbox)))
   boundCoordsEl = SubElement(geoCovEl, "boundingCoordinates")
   if scn.epsgcode == 4326:
      bbox = scn.bbox
   else:
      bboxPts = scn.translatePoints([(scn.bbox[0], scn.bbox[1]), 
                                     (scn.bbox[2], scn.bbox[3])], 
                                    srcEPSG=scn.epsgcode, dstEPSG=4326)
      bbox = (bboxPts[0][0], bboxPts[0][1], bboxPts[1][0], bboxPts[1][1])
   SubElement(boundCoordsEl, "westBoundingCoordinate", value=round(bbox[0], 3))
   SubElement(boundCoordsEl, "eastBoundingCoordinate", value=round(bbox[2], 3))
   SubElement(boundCoordsEl, "northBoundingCoordinate", value=round(bbox[3], 3))
   SubElement(boundCoordsEl, "southBoundingCoordinate", value=round(bbox[1], 3))
   
   # Temporal Coverage
   tempCovEl = SubElement(coverageEl, "temporalCoverage")
   rodEl = SubElement(tempCovEl, "rangeOfDates")
   SubElement(SubElement(rodEl, "beginDate"), 
              "calendarDate", 
              value=formatTimeYear(scn.startDate))
   
   SubElement(SubElement(rodEl, "endDate"), 
              "calendarDate", 
              value=formatTimeYear(scn.endDate))

   addContactElement(dsEl)

   customUnits = set()
   for lyr in scn.layers:
      customUnits.union(addServiceRaster(dsEl, lyr))

   if len(customUnits) > 0:
      amEl = SubElement(el, "additionalMetadata")
      mdEl = SubElement(amEl, "metadata")
      ulEl = SubElement(mdEl, "unitList")
      processCustomUnits(ulEl, customUnits)

   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))
