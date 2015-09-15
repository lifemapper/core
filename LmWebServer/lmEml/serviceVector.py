"""
@summary: Module containing functions to build ElementTree elements for EML 
             Service Vectors
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

# .............................................................................
def buildServiceVectorPackage(vec):
   """
   @summary: Builds an EML document based on a service vector object
   """
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
   
   register_namespace('', EML_NAMESPACE)
   setDefaultNamespace(EML_NAMESPACE)
   
   el = Element("eml", attrib={
                        "packageId" : getPackageId(vec, separator='.'),
                        "system" : EML_SYSTEM,
                        QName(XSI_NAMESPACE, "schemaLocation") : \
                              "%s\n%s" % (EML_NAMESPACE, EML_SCHEMA_LOCATION)})
   
   dsEl = SubElement(el, "dataset", 
                     attrib={"id" : getPackageId(vec, separator='.')})
   SubElement(dsEl, "title", value=vec.title)
   SubElement(SubElement(dsEl, "creator"),
              "organizationName",
              value="Lifemapper")
   
   if len(vec.keywords) > 0:
      kwsEl = SubElement(dsEl, "keywordSet")
      for kw in vec.keywords:
         SubElement(kwsEl, "keyword", value=kw)
         
   # Coverages
   coverageEl = SubElement(dsEl, "coverage")
   
   # Geographic Coverage
   geoCovEl = SubElement(coverageEl, "geographicCoverage")
   SubElement(geoCovEl, "geographicDescription", 
              value="Bounding Box (EPSG:%s) - %s" % (vec.epsgcode, 
                                                     str(vec.bbox)))
   boundCoordsEl = SubElement(geoCovEl, "boundingCoordinates")
   if vec.epsgcode == 4326:
      bbox = vec.bbox
   else:
      bboxPts = vec.translatePoints([(vec.bbox[0], vec.bbox[1]),
                                     (vec.bbox[2], vec.bbox[3])],
                                    srcEPSG=vec.epsgcode, dstEPSG=4326)
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
              value=formatTimeYear(vec.startDate))
   
   SubElement(SubElement(rodEl, "endDate"), 
              "calendarDate", 
              value=formatTimeYear(vec.endDate))

   addContactElement(dsEl)

   customUnits = addServiceVector(dsEl, vec)

   if len(customUnits) > 0:
      amEl = SubElement(el, "additionalMetadata")
      mdEl = SubElement(amEl, "metadata")
      ulEl = SubElement(mdEl, "unitList")
      processCustomUnits(ulEl, customUnits)
      
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# .............................................................................
def addServiceVector(el, vec):
   customUnits = set()
   
   svEl = SubElement(el, "spatialVector")
   SubElement(svEl, "entityName", value=vec.name)
   if vec.description is not None:
      SubElement(svEl, "entityDescription", value=vec.description)
   
   physEl = SubElement(svEl, "physical")
   SubElement(physEl, "objectName", value=vec.name)
   dfEl = SubElement(physEl, "dataFormat")
   extDefFrmt = SubElement(dfEl, "externallyDefinedFormat")
   SubElement(extDefFrmt, "formatName", value="Shapefile")

   distribEl = SubElement(physEl, "distribution")
   onlineEl = SubElement(distribEl, "online")
   SubElement(onlineEl, "onlineDescription", value="%s Zipped Shapefile File" % vec.name)

   attListEl = SubElement(svEl, "attributeList")
   for att in [vec._featureAttributes[k] for k in vec._featureAttributes.keys()]:
      attEl = SubElement(attListEl, "attribute", attrib={"id" : att[0]})
      SubElement(attEl, "attributeName", value=att[0])
      SubElement(attEl, "attributeDefinition", value=att[0])
      measScaleEl = SubElement(attEl, "measurementScale")
      nomEl = SubElement(measScaleEl, "nominal")
      nonNumDomEl = SubElement(nomEl, "nonNumericDomain")
      txtDomEl = SubElement(nonNumDomEl, "textDomain")
      SubElement(txtDomEl, "definition", value="Unknown")
   
   SubElement(svEl, "geometry", value="Polygon")
   SubElement(svEl, "geometricObjectCount", value=vec.featureCount)
   
   return customUnits
