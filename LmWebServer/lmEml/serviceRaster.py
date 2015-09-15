"""
@summary: 
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
from LmWebServer.lmEml.emlConstants import EML_CUSTOM_UNITS, EML_NAMESPACE, \
                                           EML_SCHEMA_LOCATION, \
                                           EML_STANDARD_UNITS, EML_SYSTEM

# .............................................................................
def buildServiceRasterPackage(rst):
   """
   @summary: Builds an EML document based on a service raster object
   """
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
   
   register_namespace('', EML_NAMESPACE)
   setDefaultNamespace(EML_NAMESPACE)   
   el = Element("eml", attrib={
                          "packageId" : getPackageId(rst, separator='.'),
                          "system" : EML_SYSTEM,
                          QName(XSI_NAMESPACE, "schemaLocation") : \
                                "%s\n%s" % (EML_NAMESPACE, EML_SCHEMA_LOCATION)
                              })

   
   dsEl = SubElement(el, "dataset", 
                         attrib={"id" : getPackageId(rst, separator='.')})
   SubElement(dsEl, "title", value=rst.title)
   SubElement(SubElement(dsEl, "creator"), 
              "organizationName", 
              value="Lifemapper")
   
   if len(rst.keywords) > 0:
      kwsEl = SubElement(dsEl, "keywordSet")
      for kw in rst.keywords:
         SubElement(kwsEl, "keyword", value=kw)
   
   # Coverages
   coverageEl = SubElement(dsEl, "coverage")

   # Geographic Coverage
   geoCovEl = SubElement(coverageEl, "geographicCoverage")
   SubElement(geoCovEl, "geographicDescription", 
                                value="Bounding Box (EPSG:%s) - %s" % (
                                                  rst.epsgcode, str(rst.bbox)))
   boundCoordsEl = SubElement(geoCovEl, "boundingCoordinates")
   if rst.epsgcode == 4326:
      bbox = rst.bbox
   else:
      bboxPts = rst.translatePoints([(rst.bbox[0], rst.bbox[1]), 
                                     (rst.bbox[2], rst.bbox[3])], 
                                    srcEPSG=rst.epsgcode, dstEPSG=4326)
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
              value=formatTimeYear(rst.startDate))
   
   SubElement(SubElement(rodEl, "endDate"), 
              "calendarDate", 
              value=formatTimeYear(rst.endDate))

   addContactElement(dsEl)

   customUnits = addServiceRaster(dsEl, rst)
   
   if len(customUnits) > 0:
      amEl = SubElement(el, "additionalMetadata")
      mdEl = SubElement(amEl, "metadata")
      ulEl = SubElement(mdEl, "unitList")
      processCustomUnits(ulEl, customUnits)
      
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# .............................................................................
def addServiceRaster(el, rst):
   """
   @summary: Adds a raster object to the EML tree as a service spatial raster
   """
   customUnits = set()
   srEl = SubElement(el, "spatialRaster")
   SubElement(srEl, "entityName", value=rst.metadataUrl)

   physEl = SubElement(srEl, "physical")
   SubElement(physEl, "objectName", value=rst.getBaseFilename())
   dfEl = SubElement(physEl, "dataFormat")
   extDefFrmt = SubElement(dfEl, "externallyDefinedFormat")
   SubElement(extDefFrmt, "formatName", value=rst.getFormatLongName())
   distribEl = SubElement(physEl, "distribution")
   onlineEl = SubElement(distribEl, "online")
   SubElement(onlineEl, "url", attrib={"function" : "download"}, value="%s/raw" % rst.metadataUrl)
   
   attListEl = SubElement(srEl, "attributeList")
   attEl = SubElement(attListEl, "attribute")
   SubElement(attEl, "attributeName", value="Value")
   SubElement(attEl, "attributeDefinition", value=rst.title)
   SubElement(attEl, "storageType", value="Real")

   measScaleEl = SubElement(attEl, "measurementScale")
   intervalEl = SubElement(measScaleEl, "interval")
   unitEl = SubElement(intervalEl, "unit")
   
   if rst.valUnits in EML_STANDARD_UNITS:
      SubElement(unitEl, "standardUnit", value=rst.valUnits)
   elif EML_CUSTOM_UNITS.has_key(rst.valUnits):
      if EML_CUSTOM_UNITS[rst.valUnits]["standard"]:
         SubElement(unitEl, "standardUnit", value=EML_CUSTOM_UNITS[rst.valUnits]["standardUnit"])
      else:
         SubElement(unitEl, "customUnit", value=rst.valUnits)
         customUnits.add(EML_CUSTOM_UNITS[rst.valUnits]["unitString"])
   elif rst.valUnits is None or len(rst.valUnits.strip()) == 0:
      SubElement(unitEl, "customUnit", value="Unknown")
      customUnits.add("<stmml:unit id=\"Unknown\"><stmml:description>This unit is unknown</stmml:description></stmml:unit>")
   else:
      SubElement(unitEl, "customUnit", value=rst.valUnits)
      customUnits.add("<stmml:unit xmlns:stmml='http://www.xml-cml.org/schema/stmml' xmlns:stmml='http://www.xml-cml.org/schema/stmml' id=\"%s\"><stmml:description>%s</stmml:description></stmml:unit>" % (rst.valUnits, rst.valUnits))

   numDomEl = SubElement(intervalEl, "numericDomain")
   SubElement(numDomEl, "numberType", value="real")
   numBoundsEl = SubElement(numDomEl, "bounds")
   SubElement(numBoundsEl, "minimum", attrib={"exclusive" : "false"}, value=rst.minVal)
   SubElement(numBoundsEl, "maximum", attrib={"exclusive" : "false"}, value=rst.maxVal)
   
   spRefEl = SubElement(srEl, "spatialReference")
   cs = rst.processWkt(rst.getSRSAsWkt())
   hcsdEl = SubElement(spRefEl, "horizCoordSysDef", attrib={"name" : cs.name})
   
   if rst.epsgcode == 4326:
      geoCSEl = SubElement(hcsdEl, "geoCoordSys")
      SubElement(geoCSEl, "datum", attrib={"name" : cs.datum})
      SubElement(geoCSEl, "spheroid", 
                 attrib={"name" : cs.spheroid.name, 
                         "semiAxisMajor" : cs.spheroid.semiAxisMajor, 
                         "denomFlatRatio" : cs.spheroid.denomFlatRatio})
   
      SubElement(geoCSEl, "primeMeridian", 
                 attrib={"name" : cs.primeMeridian.name, 
                         "longitude" : cs.primeMeridian.longitude})
      SubElement(geoCSEl, "unit", attrib={"name" : cs.unit})
   else:
      prjCSEl = SubElement(hcsdEl, "projCoordSys")
      geoCSEl = SubElement(prjCSEl, "geoCoordSys")
      SubElement(geoCSEl, "datum", attrib={"name" : cs.geogcs.name})
      SubElement(geoCSEl, "spheroid", 
                 attrib={"name" : cs.geogcs, 
                         "semiAxisMajor" : cs.geogcs.spheroid.semiAxisMajor, 
                         "denomFlatRatio" : cs.geogcs.spheroid.denomFlatRatio})
      SubElement(geoCSEl, "primeMeridian",
                 attrib={"name" : cs.geogcs.primeMeridian.name,
                         "longitude" : cs.geogcs.primeMeridian.longitude})
      SubElement(geoCSEl, "unit", attrib={"name" : cs.geogcs.unit})
      
      prjEl = SubElement(prjCSEl, "projection", attrib={"name" : cs.projectionName})
      for param in cs.parameters:
         SubElement(prjEl, "parameter", attrib={"name" : param.name, "value" : param.value})
      SubElement(prjEl, "unit", attrib={"name" : cs.unit})

   haEl = SubElement(srEl, "horizontalAccuracy")
   SubElement(haEl, "accuracyReport", value="Unknown")
   
   vaEl = SubElement(srEl, "verticalAccuracy")
   SubElement(vaEl, "accuracyReport", value="Unknown")

   SubElement(srEl, "cellSizeXDirection", value=rst.resolution)
   SubElement(srEl, "cellSizeYDirection", value=rst.resolution)
   SubElement(srEl, "numberOfBands", value="1")
   SubElement(srEl, "rasterOrigin", value="Upper Left")

   size = rst.getSize()
   SubElement(srEl, "rows", value=str(size[1]))
   SubElement(srEl, "columns", value=str(size[0]))
   SubElement(srEl, "verticals", value="1")
   SubElement(srEl, "cellGeometry", value="pixel")
   
   return customUnits

