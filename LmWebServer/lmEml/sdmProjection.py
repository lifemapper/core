"""
@summary: Module containing functions to build projection EML
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
from LmCommon.common.lmXml import Element, PI, QName, \
                                  register_namespace, setDefaultNamespace, \
                                  SubElement, tostring

from LmServer.base.utilities import formatTimeYear, getPackageId

from LmWebServer.common.lmconstants import STMML_NAMESPACE, XSI_NAMESPACE

from LmWebServer.lmEml.contact import addContactElement
from LmWebServer.lmEml.emlConstants import EML_NAMESPACE, EML_SCHEMA_LOCATION,\
                                           EML_SYSTEM

# .............................................................................
def buildSdmProjectionPackage(prj):
   """
   @summary: Builds a sdm projection EML package
   @param prj: The projection to generate the package for
   """
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
   
   register_namespace('', EML_NAMESPACE)
   setDefaultNamespace(EML_NAMESPACE)   
   el = Element("eml", attrib={
                          "packageId" : getPackageId(prj, separator='.'),
                          "system" : EML_SYSTEM,
                          QName(XSI_NAMESPACE, "schemaLocation") : \
                                "%s\n%s" % (EML_NAMESPACE, EML_SCHEMA_LOCATION)
                              })

   
   dsEl = SubElement(el, "dataset", 
                         attrib={"id" : getPackageId(prj, separator='.')})
   SubElement(dsEl, "title", value=prj.title)
   SubElement(SubElement(dsEl, "creator"), 
              "organizationName", 
              value="Lifemapper")
   
   if len(prj.keywords) > 0:
      kwsEl = SubElement(dsEl, "keywordSet")
      for kw in prj.keywords:
         SubElement(kwsEl, "keyword", value=kw)
   
   # Coverages
   coverageEl = SubElement(dsEl, "coverage")

   # Geographic Coverage
   geoCovEl = SubElement(coverageEl, "geographicCoverage")
   SubElement(geoCovEl, "geographicDescription", 
                                value="Bounding Box (EPSG:%s) - %s" % (
                                                  prj.epsgcode, str(prj.bbox)))
   boundCoordsEl = SubElement(geoCovEl, "boundingCoordinates")
   if prj.epsgcode == 4326:
      bbox = prj.bbox
   else:
      bboxPts = prj.translatePoints([(prj.bbox[0], prj.bbox[1]), 
                                     (prj.bbox[2], prj.bbox[3])], 
                                    srcEPSG=prj.epsgcode, dstEPSG=4326)
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
              value=formatTimeYear(prj.startDate))
   
   SubElement(SubElement(rodEl, "endDate"), 
              "calendarDate", 
              value=formatTimeYear(prj.endDate))

   addContactElement(dsEl)
   addSdmProjection(dsEl, prj)
   
   scn = prj._scenario
   
   am1 = SubElement(dsEl, "additionalMetadata", attrib={"id" : getPackageId(scn, separator='.')})
   m1 = SubElement(am1, "metadata")
   scnEl = SubElement(m1, "scenario")
   SubElement(scnEl, "title", value=scn.title)
   SubElement(scnEl, "ref", attrib={"url" : "%s/eml" % scn.metadataUrl})
   
   am2 = SubElement(dsEl, "additionalMetadata")
   m2 = SubElement(am2, "metadata")
   ulEl = SubElement(m2, "unitList")
   
   unitEl = SubElement(ulEl, "unit", namespace=STMML_NAMESPACE, 
                       attrib={"id" : "Probability of Presence"})
   SubElement(unitEl, "description", namespace=STMML_NAMESPACE,
              value="The predicted probability that this cell contains suitable habitat for the organism (scaled from 0 to 255)")
   
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# .............................................................................
def addSdmProjection(el, prj):
   """
   @summary: Adds an SDM projection to the specified element
   @param el: The elment to add projection data to
   @param prj: The projection data to use
   """
   spRstEl = SubElement(el, "spatialRaster")
   SubElement(spRstEl, "entityName", value=prj.metadataUrl)
   
   physEl = SubElement(spRstEl, "physical")
   SubElement(physEl, "objectName", value=prj.getBaseFilename())
   dfEl = SubElement(physEl, "dataFormat")
   extDefFmtEl = SubElement(dfEl, "externallyDefinedFormat")
   SubElement(extDefFmtEl, "formatName", value=prj.getFormatLongName())
   
   distribEl = SubElement(physEl, "distribution")
   onlineEl = SubElement(distribEl, "online")
   SubElement(onlineEl, "url", attrib={"function" : "download"}, 
              value="%s/raw" % prj.metadataUrl)

   attListEl = SubElement(spRstEl, "attributeList")
   valAttEl = SubElement(attListEl, "attribute", attrib={"id" : "Value"})
   SubElement(valAttEl, "attributeName", value="Value")
   SubElement(valAttEl, "attributeDefinition", value=prj.title)
   SubElement(valAttEl, "storageType", value="Real")
   
   measScaleEl = SubElement(valAttEl, "measurementScale")
   intervalEl = SubElement(measScaleEl, "interval")
   
   unitEl = SubElement(intervalEl, "unit")
   SubElement(unitEl, "customUnit", value="Probability of Presence")
   
   numericDomainEl = SubElement(intervalEl, "numericDomain")
   SubElement(numericDomainEl, "numberType", value="real")
   boundsEl = SubElement(numericDomainEl, "bounds")
   SubElement(boundsEl, "minimum", attrib={"exclusive" : "false"},
              value=str(prj.minVal))
   SubElement(boundsEl, "maximum", attrib={"exclusive" : "false"},
              value=str(prj.maxVal))

   spatialReferenceEl = SubElement(spRstEl, "spatialReference")
   
   cs = prj.processWkt(prj.getSRSAsWkt())
   hzCSEl = SubElement(spatialReferenceEl, "horizCoordSysDef", attrib={"name" : cs.name})
   
   if prj.epsgcode == 4326:
      geoCSEl = SubElement(hzCSEl, "geogCoordSys")
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
      projCSEl = SubElement(hzCSEl, "projCoordSys")

      geoCSEl = SubElement(projCSEl, "geogCoordSys")
      SubElement(geoCSEl, "datum", attrib={"name" : cs.geogcs.datum})
      SubElement(geoCSEl, "spheroid", 
                 attrib={"name" : cs.geogcs.spheroid.name,
                         "semiAxisMajor" : cs.geogcs.spheroid.semiAxisMajor,
                         "denomFlatRatio" : cs.geogcs.spheroid.denomFlatRatio})
      SubElement(geoCSEl, "primeMeridian", 
                 attrib={"name" : cs.geogcs.primeMeridian.name,
                         "longitude" : cs.geogcs.primeMeridian.longitude})
      SubElement(geoCSEl, "unit", attrib={"name" : cs.geogcs.unit})
      
      # Projection information
      prjEl = SubElement(projCSEl, "projection", attrib={"name" : cs.projectionName})
      for param in cs.parameters:
         SubElement(prjEl, "parameter", attrib={"name" : param.name, 
                                                "value" : str(param.value)})
      SubElement(prjEl, "unit", attrib={"name" : cs.unit})

   hzAccEl = SubElement(spRstEl, "horizontalAccuracy")
   SubElement(hzAccEl, "accuracyReport", value="Unknown")

   vertAccEl = SubElement(spRstEl, "verticalAccuracy")
   SubElement(vertAccEl, "accuracyReport", value="Unknown")
      
   SubElement(spRstEl, "cellSizeXDirection", value=prj.resolution)
   SubElement(spRstEl, "cellSizeYDirection", value=prj.resolution)
   SubElement(spRstEl, "numberOfBands", value="1")
   SubElement(spRstEl, "rasterOrigin", value="Upper Left")
   size = prj.getSize()
   SubElement(spRstEl, "rows", value=str(size[1]))
   SubElement(spRstEl, "columns", value=str(size[0]))
   SubElement(spRstEl, "verticals", value="1")
   SubElement(spRstEl, "cellGeometry", value="pixel")
