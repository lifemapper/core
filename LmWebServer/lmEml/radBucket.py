"""
@summary: 
@author: CJ Grady
@version: 
@status: alpha

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
from LmCommon.common.lmXml import (Element, PI, QName, register_namespace, 
               setDefaultNamespace, SubElement, tostring)

from LmServer.base.utilities import formatTimeYear, getPackageId

from LmWebServer.common.lmconstants import XSI_NAMESPACE
from LmWebServer.lmEml.contact import addContactElement
from LmWebServer.lmEml.emlConstants import (EML_NAMESPACE, EML_SCHEMA_LOCATION, 
                                            EML_SYSTEM)
from LmWebServer.lmEml.serviceVector import addServiceVector

# .............................................................................
def buildRadBucketPackage(bkt):
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
   
   register_namespace('', EML_NAMESPACE)
   setDefaultNamespace(EML_NAMESPACE)   
   el = Element("eml", attrib={
                          "packageId" : getPackageId(bkt, separator='.'),
                          "system" : EML_SYSTEM,
                          QName(XSI_NAMESPACE, "schemaLocation") : \
                                "%s\n%s" % (EML_NAMESPACE, EML_SCHEMA_LOCATION)
                              })
   dsEl = SubElement(el, "dataset", 
                         attrib={"id" : getPackageId(bkt, separator='.')})
   SubElement(dsEl, "title", value=bkt.title)
   SubElement(SubElement(dsEl, "creator"), 
              "organizationName", 
              value="Lifemapper")
   
   if len(bkt.keywords) > 0:
      kwsEl = SubElement(dsEl, "keywordSet")
      for kw in bkt.keywords:
         SubElement(kwsEl, "keyword", value=kw)
   
   # Coverages
   coverageEl = SubElement(dsEl, "coverage")

   # Geographic Coverage
   geoCovEl = SubElement(coverageEl, "geographicCoverage")
   SubElement(geoCovEl, "geographicDescription", 
                                value="Bounding Box (EPSG:%s) - %s" % (
                                                  bkt.epsgcode, str(bkt.bbox)))
   boundCoordsEl = SubElement(geoCovEl, "boundingCoordinates")
   if bkt.epsgcode == 4326:
      bbox = bkt.bbox
   else:
      bboxPts = bkt.translatePoints([(bkt.bbox[0], bkt.bbox[1]), 
                                     (bkt.bbox[2], bkt.bbox[3])], 
                                    srcEPSG=bkt.epsgcode, dstEPSG=4326)
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
              value=formatTimeYear(bkt.startDate))
   
   SubElement(SubElement(rodEl, "endDate"), 
              "calendarDate", 
              value=formatTimeYear(bkt.endDate))

   addContactElement(dsEl)

   addRadBucket(dsEl, bkt)
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# .............................................................................
def addRadBucket(el, bkt):
   addServiceVector(el, bkt.shapegrid)
   
   origPsEntEl = SubElement(el, "otherEntity")
   SubElement(origPsEntEl, "entityName", 
          value="Original PamSum (%s) for bucket %s" % (bkt.pamSum.id, bkt.id))
   SubElement(origPsEntEl, "entityType", value="PamSum")

   for ps in bkt.getRandomPamSums():
      randPsEntEl = SubElement(el, "otherEntity")
      SubElement(randPsEntEl, "entityName", 
                    value="Random PamSum (%s) for bucket %s" % (ps.id, bkt.id))
      SubElement(randPsEntEl, "entityType", value="PamSum")
