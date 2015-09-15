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
from LmCommon.common.lmXml import Element, fromstring, PI, QName, \
                                  register_namespace, setDefaultNamespace, \
                                  SubElement, tostring

from LmServer.base.utilities import formatTimeYear, getPackageId

from LmWebServer.common.lmconstants import XSI_NAMESPACE
from LmWebServer.lmEml.contact import addContactElement
from LmWebServer.lmEml.emlConstants import EML_KNOWN_FEATURES, EML_NAMESPACE, \
                                           EML_SCHEMA_LOCATION, EML_SYSTEM

# .............................................................................
def buildSdmOccurrenceSetPackage(occ):
   """
   @summary: Builds an occurrence set EML package
   @param occ: The occurrence set to generate the package for
   """
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
   
   register_namespace('', EML_NAMESPACE)
   setDefaultNamespace(EML_NAMESPACE)   
   el = Element("eml", attrib={
                          "packageId" : getPackageId(occ, separator='.'),
                          "system" : EML_SYSTEM,
                          QName(XSI_NAMESPACE, "schemaLocation") : \
                                "%s\n%s" % (EML_NAMESPACE, EML_SCHEMA_LOCATION)
                              })
   dsEl = SubElement(el, "dataset", 
                         attrib={"id" : getPackageId(occ, separator='.')})
   SubElement(dsEl, "title", value=occ.title)
   SubElement(SubElement(dsEl, "creator"), 
              "organizationName", 
              value="Lifemapper")
   
   if len(occ.keywords) > 0:
      kwsEl = SubElement(dsEl, "keywordSet")
      for kw in occ.keywords:
         SubElement(kwsEl, "keyword", value=kw)
   
   # Coverages
   coverageEl = SubElement(dsEl, "coverage")

   # Geographic Coverage
   geoCovEl = SubElement(coverageEl, "geographicCoverage")
   SubElement(geoCovEl, "geographicDescription", 
                                value="Bounding Box (EPSG:%s) - %s" % (
                                                  occ.epsgcode, str(occ.bbox)))
   boundCoordsEl = SubElement(geoCovEl, "boundingCoordinates")
   if occ.epsgcode == 4326:
      bbox = occ.bbox
   else:
      bboxPts = occ.translatePoints([(occ.bbox[0], occ.bbox[1]), 
                                     (occ.bbox[2], occ.bbox[3])], 
                                    srcEPSG=occ.epsgcode, dstEPSG=4326)
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
              value=formatTimeYear(occ.startDate))
   
   SubElement(SubElement(rodEl, "endDate"), 
              "calendarDate", 
              value=formatTimeYear(occ.endDate))

   # Taxonomic Coverage
   if occ.fromGbif:
      names = occ.displayName.split(' ')
      if len(names) > 0:
         taxCovEl = SubElement(coverageEl, "taxonomicCoverage")
         genusEl = SubElement(taxCovEl, "taxonomicClassification")
         SubElement(genusEl, "taxonRankName", value="Genus")
         SubElement(genusEl, "taxonRankValue", value=names[0])
         
         if len(names) > 1:
            speciesEl = SubElement(genusEl, "taxonomicCoverage")
            SubElement(speciesEl, "taxonRankName", value="Species")
            SubElement(speciesEl, "taxonRankValue", value=names[1])
            
            if len(names) == 3:
               subspEl = SubElement(speciesEl, "taxonomicClassification")
               SubElement(subspEl, "taxonRankName", value="Subspecies")
               SubElement(subspEl, "taxonRankValue", value=names[2])
            
            if len(names) > 3:
               tc = SubElement(speciesEl, "taxonomicClassification")
               if names[2] in ('var.', 'var'):
                  SubElement(tc, "taxonRankName", value="Variety")
               elif names[2] in ('subsp.', 'ssp.'):
                  SubElement(tc, "taxonRankName", value="Subspecies")
               elif names[2] == 'f.':
                  SubElement(tc, "taxonRankName", value="Form")
               SubElement(tc, "taxonRankValue", value=names[3])

   addContactElement(dsEl)

   addSdmOccurrenceSet(dsEl, occ)
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# .............................................................................
def addSdmOccurrenceSet(el, occ):
   """
   @summary: Adds an occurrence set spatialVector to an element
   @param el: The ElementTree element to add to
   @param occ: The occurrence set to use
   """
   sv = SubElement(el, "spatialVector")
   
   SubElement(sv, "entityName", value=occ.title)
   
   if occ.description is not None:
      SubElement(sv, "entityDescription", value=occ.description)

   physEl = SubElement(el, "physical")
   SubElement(physEl, "objectName", value=occ.getBaseFilename())
   SubElement(
              SubElement(
                         SubElement(physEl, "dataFormat"),
                         "externallyDefinedFormat"),
              "formatName",
              value=occ.getFormatLongName())

   SubElement(
              SubElement(
                         SubElement(physEl, "distribution"),
                         "online"
                         ),
              "url",
              attrib={"function" : "download"},
              value="%s/shapefile" % occ.metadataUrl)

   attListEl = SubElement(sv, "attributeList")
   
   for faKey in occ._featureAttributes.keys():
      fa = occ._featureAttributes[faKey][0]
      if EML_KNOWN_FEATURES.has_key(fa):
         attEl = SubElement(attListEl, "attribute")
         SubElement(attEl, "attributeName", value=fa)
         SubElement(attEl, "attributeLabel", value=EML_KNOWN_FEATURES[fa]["label"])
         SubElement(attEl, "attributeDefinition", value=EML_KNOWN_FEATURES[fa]["definition"])
         SubElement(attEl, "storageType", value=EML_KNOWN_FEATURES[fa]["storageType"])
         attEl.append(fromstring(EML_KNOWN_FEATURES[fa]["measurementScale"]))
         
   SubElement(sv, "geometry", value="Point")
   SubElement(sv, "geometricObjectCount", value=occ.featureCount)
   
   srEl = SubElement(sv, "spatialReference")
   
   cs = occ.processWkt(occ.getSRSAsWkt())
   hzCSEl = SubElement(srEl, "horizCoordSysDef", attrib={"name" : cs.name})
   
   if occ.epsgcode == 4326:
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
