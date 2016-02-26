"""
@summary: Module containing functions to convert an SDM experiment to EML
@author: CJ Grady
@version: 2.0
@status: alpha
@note: For EML 2.1.1
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
from LmCommon.common.lmXml import (CDATA, Element, fromstring, PI, QName, 
               register_namespace, setDefaultNamespace, SubElement, tostring)
from LmCommon.common.lmconstants import (JobStatus, LM_PROC_NAMESPACE, 
               LM_PROC_SCHEMA_LOCATION)

from LmServer.base.utilities import formatTimeHuman, getPackageId
from LmServer.common.localconstants import WEBSERVICES_ROOT

from LmWebServer.common.lmconstants import XSI_NAMESPACE
from LmWebServer.lmEml.contact import addContactElement
from LmWebServer.lmEml.customUnits import processCustomUnits
from LmWebServer.lmEml.emlConstants import (EML_KNOWN_FEATURES, EML_NAMESPACE, 
               EML_SCHEMA_LOCATION, EML_SYSTEM)

# .............................................................................
def buildSdmExperimentPackage(exp):
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
   
   register_namespace('', EML_NAMESPACE)
   register_namespace('lmProc', LM_PROC_NAMESPACE)
   setDefaultNamespace(EML_NAMESPACE)
   
   el = Element("eml", attrib={
                   "packageId" : getPackageId(exp, separator='.'),
                   "system" : EML_SYSTEM,
                   QName(XSI_NAMESPACE, "schemaLocation") : "%s\n%s\n%s\n%s" % (
                                                       EML_NAMESPACE, 
                                                       EML_SCHEMA_LOCATION, 
                                                       LM_PROC_NAMESPACE, 
                                                       LM_PROC_SCHEMA_LOCATION)
                   })
   
   customUnits = set()
   occ = exp.model.occurrenceSet

   dsEl = SubElement(el, "dataset", 
                     attrib={"id" : getPackageId(exp, separator='.')})
   SubElement(dsEl, "title", value="Lifemapper SDM Experiment %s" % exp.id)
   SubElement(SubElement(dsEl, "creator"),
              "organizationName",
              value="Lifemapper")
   
   # Coverages
   coverageEl = SubElement(dsEl, "coverage")
   
   # Geographic Coverage
   geoCovEl = SubElement(coverageEl, "geographicCoverage")
   SubElement(geoCovEl, "geographicDescription", 
              value="Bounding Box (EPSG:%s) - %s" % (exp.epsgcode, 
                                                     str(exp.bbox)))
   boundCoordsEl = SubElement(geoCovEl, "boundingCoordinates")
   if exp.epsgcode == 4326:
      bbox = exp.bbox
   else:
      bboxPts = exp.translatePoints([(exp.bbox[0], exp.bbox[1]),
                                     (exp.bbox[2], exp.bbox[3])],
                                    srcEPSG=exp.epsgcode, dstEPSG=4326)
      bbox = (bboxPts[0][0], bboxPts[0][1], bboxPts[1][0], bboxPts[1][1])
   SubElement(boundCoordsEl, "westBoundingCoordinate", value=round(bbox[0], 3))
   SubElement(boundCoordsEl, "eastBoundingCoordinate", value=round(bbox[2], 3))
   SubElement(boundCoordsEl, "northBoundingCoordinate", value=round(bbox[3], 3))
   SubElement(boundCoordsEl, "southBoundingCoordinate", value=round(bbox[1], 3))
   
   addContactElement(dsEl)

   customUnits = addSdmExperiment(dsEl, exp)

   if len(customUnits) > 0:
      amEl = SubElement(el, "additionalMetadata")
      mdEl = SubElement(amEl, "metadata")
      ulEl = SubElement(mdEl, "unitList")
      processCustomUnits(ulEl, customUnits)
   
   _addOccurrenceSetPostAdditionalMetadata(el, occ)
   _addExperimentSubmissionAdditionalMetadata(el, exp)
   
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# .............................................................................
def addSdmExperiment(el, exp):
   _addModel(el, exp)
   _addOccurrenceSetSpatialVector(el, exp.model.occurrenceSet)
   customUnits = set()
   for prj in exp.projections:
      customUnits.add(_addProjection(el, prj))
   for scn in [prj._scenario for prj in exp.projections]:
      _addScenario(el, scn)
   return customUnits
   
# .............................................................................
def _addModel(el, exp):
   oeEl = SubElement(el, "otherEntity", 
                     attrib={"id" : getPackageId(exp.model, separator='.')})
   SubElement(oeEl, "entityName", value="Experiment %s Model Ruleset Package" % exp.id)
   SubElement(oeEl, "entityDescription", 
              value="This is a package of outputs generated by the underlying modeling software")
   physEl = SubElement(oeEl, "physical")
   SubElement(physEl, "objectName", value="%s.zip" % exp.model.id)
   SubElement(physEl, "compressionMethod", value="gzip")
   dfEl = SubElement(physEl, "dataFormat")
   extDefFrmtEl = SubElement(dfEl, "externallyDefinedFormat")
   SubElement(extDefFrmtEl, "formatName", value="GNU zip")
   distribEl = SubElement(physEl, "distribution")
   onlineEl = SubElement(distribEl, "online")
   SubElement(onlineEl, "url", attrib={"function" : "download"}, 
              value="%s/package" % exp.metadataUrl)

   _addModelMethods(oeEl, exp)
   SubElement(oeEl, "entityType", value="Model Methods")

# .............................................................................
def _addModelMethods(el, exp):
   occ = exp.model.occurrenceSet
   methodsEl = SubElement(el, "methods")
   
   # Occurrence method step
   occMethodStep = SubElement(methodsEl, "methodStep")
   occDesc = SubElement(occMethodStep, "description")
   occSec = SubElement(occDesc, "section")
   if occ.fromGbif:
      paraVal = "Occurrence set %s retrieved from the Lifemapper GBIF cache at %s" % (occ.id, formatTimeHuman(occ.modTime))
   else:
      paraVal = "Occurrence set %s uploaded at %s" % (occ.id, formatTimeHuman(occ.modTime)) 
   SubElement(occSec, "para", value=paraVal)
   
   dsEl1 = SubElement(occMethodStep, "dataSource")
   SubElement(dsEl1, "references", value=getPackageId(occ, separator='.'))
   dsEl2 = SubElement(occMethodStep, "dataSource")
   SubElement(dsEl2, "references", value="postOccurrenceSet")
   
   # Experiment
   expMethodStep = SubElement(methodsEl, "methodStep")
   expDesc = SubElement(expMethodStep, "description")
   expSec = SubElement(expDesc, "section")
   SubElement(expSec, "para", 
              value="An experiment package was submitted through the Lifemapper SDM Experiments web service found at %s/sdm/experiments" % WEBSERVICES_ROOT)
   expDsEl = SubElement(expMethodStep, "dataSource")
   SubElement(expDsEl, "references", value="experimentSubmission")
   
   prjAlgoSec, softwareRef = _addAlgorithmMethodStep(methodsEl, exp)
   _addProjectionMethodSteps(methodsEl, exp, prjAlgoSec, softwareRef)
   
# .............................................................................
def _addAlgorithmMethodStep(el, exp):
   msEl = SubElement(el, "methodStep")
   mdlDesc = SubElement(msEl, "description")
   mdlSec = SubElement(mdlDesc, "section")
   
   if exp.model.algorithmCode == 'ATT_MAXENT':
      SubElement(mdlSec, "para", 
                 value="""\
                       Model %s was submitted to a Lifemapper compute 
                       environment and ran using ATT MaxEnt with the ATT_MAXENT 
                       algorithm and Lifemapper scenario %s""" % (
                                        exp.model.id, exp.model._scenario.id))
      softwareEl = SubElement(msEl, "software", attrib={"id" : "maxent"})
      SubElement(softwareEl, "shortName", value="MaxEnt")
      SubElement(softwareEl, "title", 
                        value="Maximum Entropy Species Distribution Modeling")
      creatorEl = SubElement(softwareEl, "creator")
      
      miroDudikEl = SubElement(creatorEl, "individualName")
      SubElement(miroDudikEl, "givenName", value="Miro")
      SubElement(miroDudikEl, "surName", value="Dudik")
      
      stevenPhillipsEl = SubElement(creatorEl, "individualName")
      SubElement(stevenPhillipsEl, "givenName", value="Steven")
      SubElement(stevenPhillipsEl, "surName", value="Phillips")

      robSchapireEl = SubElement(creatorEl, "individualName")
      SubElement(robSchapireEl, "givenName", value="Rob")
      SubElement(robSchapireEl, "surName", value="Schapire")
      
      SubElement(creatorEl, "onlineUrl", 
                          value="http://www.cs.princeton.edu/~schapire/maxent")

      implementationEl = SubElement(softwareEl, "implementation")
      softDistribEl = SubElement(implementationEl, "distribution")
      softOnlineEl = SubElement(softDistribEl, "online")
      SubElement(softOnlineEl, "url", 
                          value="http://www.cs.princeton.edu/~schapire/maxent")

      SubElement(softwareEl, "version", value="3.3.3e")
      
      prjAlgoSec = "ATT MaxEnt"
      softwareRef = "maxent"
   else:
      SubElement(mdlSec, "para", value="""\
                        Model %sbj.model.getId() was submitted to a Lifemapper
                        compute environment and ran using openModeller 
                        (om_model) using the %s algorithm
                        and Lifemapper scenario %s""" % (
                exp.model.id, exp.model.algorithmCode, exp.model._scenario.id))
      softwareEl = SubElement(msEl, "software", attrib={"id" : "openModeller"})
      SubElement(softwareEl, "shortName", value="openModeller")
      SubElement(softwareEl, "title", 
                        value="openModeller - Open Source Spatial Distribution Modeller")
      creatorEl = SubElement(softwareEl, "creator")
      SubElement(creatorEl, "organizationName", 
                 value="CRIA - Centro de Referencia em Informacao Ambiental (SP - Brazil)")
      SubElement(creatorEl, "onlineUrl", 
                 value="http://openmodeller.sourceforge.net")
      implementationEl = SubElement(softwareEl, "implementation")
      softDistribEl = SubElement(implementationEl, "distribution")
      softOnlineEl = SubElement(softDistribEl, "online")
      SubElement(softOnlineEl, "url", 
                          value="http://openmodeller.sourceforge.net")
      SubElement(softwareEl, "version", value="1.3")
      
      prjAlgoSec = "openModeller (om_project)"
      softwareRef = "openModeller"
   
   algoDs = SubElement(msEl, "dataSource")
   SubElement(algoDs, "references", 
                        value=getPackageId(exp.model._scenario, separator='.'))

   return prjAlgoSec, softwareRef

# .............................................................................
def _addProjectionMethodSteps(el, exp, prjAlgoSec, softwareRef):
   for prj in exp.projections:
      msEl = SubElement(el, "methodStep")
      descEl = SubElement(msEl, "description")
      secEl = SubElement(descEl, "section")
      SubElement(secEl, "para", 
                 value="""Projection %s was submitted to a Lifemapper compute 
                          environment and ran through %s applying model %s to 
                          scenario %s""" % (
                           prj.id, prjAlgoSec, exp.model.id, prj._scenario.id))
      softwareEl = SubElement(msEl, "software")
      SubElement(softwareEl, "references", value=softwareRef)

      scnDs = SubElement(msEl, "dataSource")
      SubElement(scnDs, "references", 
                 value=getPackageId(prj._scenario, separator='.'))
      if prj.status == JobStatus.COMPLETE:
         prjDs = SubElement(msEl, "dataSource")
         SubElement(prjDs, "references", value=getPackageId(prj, separator='.'))

# .............................................................................
def _addOccurrenceSetSpatialVector(el, occ):
   svEl = SubElement(el, "spatialVector", 
                     attrib={"id" : getPackageId(occ, separator='.')})
   SubElement(svEl, "entityName", value=occ.title)
   if occ.description is not None:
      SubElement(svEl, "entityDescription", value=occ.description)

   physEl = SubElement(svEl, "physical")
   SubElement(physEl, "objectName", value=occ.getBaseFilename())
   dfEl = SubElement(physEl, "dataFormat")
   extDefFrmt = SubElement(dfEl, "externallyDefinedFormat")
   SubElement(extDefFrmt, "formatName", value=occ.getFormatLongName())
   distribEl = SubElement(physEl, "distribution")
   onlineEl = SubElement(distribEl, "online")
   SubElement(onlineEl, "url", attrib={"function" : "download"}, 
              value="%s/shapefile" % occ.metadataUrl)

   attList = SubElement(svEl, "attributeList")
   
   for faKey in occ._featureAttributes.keys():
      fa = occ._featureAttributes[faKey][0]
      if EML_KNOWN_FEATURES.has_key(fa):
         attEl = SubElement(attList, "attribute")
         SubElement(attEl, "attributeName", value=fa)
         SubElement(attEl, "attributeLabel", value=EML_KNOWN_FEATURES[fa]["label"])
         SubElement(attEl, "attributeDefinition", value=EML_KNOWN_FEATURES[fa]["definition"])
         SubElement(attEl, "storageType", value=EML_KNOWN_FEATURES[fa]["storageType"])
         attEl.append(fromstring(EML_KNOWN_FEATURES[fa]["measurementScale"]))

   SubElement(svEl, "geometry", value="Point")
   SubElement(svEl, "geometricObjectCount", value=occ.featureCount)
   
   srEl = SubElement(svEl, "spatialReference")
   cs = occ.processWkt(occ.getSRSAsWkt())
   hcSysDefEl = SubElement(srEl, "horizCoordSysDef", attrib={"name" : cs.name})
   if occ.epsgcode == 4326:
      gcSysEl = SubElement(hcSysDefEl, "geogCoordSys")
      SubElement(gcSysEl, "datum", attrib={"name" : cs.datum})
      SubElement(gcSysEl, "spheroid", 
                 attrib={"name" : cs.spheroid.name, 
                         "semiAxisMajor" : cs.spheroid.semiAxisMajor, 
                         "denomFlatRatio" : cs.spheroid.denomFlatRatio})
      SubElement(gcSysEl, "primeMeridian", 
                 attrib={"name" : cs.primeMeridian.name, 
                         "longitude" : cs.primeMeridian.longitude})
      SubElement(gcSysEl, "unit", attrib={"name" : cs.unit})
   else:
      prjCsEl = SubElement(hcSysDefEl, "projCoordSys")
      gcSysEl = SubElement(prjCsEl, "geogCoordSys")
      SubElement(gcSysEl, "datum", attrib={"name" : cs.geogcs.datum})
      SubElement(gcSysEl, "spheroid", 
                 attrib={"name" : cs.geogcs.spheroid.name,
                         "semiAxisMajor" : cs.geogcs.spheroid.semiAxisMajor,
                         "denomFlatRatio" : cs.geogcs.spheroid.denomFlatRatio})
      SubElement(gcSysEl, "primeMeridian", 
                 attrib={"name" : cs.geogcs.primeMeridian.name, 
                         "longitude" : cs.geogcs.primeMeridian.longitude})
      SubElement(gcSysEl, "unit", attrib={"name" : cs.geogcs.unit})
      prjEl = SubElement(prjCsEl, "projection", 
                         attrib={"name" : cs.projectionName})
      for param in cs.parameters:
         SubElement(prjEl, "parameter", 
                    attrib={"name" : param.name, "value" : param.value})
      SubElement(prjEl, "unit", attrib={"name" : cs.unit})

# .............................................................................
def _addProjection(el, prj):
   customUnits = set()
   if prj.status == JobStatus.COMPLETE:
      srEl = SubElement(el, "spatialRaster", 
                        attrib={"id" : getPackageId(prj, separator='.')})
      SubElement(srEl, "entityName", value=prj.metadataUrl)
      physEl = SubElement(srEl, "physical")
      SubElement(physEl, "objectName", value=prj.getBaseFilename())
      dfEl = SubElement(physEl, "dataFormat")
      extDefFrmt = SubElement(dfEl, "externallyDefinedFormat")
      SubElement(extDefFrmt, "formatName", value=prj.getFormatLongName())
      distribEl = SubElement(physEl, "distribution")
      onlineEl = SubElement(distribEl, "online")
      SubElement(onlineEl, "url", attrib={"function" : "download"}, 
                 value="%s/raw" % prj.metadataUrl)
      
      attListEl = SubElement(srEl, "attributeList")
      attEl = SubElement(attListEl, "attribute")
      SubElement(attEl, "attributeName", value="Value")
      SubElement(attEl, "attributeDefinition", value=prj.title)
      SubElement(attEl, "storageType", value="Real")
      measScaleEl = SubElement(attEl, "measurementScale")
      intervalEl = SubElement(measScaleEl, "interval")
      unitEl = SubElement(intervalEl, "unit")
      SubElement(unitEl, "customUnit", value="Probability of Presence")
      customUnits.update(["<stmml:unit xmlns:stmml='http://www.xml-cml.org/schema/stmml' id=\"Probability of Presence\"><stmml:description>The predicted probability that this cell contains suitable habitat for the organism (scaled from 0 to 255)</stmml:description></stmml:unit>"])
      
      numericDomainEl = SubElement(intervalEl, "numericDomain")
      SubElement(numericDomainEl, "numberType", value="real")
      boundsEl = SubElement(numericDomainEl, "bounds")
      SubElement(boundsEl, "minimum", attrib={"exclusive" : "false"}, 
                 value=prj.minVal)
      SubElement(boundsEl, "maximum", attrib={"exclusive" : "false"},
                 value=prj.maxVal)
      
      spRefEl = SubElement(srEl, "spatialReference")
      cs = prj.processWkt(prj.getSRSAsWkt())
      hcSysDefEl = SubElement(spRefEl, 
                              "horizCoordSysDef", attrib={"name" : cs.name})
      if prj.epsgcode == 4326:
         geogcs = cs
         gcSysEl = SubElement(hcSysDefEl, "geogCoordSys")
      else:
         geogcs = cs.geogcs
         prjCsEl = SubElement(hcSysDefEl, "projCoordSys")
         gcSysEl = SubElement(prjCsEl, "geogCoordSys")
         prjEl = SubElement(prjCsEl, "projection", 
                            attrib={"name" : cs.projectionName})
         for param in cs.parameters:
            SubElement(prjEl, "parameter", 
                        attrib={"name" : param.name, "value" : param.value})
         SubElement(prjEl, "unit", attrib={"name" : cs.unit})
      
      SubElement(gcSysEl, "datum", attrib={"name" : geogcs.name})
      SubElement(gcSysEl, "spheroid", 
                 attrib={"name" : geogcs.spheroid.name,
                         "semiAxisMajor" : geogcs.spheroid.semiAxisMajor,
                         "denomFlatRatio" : geogcs.spheroid.denomFlatRatio})
      SubElement(gcSysEl, "primeMeridian",
                 attrib={"name" : geogcs.primeMeridian.name,
                         "longitude" : geogcs.primeMeridian.longitude})
      SubElement(gcSysEl, "unit", attrib={"name" : geogcs.unit})
   
      SubElement(SubElement(srEl, "horizontalAccuracy"), 
                 "accuracyReport", 
                 value="Unknown")

      SubElement(SubElement(srEl, "verticalAccuracy"), 
                 "accuracyReport", 
                 value="Unknown")

      SubElement(srEl, "cellSizeXDirection", value=prj.resolution)
      SubElement(srEl, "cellSizeYDirection", value=prj.resolution)
      SubElement(srEl, "numberOfBands", value="1")
      SubElement(srEl, "rasterOrigin", value="Upper Left")
      size = prj.getSize()
      SubElement(srEl, "rows", value=size[1])
      SubElement(srEl, "columns", value=size[0])
      SubElement(srEl, "verticals", value="1")
      SubElement(srEl, "cellGeometry", value="pixel")

# .............................................................................
def _addScenario(el, scn):
   amEl = SubElement(el, "additionalMetadata", 
                     attrib={"id" : getPackageId(scn, separator='.')})
   metadataEl = SubElement(amEl, "metadata")
   scnEl = SubElement(metadataEl, "scenario")
   SubElement(scnEl, "title", value=scn.title)
   SubElement(scnEl, "ref", attrib={"url" : "%s/eml" % scn.metadataUrl})

# .............................................................................      
def _addOccurrenceSetPostAdditionalMetadata(el, occ):
   amEl = SubElement(el, "additionalMetadata", 
                     attrib={"id" : "postOccurrenceSet"})
   metadataEl = SubElement(amEl, "metadata")
   httpMsgEl = SubElement(metadataEl, "HTTPmessage", namespace=LM_PROC_NAMESPACE)
   reqEl = SubElement(httpMsgEl, "Request", namespace=LM_PROC_NAMESPACE)
   SubElement(reqEl, "Method", value="POST", namespace=LM_PROC_NAMESPACE)
   
   headersEl = SubElement(reqEl, "Headers", namespace=LM_PROC_NAMESPACE)
   ctHeader = SubElement(headersEl, "Header", namespace=LM_PROC_NAMESPACE)
   SubElement(ctHeader, "name", value="Content-Type", namespace=LM_PROC_NAMESPACE)
   SubElement(ctHeader, "value", value="application/x-gzip", 
              namespace=LM_PROC_NAMESPACE)
   msgBodyEl = SubElement(reqEl, "MessageBody", namespace=LM_PROC_NAMESPACE)
   SubElement(msgBodyEl, "OnlineResource", 
              value="%s/shapefile" % occ.metadataUrl, namespace=LM_PROC_NAMESPACE)
   SubElement(reqEl, "URI", 
              value="%s/services/sdm/occurrences" % WEBSERVICES_ROOT, 
              namespace=LM_PROC_NAMESPACE)
   
   reprEl = SubElement(httpMsgEl, "Representation", 
                       attrib={"HTTP-Version" : "1.1", "Status-Code" : "201"}, 
                       namespace=LM_PROC_NAMESPACE)
   reprMsgBodyEl = SubElement(reprEl, "MessageBody", namespace=LM_PROC_NAMESPACE)
   SubElement(reprMsgBodyEl, "OnlineResource", value=occ.metadataUrl, 
              namespace=LM_PROC_NAMESPACE)

# .............................................................................
def _addExperimentSubmissionAdditionalMetadata(el, exp):
   amEl = SubElement(el, "additionalMetadata", 
                     attrib={"id" : "experimentSubmission"})
   metadataEl = SubElement(amEl, "metadata")
   httpMsgEl = SubElement(metadataEl, "HTTPmessage", namespace=LM_PROC_NAMESPACE)
   reqEl = SubElement(httpMsgEl, "Request", namespace=LM_PROC_NAMESPACE)
   SubElement(reqEl, "Method", value="POST", namespace=LM_PROC_NAMESPACE)
   
   headersEl = SubElement(reqEl, "Headers", namespace=LM_PROC_NAMESPACE)
   ctHeader = SubElement(headersEl, "Header", namespace=LM_PROC_NAMESPACE)
   SubElement(ctHeader, "name", value="Content-Type", namespace=LM_PROC_NAMESPACE)
   SubElement(ctHeader, "value", value="application/xml", 
              namespace=LM_PROC_NAMESPACE)
   msgBodyEl = SubElement(reqEl, "MessageBody", namespace=LM_PROC_NAMESPACE)
   litContentEl = SubElement(msgBodyEl, "LiteralContent", namespace=LM_PROC_NAMESPACE)
   
   algoParameters = ["<lm:%s>%s</lm:%s>\n" % (k, exp.model._algorithm.parameters[k], k) for k in exp.model._algorithm.parameters.keys()]
   prjScns = ["<lm:projectionScenario>%s</lm:projectionScenario>\n" % prj._scenario.id for prj in exp.projections]
   prjMask = "<lm:projectionMask>%s</lm:projectionMask>" % exp.projections[0]._mask.id if len(exp.projections) > 0 else ""
   
   cdata = CDATA("""\
                     <lm:request xmlns:lm="{website}"
                                 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                 xsi:schemaLocation="{website} 
                                                     {website}/schemas/serviceRequest.xsd">
                        <lm:experiment>
                           <lm:algorithm>
                              <lm:algorithmCode>{algoCode}</lm:algorithmCode>
                              <lm:parameters>
{algoParams}
                              </lm:parameters>
                           </lm:algorithm>
                           <lm:occurrenceSetId>{occId}</lm:occurrenceSetId>
                           <lm:modelScenario>{mdlScnId}</lm:modelScenario>
{prjSection}
                           <lm:modelMask>{mdlMaskId}</lm:modelMask>
                           {prjMask}
                        </lm:experiment>
                     </lm:request>""".format(website=WEBSERVICES_ROOT, 
                                             algoCode=exp.model.algorithmCode,
              algoParams='                              '.join(algoParameters),
              occId=exp.model.occurrenceSet.id,
              mdlScnId=exp.model._scenario.id,
              prjSection='                           '.join(prjScns),
              mdlMaskId=exp.model._mask.id,
              prjMask=prjMask))
   
   litContentEl.append(cdata)
   
   SubElement(reqEl, "URI", 
              value="%s/services/sdm/experiments" % WEBSERVICES_ROOT, 
              namespace=LM_PROC_NAMESPACE)
   
   reprEl = SubElement(httpMsgEl, "Representation", 
                       attrib={"HTTP-Version" : "1.1", "Status-Code" : "202"}, 
                       namespace=LM_PROC_NAMESPACE)
   reprMsgBodyEl = SubElement(reprEl, "MessageBody", namespace=LM_PROC_NAMESPACE)
   SubElement(reprMsgBodyEl, "OnlineResource", value=exp.metadataUrl, 
              namespace=LM_PROC_NAMESPACE)
