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
from LmCommon.common.lmXml import CDATA, Element, PI, QName, \
                                  register_namespace, setDefaultNamespace, \
                                  SubElement, tostring
from LmCommon.common.lmconstants import LM_NAMESPACE, LM_PROC_NAMESPACE
from LmCommon.common.localconstants import WEBSERVICES_ROOT

from LmServer.base.layer import Raster
from LmServer.base.utilities import getPackageId

from LmWebServer.common.lmconstants import XSI_NAMESPACE
from LmWebServer.lmEml.contact import addContactElement
from LmWebServer.lmEml.emlConstants import EML_NAMESPACE, \
                                           EML_SCHEMA_LOCATION, EML_SYSTEM
from LmWebServer.lmEml.radBucket import addRadBucket
from LmWebServer.lmEml.serviceRaster import addServiceRaster
from LmWebServer.lmEml.serviceVector import addServiceVector


# .............................................................................
def buildRadExperimentPackage(exp):
   pis = []
   pis.append(PI("xml", 'version="1.0"'))
   
   register_namespace('', EML_NAMESPACE)
   setDefaultNamespace(EML_NAMESPACE)
   
   el = Element("eml", attrib={
                        "packageId" : getPackageId(exp, separator='.'),
                        "system" : EML_SYSTEM,
                        QName(XSI_NAMESPACE, "schemaLocation") : \
                              "%s\n%s" % (EML_NAMESPACE, EML_SCHEMA_LOCATION)})
   
   dsEl = SubElement(el, "dataset", 
                     attrib={"id" : getPackageId(exp, separator='.')})
   SubElement(dsEl, "title", value=exp.title)
   SubElement(SubElement(dsEl, "creator"),
              "organizationName",
              value="Lifemapper")
   
   if len(exp.keywords) > 0:
      kwsEl = SubElement(dsEl, "keywordSet")
      for kw in exp.keywords:
         SubElement(kwsEl, "keyword", value=kw)
         
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

   addRadExperiment(dsEl, exp)
   
   return "%s\n%s" % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# .............................................................................
def addRadExperiment(el, exp):
   for bkt in exp.bucketList:
      addRadBucket(el, bkt)
   
   _addProtocol(el, exp)
   
   # Add layers
   for lyr in exp.orgLayerSet.layers:
      if isinstance(lyr, Raster):
         addServiceRaster(el, lyr)
      else:
         addServiceVector(el, lyr)
   
   for lyr in exp.expLayerSet.layers:
      if isinstance(lyr, Raster):
         addServiceRaster(el, lyr)
      else:
         addServiceRaster(el, lyr)

# .............................................................................
def _addProtocol(el, exp):
   # Add protocol section
   protocolEl = SubElement(el, "protocol")
   
   _addExperimentProcStep(protocolEl, exp)

   for bkt in exp.bucketList:
      _addBucketProcStep(protocolEl, exp, bkt)


# .............................................................................
def _addExperimentProcStep(protocolEl, exp):
   # Add experiment
   expProcStepEl = SubElement(protocolEl, "proceduralStep", 
                              attrib={
                                      "object" : "experiment", 
                                      "ref" : getPackageId(exp, separator='.')})
   httpMessageEl = SubElement(expProcStepEl, "HTTPmessage", 
                              namespace=LM_PROC_NAMESPACE)
   reqEl = SubElement(httpMessageEl, "Request", namespace=LM_PROC_NAMESPACE)
   SubElement(reqEl, "Method", value="POST", namespace=LM_PROC_NAMESPACE)
   reqHeadersEl = SubElement(reqEl, "Headers", namespace=LM_PROC_NAMESPACE)
   ctHeadEl = SubElement(reqHeadersEl, "Header", namespace=LM_PROC_NAMESPACE)
   SubElement(ctHeadEl, "name", value="Content-Type", 
              namespace=LM_PROC_NAMESPACE)
   SubElement(ctHeadEl, "value", value="application/xml", 
              namespace=LM_PROC_NAMESPACE)
   
   msgBodyEl = SubElement(reqEl, "MessageBody", namespace=LM_PROC_NAMESPACE)
   litCntEl = SubElement(msgBodyEl, "LiteralContent")
   commentEl = CDATA()
   expReqEl = SubElement(commentEl, "request", 
              attrib={"xmlns:lm" : LM_NAMESPACE,
                      "xmlns:xsi" : XSI_NAMESPACE,
                      "xsi:schemaLocation" : \
                        "%s /schemas/radServiceRequest.xsd" % WEBSERVICES_ROOT}, 
              namespace=LM_NAMESPACE)
   expEl = SubElement(expReqEl, "experiment", namespace=LM_NAMESPACE)
   SubElement(expEl, "name", value=exp.name, namespace=LM_NAMESPACE)
   SubElement(expEl, "epsgCode", value=exp.epsgcode, namespace=LM_NAMESPACE)
   litCntEl.append(commentEl)
   
   SubElement(reqEl, "URI", 
              value="%s/services/rad/experiments" % WEBSERVICES_ROOT, 
              namespace=LM_PROC_NAMESPACE)

   repEl = SubElement(reqEl, "Representation", namespace=LM_PROC_NAMESPACE, 
                      attrib={"HTTP-Version" : "1.1", "Status-Code" : "201"})
   repMsgBodyEl = SubElement(repEl, "MessageBody", namespace=LM_PROC_NAMESPACE)
   SubElement(repMsgBodyEl, "OnlineResource", value="%s/xml" % exp.metadataUrl, 
              namespace=LM_PROC_NAMESPACE)

   secEl = SubElement(expProcStepEl, "section")
   SubElement(secEl, "para", 
              value="Send a post request to %s/services/rad/experiments/ to initialize a new experiment" % WEBSERVICES_ROOT)

# .............................................................................
def _addBucketProcStep(protocolEl, exp, bkt):
   # Add bucket
   procStepEl = SubElement(protocolEl, "proceduralStep", 
                           attrib={"object" : "bucket",
                                   "ref" : getPackageId(bkt, separator='.')})

   httpMessageEl = SubElement(procStepEl, "HTTPmessage", 
                              namespace=LM_PROC_NAMESPACE)
   reqEl = SubElement(httpMessageEl, "Request", namespace=LM_PROC_NAMESPACE)
   SubElement(reqEl, "Method", value="POST", namespace=LM_PROC_NAMESPACE)
   reqHeadersEl = SubElement(reqEl, "Headers", namespace=LM_PROC_NAMESPACE)
   ctHeadEl = SubElement(reqHeadersEl, "Header", namespace=LM_PROC_NAMESPACE)
   SubElement(ctHeadEl, "name", value="Content-Type", 
              namespace=LM_PROC_NAMESPACE)
   SubElement(ctHeadEl, "value", value="application/xml", 
              namespace=LM_PROC_NAMESPACE)
   
   msgBodyEl = SubElement(reqEl, "MessageBody", namespace=LM_PROC_NAMESPACE)
   litCntEl = SubElement(msgBodyEl, "LiteralContent")

   if bkt.shapegrid.cellsides == 4:
      shapeSection = "<lmRad:cellShape>square</lmRad:cellShape>"
   else:
      shapeSection = "<lmRad:cellShape>hexagon</lmRad:cellShape>"

   commentEl = CDATA("""\
                        <?xml version="1.0" encoding="UTF-8"?>
                        <wps:Execute version="1.0.0" service="WPS" 
                                     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                                     xmlns="http://www.opengis.net/wps/1.0.0" 
                                     xmlns:wfs="http://www.opengis.net/wfs" 
                                     xmlns:wps="http://www.opengis.net/wps/1.0.0" 
                                     xmlns:ows="http://www.opengis.net/ows/1.1" 
                                     xmlns:xlink="http://www.w3.org/1999/xlink" 
                                     xmlns:lmRad="{website}"
                                     xsi:schemaLocation="http://www.opengis.net/wps/1.0.0 http://schemas.opengis.net/wps/1.0.0/wpsAll.xsd">
                          <ows:Identifier>addbucket</ows:Identifier>
                          <wps:DataInputs>
                              <wps:Input>
                                 <ows:Identifier>bucket</ows:Identifier>
                                 <wps:Data>
                                    <wps:ComplexData>
                                       <lmRad:shapegrid>
                                          <lmRad:name>{shapegridName}</lmRad:name>
                                          {shapeSection}
                                          <lmRad:cellSize>{cellSize}</lmRad:cellSize>
                                          <lmRad:mapUnits>{mapUnits}</lmRad:mapUnits>
                                          <lmRad:epsgCode>{epsgCode}</lmRad:epsgCode>
                                          <lmRad:bounds>{bounds}</lmRad:bounds>
                                       </lmRad:shapegrid>
                                    </wps:ComplexData>
                                 </wps:Data>
                              </wps:Input>
                          </wps:DataInputs>
                          <wps:ResponseForm>
                            <wps:RawDataOutput mimeType="application/gml-3.1.1">
                              <ows:Identifier>result</ows:Identifier>
                            </wps:RawDataOutput>
                          </wps:ResponseForm>
                        </wps:Execute>""".format(
                                               shapgridName=bkt.shapegrid.name, 
                                               shapeSection=shapeSection, 
                                               cellSize=bkt.shapegrid.cellsize, 
                                               mapUnits=bkt.shapegrid.mapUnits, 
                                               epsgCode=bkt.shapegrid.epsgcode, 
                                               bounds=bkt.shapegrid.bbox,
                                               website=WEBSERVICES_ROOT))
   
   litCntEl.append(commentEl)
   
   SubElement(reqEl, "URI", 
              value="%s/services/rad/experiments/%s/addBucket" % (WEBSERVICES_ROOT, exp.id), 
              namespace=LM_PROC_NAMESPACE)

   repEl = SubElement(reqEl, "Representation", namespace=LM_PROC_NAMESPACE, 
                      attrib={"HTTP-Version" : "1.1", "Status-Code" : "201"})
   repMsgBodyEl = SubElement(repEl, "MessageBody", namespace=LM_PROC_NAMESPACE)
   SubElement(repMsgBodyEl, "OnlineResource", value="%s/xml" % bkt.metadataUrl, 
              namespace=LM_PROC_NAMESPACE)

   secEl = SubElement(procStepEl, "section")
   SubElement(secEl, "para", 
              value="Send a post request to %s/services/rad/experiments/%s/addBucket to initialize a new experiment" % (WEBSERVICES_ROOT, exp.id))

