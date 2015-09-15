"""
@summary: Module that will format an object so that it can be read by a 
             computational unit for processing
@author: CJ Grady
@version: 1.0
@status: alpha
@note: Part of the Factory pattern
@see: Formatter
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
from LmCommon.common.lmconstants import ProcessType, DEFAULT_POST_USER
from LmCommon.common.lmXml import CDATA, Element, SubElement, fromstring, tostring
from LmCommon.common.localconstants import WEBSERVICES_ROOT, ARCHIVE_USER

from LmServer.base.lmobj import LMError
from LmServer.common.localconstants import POINT_COUNT_MAX
from LmServer.rad.radJob import RADBuildGridJob, RADCompressJob, \
                                RADIntersectJob, RADSplotchJob, RADSwapJob, \
                                RADCalculateJob, RADGradyJob
from LmServer.sdm.sdmJob import SDMModelJob, SDMProjectionJob, \
                                      SDMOccurrenceJob

from LmWebServer.common.lmconstants import SCALE_DATA_TYPE, \
                             SCALE_PROJECTION_MAXIMUM, SCALE_PROJECTION_MINIMUM
from LmWebServer.formatters.formatter import Formatter, FormatterResponse

# .............................................................................
class JobFormatter(Formatter):
   """
   @summary: Formatter class for job output
   """
   # ..................................
   def format(self):
      """
      @summary: Formats the object
      @return: A response containing the content and metadata of the format 
                  operation
      @rtype: FormatterResponse
      """
      if isinstance(self.obj, SDMModelJob):
         dObj = self.obj.jobData._dataObj
         if dObj.algorithmCode == 'ATT_MAXENT':
            jobType = "3"
            processType = ProcessType.ATT_MODEL
            #rasterFormat = "image/x-aaigrid"
            rasterFormat = "AAIGrid"
         else:
            jobType = "1"
            processType = ProcessType.OM_MODEL
            #rasterFormat = "image/tiff"
            rasterFormat = "GTiff"
         jobId = self.obj.jobData.jid
         tree = Element("job")
         
         # Add post processing element to post back to job server
         postPEl = SubElement(tree, "postProcessing")
         postEl = SubElement(postPEl, "post")
         SubElement(postEl, "jobServer", value="%s/jobs" % WEBSERVICES_ROOT)
         
         
         SubElement(tree, "jobType", value=jobType)
         SubElement(tree, "processType", value=processType)
         SubElement(tree, "jobId", value=jobId)
         SubElement(tree, "parentUrl", value=dObj.metadataUrl)
         SubElement(tree, "url", value=dObj.metadataUrl)
         SubElement(tree, "userId", value=dObj.user)
         alg = SubElement(tree, "algorithm", 
                                         attrib={"code": dObj._algorithm.code})
         params = dObj._algorithm._parameters
         for param in params:
            SubElement(alg, "parameter", attrib={"id": param, 
                                                 "value": str(params[param])})
         points = SubElement(tree, "points", attrib={
                        "displayName": dObj.occurrenceSet.displayName.strip()})
         
         if dObj.algorithmCode != 'ATT_MAXENT':
            # Make sure we send the WKT of the points with the request

            # CJG - 2013-10-28
            # Since we have removed the projection information for 2163 layers,
            #   we need to tell openModeller that the points are also in 
            #   EPSG:4326.  Remove this once it is handled on compute side.
            
            if dObj.occurrenceSet.epsgcode != 2163:
               try:
                  wkt = dObj.occurrenceSet.getSRSAsWkt()
               except:
                  srs = dObj.occurrenceSet.createSRSFromEPSG()
                  wkt = srs.ExportToWkt()
            else:
               wkt = """GEOGCS["GCS_WGS_1984",DATUM["WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]"""
            
            SubElement(points, "wkt", value=wkt)
         
         # Using new Vector.features
         # ((fid, x, y), (fid, x, y) ...)
         pts = dObj.occurrenceSet.getFeaturesIdLongLat()
         for pt in pts:
            SubElement(points, "point", attrib={"id": str(pt[0]), 
                                                "y": str(pt[2]), 
                                                "x": str(pt[1])} )
         largeScn = dObj.layers[0].resolution < 0.09
         layers = SubElement(tree, "layers", attrib={"large" : str(largeScn)})
         
         for lyr in dObj.layers:
            lyrUrl = lyr.getURL(format=rasterFormat)
            SubElement(layers, "layer", value=lyrUrl)

         # Add mask
         mask = dObj.getMask()
         if mask is not None:
            SubElement(tree, "mask", value=mask.getURL(format=rasterFormat))
         
         cont = tostring(tree)
      elif isinstance(self.obj, SDMProjectionJob):
         dObj = self.obj.jobData._dataObj
         if dObj.algorithmCode == 'ATT_MAXENT':
            jobType = "4"
            processType = ProcessType.ATT_PROJECT
            #rasterFormat = "image/x-aaigrid"
            rasterFormat = "AAIGrid"
         else:
            jobType = "2"
            processType = ProcessType.OM_PROJECT
            #rasterFormat = "image/tiff"
            rasterFormat = "GTiff"
         jobId = self.obj.jobData.jid
         mdl = dObj.getModel()
         rawModel = open(mdl.ruleset).read()
         
         tree = Element("job")

         # Add post processing element to post back to job server
         postPEl = SubElement(tree, "postProcessing")
         postEl = SubElement(postPEl, "post")
         SubElement(postEl, "jobServer", value="%s/jobs" % WEBSERVICES_ROOT)
         
         SubElement(tree, "jobType", value=jobType)
         SubElement(tree, "processType", value=processType)
         SubElement(tree, "jobId", value=jobId)
         SubElement(tree, "parentUrl", value=dObj.getModel().metadataUrl)
         SubElement(tree, "url", value=dObj.metadataUrl)
         SubElement(tree, "userId", value=dObj.user)
         
         if mdl.algorithmCode != 'ATT_MAXENT':
            mdlEl = fromstring(rawModel)
            alg = tostring(mdlEl.find("Algorithm"))
            algEl = SubElement(tree, 'algorithm')
            algEl.append(CDATA(alg))
            #SubElement(algEl, '![CDATA[', value=alg)
            
         else:
            lambdasEl = SubElement(tree, 'lambdas')
            lambdasEl.append(CDATA(rawModel))
            #SubElement(lambdasEl, '![CDATA[', value=rawModel)
            #SubElement(tree, 'lambdas', value=rawModel)
         
            # If ARCHIVE_USER job, add post processing instruction to scale
            if dObj.user in [ARCHIVE_USER, DEFAULT_POST_USER]:
               scaleEl = SubElement(postPEl, "scale", 
                              attrib={"scaleMin" : SCALE_PROJECTION_MINIMUM,
                                      "scaleMax" : SCALE_PROJECTION_MAXIMUM,
                                      "dataType" : SCALE_DATA_TYPE})
            
            # ################################################################
            # #        TEMPORARY FIX FOR CHARLIE, Remove when possible       #
            # ################################################################
            # Multiply Charlie's data by 100,000
            # TODO: Remove this when we are done with Charlie's data.  Or, add
            #          entries in the config files for which users to do this
            #          for and what to multiply by
            elif dObj.user == 'cgwillis':
               multEl = SubElement(postPEl, "multiply", 
                                   attrib={"multiplier" : 100000})
               
            # Attach maxent algorithm parameters
            alg = SubElement(tree, "algorithm", 
                                         attrib={"code": mdl.algorithmCode})
            params = mdl._algorithm._parameters
            for param in params:
               SubElement(alg, "parameter", attrib={"id": param, 
                                                 "value": str(params[param])})

         largeScn = dObj.layers[0].resolution < 0.09
         layers = SubElement(tree, "layers", attrib={"large" : str(largeScn)})
         
         for lyr in dObj.layers:
            # get directly from EnvironmentalLayer
            lyrUrl = lyr.getURL(format=rasterFormat)
            SubElement(layers, "layer", value=lyrUrl)

         # Add mask
         mask = dObj.getMask()
         if mask is not None:
            SubElement(tree, "mask", value=mask.getURL(format=rasterFormat))
         
         temp = tostring(tree)
         #temp = temp.replace('<![CDATA[>', '<![CDATA[')
         #temp = temp.replace('</![CDATA[>', ']]>')
         temp = temp.replace('&lt;', '<')
         temp = temp.replace('&gt;', '>')
         cont = temp
      elif isinstance(self.obj, SDMOccurrenceJob):
         jobType = self.obj.processType
         jobId = self.obj.jobData.jid
         processType = self.obj.processType
         
         tree = Element("Job")

         # Add post processing element to post back to job server
         postPEl = SubElement(tree, "postProcessing")
         postEl = SubElement(postPEl, "post")
         SubElement(postEl, "jobServer", value="%s/jobs" % WEBSERVICES_ROOT)
         

         SubElement(tree, "jobType", value=self.obj.processType)
         SubElement(tree, "jobId", value=jobId)
         SubElement(tree, "processType", value=self.obj.processType)
         SubElement(tree, "parentUrl", value="None")
         SubElement(tree, "url", value=self.obj.jobData.objectUrl)
         SubElement(tree, "userId", value=self.obj.jobData.userId)
         
         if self.obj.processType == ProcessType.BISON_TAXA_OCCURRENCE:
            SubElement(tree, "pointsUrl", value=self.obj.jobData._dataObj['dlocation'])
            SubElement(tree, "maxPoints", value=POINT_COUNT_MAX)
         elif self.obj.processType in [ProcessType.GBIF_TAXA_OCCURRENCE, ProcessType.IDIGBIO_TAXA_OCCURRENCE]:
            SubElement(tree, "count", value=self.obj.dataObj['count'])
            SubElement(tree, "maxPoints", value=POINT_COUNT_MAX)
            occData = SubElement(tree, "points")
            occData.append(CDATA(self.obj.jobData.delimitedOccurrenceValues))
         else:
            raise LMError("Unknown occurrence set process type: %s" % self.obj.processType)
         #SubElement(occData, '![CDATA[', value=self.obj.jobData.delimitedOccurrenceValues)
         temp = tostring(tree)
         #temp = temp.replace('<![CDATA[>', '<![CDATA[')
         #temp = temp.replace('</![CDATA[>', ']]>')
         temp = temp.replace('&lt;', '<')
         temp = temp.replace('&gt;', '>')
         cont = temp
      elif isinstance(self.obj, RADIntersectJob):
         jobType = ProcessType.RAD_INTERSECT
         jobId = self.obj.jobData.jid
         processType = ProcessType.RAD_INTERSECT
         tree = Element("Job")
         
         # Add post processing element to post back to job server
         postPEl = SubElement(tree, "postProcessing")
         postEl = SubElement(postPEl, "post")
         SubElement(postEl, "jobServer", value="%s/jobs" % WEBSERVICES_ROOT)
         
         SubElement(tree, "jobType", value=self.obj.processType)
         SubElement(tree, "jobId", value=jobId)
         SubElement(tree, "processType", value=self.obj.processType)
         SubElement(tree, "parentUrl", value="None")
         SubElement(tree, "url", value=self.obj.jobData.objectUrl)
         SubElement(tree, "userId", value=self.obj.jobData.userId)
         sg = SubElement(tree, "shapegrid")
         sgUrl = self.obj.dataObj['shapegrid']['shapegridUrl']
         SubElement(sg, "url", value=sgUrl)
         SubElement(sg, "localIdIndex", value=self.obj.dataObj['shapegrid']['localIdIdx'])
         layerSet = SubElement(tree, "layerSet")
         for lyrKey in self.obj.dataObj['layerset'].keys():
            lyr = self.obj.dataObj['layerset'][lyrKey]
            lyrEl = SubElement(layerSet, "layer")
            SubElement(lyrEl, "index", value=lyrKey)
            SubElement(lyrEl, "url", value=lyr['layerUrl'])
            SubElement(lyrEl, "isRaster", value=str(lyr['isRaster']))
            try:
               SubElement(lyrEl, "resolution", value=lyr['resolution'])
            except:
               pass
            SubElement(lyrEl, "isOrganism", value=lyr['isOrganism'])
            SubElement(lyrEl, "attrPresence", value=lyr['attrPresence'])
            SubElement(lyrEl, "minPresence", value=lyr['minPresence'])
            SubElement(lyrEl, "maxPresence", value=lyr['maxPresence'])
            SubElement(lyrEl, "percentPresence", value=lyr['percentPresence'])
            SubElement(lyrEl, "attrAbsence", value=lyr['attrAbsence'])
            SubElement(lyrEl, "minAbsence", value=lyr['minAbsence'])
            SubElement(lyrEl, "maxAbsence", value=lyr['maxAbsence'])
            SubElement(lyrEl, "percentAbsence", value=lyr['percentAbsence'])
         
         cont = tostring(tree)
         
      elif isinstance(self.obj, (RADSplotchJob, RADSwapJob, RADCompressJob, 
                                 RADCalculateJob, RADBuildGridJob, RADGradyJob)):
         cont = self.obj.serialize()
         jobType = self.obj.processType
         jobId = self.obj.jobData.jid
         
      else:
         raise LMError("Don't know how to format object of type: %s for job" % str(self.obj.__class__))
      
      return FormatterResponse(cont, contentType="application/xml", filename="job%s-%s.xml" % (jobType, jobId))
