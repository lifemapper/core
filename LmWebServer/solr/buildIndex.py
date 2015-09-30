"""
@summary: This script builds the projections index for Solr
@author: CJ Grady
"""
from mx.DateTime.DateTime import DateTimeFromMJD
#from subprocess import Popen
import subprocess
from time import sleep

from LmCommon.common.lmconstants import JobStatus
from LmServer.common.log import ConsoleLogger
from LmServer.db.peruser import Peruser

COLLECTION = "lmArchive"
SOLR_POST_COMMAND = '/opt/solr/bin/post'

# .............................................................................
def getBboxWKT(bbox):
   """
   @summary: Gets the Solr WKT for a bounding box
   @param bbox: The bounding box to format
   """
   return "ENVELOPE({0}, {2}, {3}, {1})".format(*bbox)

# .............................................................................
def getModTimeStringFromMJD(mjd):
   """
   @summary: Converts a time in Modified Julian Day format into a string format
                that Solr can index
   @param mjd: The time in MJD format
   """
   return DateTimeFromMJD(mjd).strftime('%Y-%m-%dT%H:%M:%SZ')

# .............................................................................
def makeProjectionDoc(prj):
   """
   @summary: Make a projection document to be posted to Solr
   @param prj: A projection object to convert into a document
   """
   id = prj.getId()
   userId = prj.getUserId()
   
   occ = prj.getOccurrenceSet()
   
   displayName = occ.displayName
   occId = occ.getId()
   occUrl = occ.metadataUrl
   occBboxWKT = getBboxWKT(occ.bbox)
   occModTime = getModTimeStringFromMJD(occ.modTime)
   occDlUrl = "%s/shapefile" % occ.metadataUrl
   numPoints = occ.count
   epsg = occ.epsgcode
   
   mdl = prj.getModel()

   mdlId = mdl.id
   mdlUrl = mdl.metadataUrl
   rulesetUrl = "%s/model" % mdl.metadataUrl
   mdlModTime = getModTimeStringFromMJD(mdl.modTime)

   mdlScn = mdl.getScenario()

   mdlScnCode = mdlScn.code
   mdlScnId = mdlScn.id
   mdlScnUrl = mdlScn.metadataUrl

   algoCode = mdl.algorithmCode
   algoParams = mdl.getAlgorithmParameters()

   prjId = prj.id
   prjUrl = prj.metadataUrl
   prjBboxWKT = getBboxWKT(prj.bbox)
   prjDlUrl = "%s/GTiff" % prj.metadataUrl
   prjModTime = getModTimeStringFromMJD(prj.modTime)

   prjScn = prj.getScenario()

   prjScnCode = prjScn.code
   prjScnId = prjScn.id
   prjScnUrl = prjScn.metadataUrl
   
   doc = """\
<add>
   <doc>
      <field name="id">{id}</field>
      <field name="userId">{userId}</field>
      <field name="displayName">{displayName}</field>
      <field name="occurrenceSetId">{occId}</field>
      <field name="occurrenceSetMetadataUrl">{occUrl}</field>
      <field name="occurrenceSetBBox">{occBboxWKT}</field>
      <field name="occurrenceSetModTime">{occModTime}</field>
      <field name="occurrenceSetDownloadUrl">{occDlUrl}</field>
      <field name="numberOfOccurrencePoints">{numPoints}</field>
      <field name="epsgCode">{epsg}</field>
      <field name="modelId">{mdlId}</field>
      <field name="modelMetadataUrl">{mdlUrl}</field>
      <field name="modelRulesetUrl">{rulesetUrl}</field>
      <field name="modelModTime">{mdlModTime}</field>
      <field name="modelScenarioCode">{mdlScnCode}</field>
      <field name="modelScenarioId">{mdlScnId}</field>
      <field name="modelScenarioUrl">{mdlScnUrl}</field>
      <field name="algorithmCode">{algoCode}</field>
      <field name="algorithmParameters">{algoParams}</field>
      <field name="projectionId">{prjId}</field>
      <field name="projectionMetadataUrl">{prjUrl}</field>
      <field name="projectionBBox">{prjBboxWKT}</field>
      <field name="projectionDownloadUrl">{prjDlUrl}</field>
      <field name="projectionModTime">{prjModTime}</field>
      <field name="projectionScenarioCode">{prjScnCode}</field>
      <field name="projectionScenarioId">{prjScnId}</field>
      <field name="projectionScenarioUrl">{prjScnUrl}</field>
   </doc>
</add>""".format(id=id, userId=userId, displayName=displayName, occId=occId,
                 occUrl=occUrl, occBboxWKT=occBboxWKT, occModTime=occModTime,
                 occDlUrl=occDlUrl, numPoints=numPoints, epsg=epsg, 
                 mdlId=mdlId, mdlUrl=mdlUrl, rulesetUrl=rulesetUrl, 
                 mdlModTime=mdlModTime, mdlScnCode=mdlScnCode, 
                 mdlScnId=mdlScnId, mdlScnUrl=mdlScnUrl, algoCode=algoCode, 
                 algoParams=algoParams, prjId=prjId, prjUrl=prjUrl, 
                 prjBboxWKT=prjBboxWKT, prjDlUrl=prjDlUrl, 
                 prjModTime=prjModTime, prjScnCode=prjScnCode,
                 prjScnId=prjScnId, prjScnUrl=prjScnUrl)
   return doc

# .............................................................................
def postDocument(doc, collection, mimeType='application/xml'):
   """
   @summary: Posts a document to a local Solr server
   @param doc: A document to post (string)
   @param collection: The collection to add the document to
   @param mimeType: (optional) The MIME type of the document
   """
   cmd = "{cmd} -c {collection} -type {mimeType} -out no -d $'{data}'".format(
            cmd=SOLR_POST_COMMAND, collection=collection, mimeType=mimeType,
            data=doc)
   #p = Popen(cmd)
   #while p.poll() is None:
   #   sleep(.2)
   subprocess.call(cmd, shell=True)
      

# .............................................................................
if __name__ == "__main__":
   
   peruser = Peruser(ConsoleLogger())
   peruser.openConnections()
   
   beforeTime = None
   maxReturned = 10000
   startRecord = 0
   
   prjs = peruser.listProjections(startRecord, maxReturned, 
                              beforeTime=beforeTime, status=JobStatus.COMPLETE, 
                              atom=False)
   while len(prjs) > 0:
      # Make documents and post
      for prj in prjs:
         doc = makeProjectionDoc(prj)
         # Post document
         postDocument(doc, COLLECTION, mimeType='application/xml')
      
      #startRecord += len(prjs)
      beforeTime = prjs[-1].modTime
      if len(prjs) == maxReturned:
         prjs = peruser.listProjections(startRecord, maxReturned, 
                              beforeTime=beforeTime, status=JobStatus.COMPLETE, 
                              atom=False)
      else:
         prjs = []
   
   peruser.closeConnections()
   
   
