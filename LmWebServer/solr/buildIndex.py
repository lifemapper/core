"""
@summary: This script builds the projections index for Solr
@author: CJ Grady
@todo: Get constants from local constants
@todo: Add spatial components
@todo: Change calling mechanism from a loop through projections to posting 
          projections individually as they are completed.
"""
from mx.DateTime.DateTime import DateTimeFromMJD
import os
#from subprocess import Popen
import subprocess
import tempfile
#from time import sleep

from LmCommon.common.lmconstants import JobStatus
from LmServer.common.datalocator import EarlJr
from LmServer.common.localconstants import TEMP_PATH
from LmServer.common.log import ConsoleLogger
from LmServer.db.peruser import Peruser

COLLECTION = "lmArchive"
SOLR_POST_COMMAND = '/opt/solr/bin/post'

POST_THRESHOLD = 500

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
   </doc>""".format(id=id, userId=userId, displayName=displayName, occId=occId,
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
def makeOccurrenceDoc(occId, acceptedName, numPoints, numMod, binomial, downloadUrl):
   """
   @summary: Make an occurrence set document to be posted to Solr
   @param occId: The occurrence set id
   @param acceptedName: The accepted name of the occurrence set taxon
   @param numPoints: The number of points in the occurrence set
   @param binomial: The binomial name of the taxon
   @param downloadUrl: A URL where the occurrence set can be downloaded
   """
   doc = """\
   <doc>
      <field name="id">{id}</field>
      <field name="displayName">{displayName}</field>
      <field name="occurrenceSetId">{occId}</field>
      <field name="numberOfOccurrencePoints">{numPoints}</field>
      <field name="numberOfModels">{numModels}</field>
      <field name="binomial">{binomial}</field>
      <field name="occurrenceSetDownloadUrl">{occDlUrl}</field>
   </doc>""".format(id=occId, displayName=acceptedName, occId=occId,
                 numPoints=numPoints, numModels=numMod, binomial=binomial, 
                 occDlUrl=downloadUrl)
   return doc

# .............................................................................
def postDocuments(docs, collection, mimeType='application/xml'):
   """
   @summary: Posts a document to a local Solr server
   @param doc: A list of documents to post (list of strings)
   @param collection: The collection to add the document to
   @param mimeType: (deprecated) This is not needed if posting a file
   """
   
   fd, postFn = tempfile.mkstemp(suffix='.xml', dir=TEMP_PATH)
   os.write(fd, "<add>\n")
   for doc in docs:
      os.write(fd, doc)
      os.write(fd, '\n')
   os.write(fd, "</add>")
   os.close(fd)
   
   cmd = "{cmd} -c {collection} -out no {filename}".format(
            cmd=SOLR_POST_COMMAND, collection=collection, filename=postFn)

   subprocess.call(cmd, shell=True)

   # Clean up
   os.remove(postFn)
      
# .............................................................................
def buildProjectionIndex():
   """
   @summary: Build the Lifemapper archive Solr index
   """
   peruser = Peruser(ConsoleLogger())
   peruser.openConnections()
   
   beforeTime = None
   maxReturned = 10000
   startRecord = 0
   
   prjs = peruser.listProjections(startRecord, maxReturned, 
                              beforeTime=beforeTime, status=JobStatus.COMPLETE, 
                              atom=False)
   docs = []
   while len(prjs) > 0:
      # Make documents and post
      for prj in prjs:
         docs.append(makeProjectionDoc(prj))
         
         # If there are enough docs to meet the threshold, post them
         if len(docs) >= POST_THRESHOLD:
            # Post document
            postDocuments(docs, COLLECTION, mimeType='application/xml')
            docs = []
      
      #startRecord += len(prjs)
      beforeTime = prjs[-1].modTime
      if len(prjs) == maxReturned:
         prjs = peruser.listProjections(startRecord, maxReturned, 
                              beforeTime=beforeTime, status=JobStatus.COMPLETE, 
                              atom=False)
      else:
         prjs = []
   
   # Post any remaining docs
   if len(docs) > 0:
      postDocuments(docs, COLLECTION, mimeType='application/xml')
   
   peruser.closeConnections()
   
# .............................................................................
def buildOccurrenceIndex():
   """
   @summary: Build the Lifemapper Species Hint service index
   """
   collection = "lmSpeciesHint"
   peruser = Peruser(ConsoleLogger())
   peruser.openConnections()
   
   speciesList = peruser.getOccurrenceStats()
   #speciesList.sort(compareTitles)
   peruser.closeConnections()
   
   ej = EarlJr()
   
   docs = []
   for sp in speciesList:
      acceptedName = sp[1].strip()
      nameParts = acceptedName.split(' ')
      
      binomial = nameParts[0]
      
      if len(nameParts) > 1:
         if nameParts[1].lower() == nameParts[1] and \
               not nameParts[1].startswith('('): # This is a check for an author
            binomial = "%s %s" % (nameParts[0], nameParts[1]) 
      
      occId = str(sp[0])
      numOcc = str(sp[3])
      numMod = str(sp[4])
      
      downloadUrl = "{0}/shapefile".format(ej.constructLMMetadataUrl(
                                               "occurrences", occId, "sdm"))
      
      docs.append(makeOccurrenceDoc(occId, acceptedName, numOcc, numMod, 
                                    binomial, downloadUrl))
      
      # If the number of docs has reached the threshold
      if len(docs) >= POST_THRESHOLD:
         # Post document
         postDocuments(docs, collection, mimeType='application/xml')
   
   # Post any leftovers
   if len(docs) > 0:
      postDocuments(docs, collection, mimeType='application/xml')
   

# .............................................................................
if __name__ == "__main__":
   buildProjectionIndex()
   buildOccurrenceIndex()
   