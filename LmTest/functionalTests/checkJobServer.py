"""
@summary: Tests job retrieval and post for the job server
@author: CJ Grady
@version: 1.0
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
from mx.DateTime import gmt
import os
from random import choice
from StringIO import StringIO
import traceback
import urllib
import urllib2
import zipfile

from LmCommon.common.lmXml import deserialize, fromstring
from LmCommon.common.lmconstants import (JobStatus, ProcessType, Instances)

from LmServer.sdm.algorithm import Algorithm
from LmServer.common.lmconstants import JobFamily, TEST_DATA_PATH
from LmServer.common.localconstants import (WEBSERVICES_ROOT, SCENARIO_PACKAGE_MAPUNITS, 
                                            SCENARIO_PACKAGE_EPSG)
from LmServer.common.log import ThreadLogger
from LmServer.db.scribe import Scribe
from LmServer.sdm.envlayer import EnvironmentalLayer
from LmServer.sdm.scenario import Scenario
from LmServer.sdm.sdmJob import SDMModelJob, SDMOccurrenceJob, SDMProjectionJob

from LmWebServer.services.common.userdata import DataPoster

# Note: not using constant (LM_JOB_SERVER) because it is part of LmCompute
JOB_SERVER_ROOT = "{0}/jobs".format(WEBSERVICES_ROOT) 
# .............................................................................
def _getJob(jobType, user):
   """
   @summary: Gets a test job id.  Since we are not running this job, the status
                should be reset
   """
   getJobUrl = "{0}?numJobs=1&request=GetJob&user={1}&jobTypes={2}".format(
                                                                 JOB_SERVER_ROOT,
                                                                 user,
                                                                 jobType)
   getJobResponse = urllib2.urlopen(getJobUrl).read()
   jobs = []
   try:
      jobStream = StringIO()
      jobStream.write(getJobResponse)
      jobStream.seek(0)
            
      with zipfile.ZipFile(jobStream, 'r', allowZip64=True) as z:
         for zname in z.namelist():
            jXml = z.read(zname)
            try:
               j = deserialize(fromstring(jXml)) # This will fail if not xml
               jobs.append(jXml)
            except:
               pass # Not xml
               
   except Exception, e: 
      # Try as XML
      try:
         _ = deserialize(fromstring(getJobResponse)) # This will fail if not xml
         jobs.append(getJobResponse)
      except Exception, e2:
         # Fail out
         pass
   #job = deserialize(fromstring(getJobResponse))
   if len(jobs) > 0:
      job = deserialize(fromstring(jobs[0]))
      jobId = int(job.jobId)
   else:
      raise Exception('Failed to retrieve any jobs')
   return jobId

# .............................................................................
def _postJob(processType, jobId, content, component, contentType):
   """
   @summary: Posts test job content back to the job server
   """
   parameters = [
                 ("jobType", processType),
                 ("jobId", jobId),
                 ("request", "PostJob"),
                 ("component", component)]
   headers = {"Content-Type": contentType}
   urlparams = urllib.urlencode(parameters)
   
   url = "{0}?{1}".format(JOB_SERVER_ROOT, urlparams)
   postJobReq = urllib2.Request(url, data=content, headers=headers)
   postJobResponse = urllib2.urlopen(postJobReq).read()
   return postJobResponse

# .............................................................................
def _testJob(processType, jobId, userId, postContent, postComponent, postContentType):
   """
   @summary: Pseudo-tests for a job
   """
   pulledJobId = _getJob(processType, userId)
   
   # If the job we pulled is not the one we inserted, reset it
   if int(pulledJobId) != jobId:
      _updateJob(processType, pulledJobId, 1)
   
   # Update the job, make sure to use a status that we don't normally use so
   #    that we can see it is a test job
   _updateJob(processType, jobId, 99, progress=99)
   
   # Post the job results back
   _updateJob(processType, jobId, JobStatus.COMPLETE, progress=100)
   _postJob(processType, jobId, postContent, postComponent, postContentType)
   
# .............................................................................
def _updateJob(processType, jobId, status, progress=1):
   """
   @summary: Tests that a job can be updated
   """
   parameters = [("request", "UpdateJob"),
                 ("jobType", processType),
                 ("jobId", jobId),
                 ("status", status),
                 ("progress", progress)]
   urlparams = urllib.urlencode(parameters)
   updateJobReq = urllib2.Request(JOB_SERVER_ROOT, data=urlparams)
   updateJobResponse = urllib2.urlopen(updateJobReq).read()
   return updateJobResponse

# .............................................................................
def cleanUp(scribe, occ, occJobId, mdlJobId, prjJobId):
   """
   @summary: Removes any leftover jobs
   """
   _cleanUpSDMJob(scribe, prjJobId, "Projection job")
   _cleanUpSDMJob(scribe, mdlJobId, "Model job")
   _cleanUpSDMJob(scribe, occJobId, "Occurrence set job")
   
   scribe.completelyRemoveOccurrenceSet(occ)

# .............................................................................
def _cleanUpSDMJob(scribe, jobId, jobTypeStr):
   # Look for job
   try:
      job = scribe.getJob(JobFamily.SDM, jobId)
      print "Deleting {0} (job id: {1})".format(jobTypeStr, jobId)
      scribe.deleteSDMJob(job)
   except Exception, e:
      print "Exception (%s) deleting job" % str(e)
   
# .............................................................................
def insertTestJobChain(scribe, userId):
   """
   @summary: Initiates the jobs for testing
   """
   
   # Post an occurrence set via the data poster
   # Use that id to start with
   # Initialize the chain
   
   dp = DataPoster(userId, scribe.log)
   
   body = open(os.path.abspath(os.path.join(TEST_DATA_PATH, 'testPoints.zip')), 'rb').read()
   
   parameters = {
                 "pointsType" : "shapefile",
                 "displayName" : "Test Chain%s" % gmt().mjd,
                 "epsgCode" : "4326",
                 "name" : "Test points%s" % gmt().mjd
                 }
   
   
   occ = dp.postSDMOccurrenceSet(parameters, body)
   
   rawFn = os.path.abspath(os.path.join(TEST_DATA_PATH, 'testPoints.csv'))
   
   occ.setRawDLocation(os.path.abspath(rawFn), gmt().mjd)
   occ.updateStatus(JobStatus.INITIALIZE)
   
   scribe.updateOccState(occ)
   
   # Pick between Maxent and Bioclim
   algoCodes = ['BIOCLIM', 'ATT_MAXENT']
   algoCode = choice(algoCodes)
   alg = Algorithm(algoCode)
   
   if algoCode == 'BIOCLIM':
      mdlProcType = ProcessType.OM_MODEL
      prjProcType = ProcessType.OM_PROJECT
   else:
      mdlProcType = ProcessType.ATT_MODEL
      prjProcType = ProcessType.ATT_PROJECT
   
   # Pick a scenario
   scnAtoms = scribe.listScenarios(0, 1, userId=userId)
   if len(scnAtoms) > 0:
      scnId = scnAtoms[0].id
      mdlScn = prjScn = scribe.getScenario(scnId)
   else:
      # Need to insert data
      lyr1 = EnvironmentalLayer("testLayer1", title="Testing layer 1", 
                                valUnits='degrees', gdalFormat='GTiff', 
                                mapunits=SCENARIO_PACKAGE_MAPUNITS, resolution='2.5', 
                                epsgcode=SCENARIO_PACKAGE_EPSG, 
                                layerType='type1', userId=userId)
      lyr2 = EnvironmentalLayer("testLayer2", title="Testing layer 2", 
                                valUnits='degrees', gdalFormat='GTiff', 
                                mapunits=SCENARIO_PACKAGE_MAPUNITS, resolution='2.5', 
                                epsgcode=SCENARIO_PACKAGE_EPSG, 
                                layerType='type2', userId=userId)
      lyr3 = EnvironmentalLayer("testLayer3", title="Testing layer 3", 
                                valUnits='degrees', gdalFormat='GTiff', 
                                mapunits=SCENARIO_PACKAGE_MAPUNITS, resolution='2.5', 
                                epsgcode=SCENARIO_PACKAGE_EPSG, 
                                layerType='type3', userId=userId)
      lyr4 = EnvironmentalLayer("testLayer4", title="Testing layer 4", 
                                valUnits='degrees', gdalFormat='GTiff', 
                                mapunits=SCENARIO_PACKAGE_MAPUNITS, resolution='2.5', 
                                epsgcode=SCENARIO_PACKAGE_EPSG, 
                                layerType='type4', userId=userId)
      # Write layer data
      with open(os.path.join(TEST_DATA_PATH, 'testLayer1.tif')) as f1:
         lyr1Cnt = f1.read()
      lyr1.writeLayer(srcData=lyr1Cnt)
      
      with open(os.path.join(TEST_DATA_PATH, 'testLayer2.tif')) as f2:
         lyr2Cnt = f2.read()
      lyr2.writeLayer(srcData=lyr2Cnt)
      
      with open(os.path.join(TEST_DATA_PATH, 'testLayer3.tif')) as f3:
         lyr3Cnt = f3.read()
      lyr3.writeLayer(srcData=lyr3Cnt)
      
      with open(os.path.join(TEST_DATA_PATH, 'testLayer4.tif')) as f4:
         lyr4Cnt = f4.read()
      lyr4.writeLayer(srcData=lyr4Cnt)
      
      # Insert layers
      lyr1 = scribe.insertLayer(lyr1)
      lyr2 = scribe.insertLayer(lyr2)
      lyr3 = scribe.insertLayer(lyr3)
      lyr4 = scribe.insertLayer(lyr4)
      
      # Create scenario
      scn = Scenario('testScn', title="Job server test scenario", 
               units="dd", res="2.5", epsgcode=4326, 
               layers=[lyr1, lyr2, lyr3, lyr4], userId=userId)
      
      scribe.insertScenario(scn)
      scnId = scn.getId()
      mdlScn = prjScn = scn
      
   # @TODO: This should be changed to read the DATASOURCE from LmServer.localconstants
   DATASOURCE = Instances.GBIF
   if DATASOURCE == Instances.BISON:
      processtype = ProcessType.BISON_TAXA_OCCURRENCE
   elif DATASOURCE == Instances.GBIF and os.path.exists(occ.getRawDLocation()):
      processtype = ProcessType.GBIF_TAXA_OCCURRENCE
   elif DATASOURCE == Instances.IDIGBIO and os.path.exists(occ.getRawDLocation()):
      processtype = ProcessType.IDIGBIO_TAXA_OCCURRENCE
   
   jobs = scribe.initSDMChain(userId, occ, [alg], mdlScn, [prjScn], 
                              occJobProcessType=processtype, priority=10)
   
   for job in jobs:
      if isinstance(job, SDMProjectionJob):
         prjJobId = job.getId()
         print "Projection job id:", prjJobId
      elif isinstance(job, SDMModelJob):
         mdlJobId = job.getId()
         print "Model job id:", mdlJobId
      elif isinstance(job, SDMOccurrenceJob):
         occJobId = job.getId()
         print "Occurrence job id:", occJobId

   return occ, occJobId, mdlJobId, prjJobId, mdlProcType, prjProcType

# .............................................................................
def testModelJob(mdlJobType, mdlJobId, userId):
   """
   @summary: Pseudo-tests for model jobs
   """
   postComponent = "model"
   if mdlJobType == ProcessType.OM_MODEL:
      postContentType = "application/xml"
      # Note: This isn't a real result, but it will pass the sanity check
      postContent = """\
<SerializedModel>
   <Sampler>
      <Environment NumLayers="16">
         <Map Id="/var/lm/data/layers/lifemapper_org/services/sdm/layers/266/GTiff/GTiff" IsCategorical="0" Min="0" Max="26.34738922119140625"/>
         <Map Id="/var/lm/data/layers/lifemapper_org/services/sdm/layers/267/GTiff/GTiff" IsCategorical="0" Min="0" Max="58.335498809814453125"/>
         <Mask Id="/var/lm/data/layers/lifemapper_org/services/sdm/layers/266/GTiff/GTiff"/>
      </Environment>
      <Presence Label="Paradulichia typica Boeck, 1871" Count="2">
         <CoordinateSystem>GEOGCS[&quot;GCS_WGS_1984&quot;,DATUM[&quot;WGS_1984&quot;,SPHEROID[&quot;WGS_84&quot;,6378137,298.257223563]],PRIMEM[&quot;Greenwich&quot;,0],UNIT[&quot;Degree&quot;,0.017453292519943295]]</CoordinateSystem>
         <Point Id="60" X="-66.6805999999999983174348" Y="44.80389999999999872670742" Sample="3.3996899127960205078125 3.9258100986480712890625 3.3941667079925537109375 2.651609897613525390625 2.9317200183868408203125 3.9258100986480712890625 3.3941667079925537109375 4.403230190277099609375 4.00634670257568359375 5.775000095367431640625 -5.099999904632568359375 -3.8333332538604736328125 15.5 14.3999996185302734375 0.4664461314678192138671875 7.469711780548095703125"/>
         <Point Id="90" X="-70.67583000000000481577445" Y="42.56400000000000005684342" Sample="3.0158140659332275390625 3.0741899013519287109375 3.1349799633026123046875 2.3677399158477783203125 2.5277767181396484375 3.0741899013519287109375 3.1349799633026123046875 3.946670055389404296875 3.5058767795562744140625 9.69999980926513671875 -3.099999904632568359375 -1.7999999523162841796875 22.200000762939453125 20.799999237060546875 0.437892496585845947265625 9.128078460693359375"/>
      </Presence>
   </Sampler>
   <Algorithm Id="GARP_BS" Version="3.0.4">
      <Parameters>
         <Parameter Id="TrainingProportion" Value="50"/>
      </Parameters>
      <Normalization Class="ScaleNormalizer" UseLayerAsRef="1" Min="-1" Max="1" Offsets="-1 -1 -1 -1 -1 -1 -1 -1 -1 0.005958831833507538533467596 0.2899999926090239821263594 0.2634730439267116697266147 -0.4646271432417384827928686 -0.3975903728119728475220995 -1 -1" Scales="0.07590884938198676212817162 0.03428444156311074436649378 0.08045872498009011553055103 0.1102614821623311980314242 0.1072761844897962557610072 0.06812429752925201753566142 0.08045872498009011553055103 0.02387965507934120684696566 0.02695162394062220259649543 0.03250270782022324222992893 0.02500000059604646357458435 0.0256629594597213989137785 0.03824091833987582067511113 0.03804692458628328860514856 0.0636147047123583053718221 0.08380190936223656794634707"/>
      <Model>
         <BestSubsets Count="1">
            <Run Id="9" OmissionError="100" CommissionError="0">
               <Algorithm Id="GARP" Version="3.3">
                  <Parameters>
                     <Parameter Id="Resamples" Value="2500"/>
                  </Parameters>
                  <Normalization Class="ScaleNormalizer" UseLayerAsRef="1" Min="-1" Max="1" Offsets="" Scales=""/>
                  <Model>
                     <Garp Generations="10" AccuracyLimit="0" Mortality="0.9000000000000000222044605" Significance="2.700000000000000177635684" FinalCrossoverRate="0.1000000000000000055511151" FinalMutationRate="0.5999999999999999777955395" FinalGapSize="0.1000000000000000055511151">
                        <FittestRules Count="50">
                           <Rule Type="!" Prediction="0" Chromosome1="-0.9843137254901961341602146 -0.9921568627450980670801073 -1 -0.7249636963009833401372362 -1 -1 -0.9921568627450980670801073 -0.9764705882352940902180194 -1 -0.4509803921568626972060656 -0.6627450980392156631992862 -0.678431372549019640061374 -0.199999999999999955591079 -1 -0.9921568627450980670801073 -1" Chromosome2="-0.5921568627450980448756468 -0.8588235294117647633527213 1 0.6128777906298636946047509 1 1 -0.3725490196078431459625335 -0.7803921568627451010868867 1 0.8588235294117647633527213 0.9137254901960785158365752 0.9215686274509804487564679 0.7176470588235293046608376 1 -0.7098039215686273717409449 1" Performance="35.12794867655351538360264 0.4928000000000000158095759 0.4995999999999999885424984 0.5003999999999999559463504 0.4928000000000000158095759 1 0 0.4928000000000000158095759 35.12794867655351538360264 0"/>
                           <Rule Type="!" Prediction="0" Chromosome1="-0.9764705882352940902180194 -0.9921568627450980670801073 -0.9921568627450980670801073 -1 -0.9921568627450980670801073 -0.8189271899853267644431298 -1 -0.9921568627450980670801073 -0.9843137254901961341602146 -0.4823529411764705399079389 -1 -0.7019607843137254388210522 -1 -0.2549019607843137080749329 -1 -0.4566382590170000033147346" Chromosome2="-0.6313725490196078204974128 -0.8274509803921568096285455 -0.5529411764705882692538808 1 -0.5607843137254902021737735 -0.3948564384936118498714563 1 -0.6078431372549020217377347 -0.7568627450980391913049061 0.8588235294117647633527213 1 0.9215686274509804487564679 1 0.7882352941176471450290819 1 0.3304944912373435350616546" Performance="35.0423054398027531419757 0.4904000000000000025757174 0.4995999999999999885424984 0.5003999999999999559463504 0.4904000000000000025757174 1 0 0.4904000000000000025757174 35.0423054398027531419757 0"/>
                           <Rule Type="!" Prediction="0" Chromosome1="-0.9921568627450980670801073 -0.9921568627450980670801073 -1 -0.7664393396634194832728326 -0.9921568627450980670801073 -0.9921568627450980670801073 -0.9921568627450980670801073 -1 -0.9764705882352940902180194 -0.521568627450980315529705 -0.6078431372549020217377347 -0.6627450980392156631992862 -1 -0.2784313725490196178569136 -1 -0.9450980392156862475161461" Chromosome2="-0.3490196078431372361805529 -0.7176470588235294156831401 1 0.6688390078230036506212741 -0.6705882352941177071414813 -0.3960784313725490557445141 -0.3960784313725490557445141 1 -0.7803921568627451010868867 0.8823529411764705621123994 0.8901960784313724950322921 0.9137254901960785158365752 1 0.7882352941176471450290819 1 0.5137254901960783826098123" Performance="34.94212302607350295602373 0.4875999999999999778843573 0.4995999999999999885424984 0.5003999999999999559463504 0.4875999999999999778843573 1 0 0.4875999999999999778843573 34.94212302607350295602373 0"/>
                        </FittestRules>
                     </Garp>
                  </Model>
               </Algorithm>
            </Run>
         </BestSubsets>
      </Model>
   </Algorithm>
   <Statistics>
      <ConfusionMatrix Threshold="0.5" Accuracy="0" OmissionError="100" CommissionError="-100" TruePositives="0" FalsePositives="2" TrueNegatives="0" FalseNegatives="0"/>
      <RocCurve Auc="0.5" NumBackgroundPoints="10000" Points="0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 1"/>
   </Statistics>
</SerializedModel>"""
   else:
      postContentType = "text/plain"
      # Note: This is junk output that was compacted to pass a sanity check
      postContent = """\
layer0, 0.0, 5.465950234793127E-4, 12.605795860290527
layer8, 0.0, 0.0011111099738627672, 36.11686706542969
layer9, -16.00108020806858, -30.883333206176758, 30.383333206176758
linearPredictorNormalizer, 3.8757348894277786
densityNormalizer, 341.3044595280463
numBackgroundPoints, 10003
entropy, 8.350027226502103
"""
   _testJob(mdlJobType, mdlJobId, userId, postContent, postComponent, postContentType)

# .............................................................................
def testOccurrenceSetJob(occJobId, userId):
   """
   @summary: Pseudo-tests for occurrence set jobs
   """
   content = open(os.path.join(TEST_DATA_PATH, 'testPoints.zip'), 'rb').read()
   _testJob(ProcessType.GBIF_TAXA_OCCURRENCE, occJobId, userId, content, "package", "application/zip")
   
# .............................................................................
def testProjectionJob(prjJobType, prjJobId, userId):
   """
   @summary: Pseudo-tests for projection jobs
   """
   postContent = open(os.path.join(TEST_DATA_PATH, 'testPrj.tif'), 'rb').read()
   postComponent = "projection"
   postContentType = "image/tiff"
   _testJob(prjJobType, prjJobId, userId, postContent, postComponent, postContentType)


# .............................................................................
if __name__ == "__main__":
   # ..........................................................................
   log = ThreadLogger('jobServerTester')
   userId = 'unitTest'
   scribe = Scribe(log)
   scribe.openConnections()
   
   try:
         occ, ojId, mjId, pjId, mType, pType = insertTestJobChain(scribe, userId)
   except Exception, e:
      print "Jobchain Test failed: %s" % str(e)
      print traceback.format_exc()
   
   try:
      testOccurrenceSetJob(ojId, userId)
   except Exception, e:
      print "Occurrence Test failed: %s" % str(e)
      print traceback.format_exc()

   try:
      testModelJob(mType, mjId, userId)
   except Exception, e:
      print "Model Test failed: %s" % str(e)
      print traceback.format_exc()

   try:
      testProjectionJob(pType, pjId, userId)
   except Exception, e:
      print "Projection Test failed: %s" % str(e)
      print traceback.format_exc()
   
   # Do not need to clean up.  The job server does it on POST now.
   #cleanUp(scribe, occ, ojId, mjId, pjId)
   
   
   scribe.closeConnections()
   
   
