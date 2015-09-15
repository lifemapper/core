"""
@summary: Module containing functions to generate PROV documents for Lifemapper 
             objects
@author: CJ Grady
@version: 0.1
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

@todo: Evaluate if lmobject would be a better base class
@todo: Additional information for occurrence sets?
@todo: Additional information for climate layers?
@todo: Make sure model scenario layer are populated
@todo: Can RAD and SDM layers be combined?
@todo: Should PA layers be handled in RAD experiment instead of their own function?
@todo: Add Ancillary layers to an experiment
@todo: RAD Bucket as base
@todo: RAD layer as base
@todo: RAD pam sum as base
"""
from LmCommon.common.lmconstants import RandomizeMethods
from LmCommon.common.localconstants import WEBSERVICES_ROOT

from LmServer.common.log import ConsoleLogger
from LmServer.db.peruser import Peruser
from LmServer.rad.radbucket import RADBucket
from LmServer.rad.radexperiment import RADExperiment
from LmServer.sdm.envlayer import EnvironmentalLayer
from LmServer.sdm.occlayer import OccurrenceLayer
from LmServer.sdm.scenario import Scenario
from LmServer.sdm.sdmexperiment import SDMExperiment
from LmServer.sdm.sdmprojection import SDMProjection

import LmWebServer.lmProv.lmProv as prov

# .............................................................................
class ProvDocumentGenerator(object):
   """
   @summary: Generates a PROV document object for a Lifemapper object
   """
   # ......................................
   def __init__(self, lmObj, log=ConsoleLogger()):
      """
      @param lmObj: The Lifemapper object to generate the PROV document for
      """
      self.doc = prov.Document()
      self.lmObj = lmObj
      self.log = log
      self.peruser = Peruser(log)
      
   # ......................................
   def generate(self):
      """
      @summary: Generates the PROV document
      """
      self.peruser.openConnections()
      if isinstance(self.lmObj, EnvironmentalLayer):
         self._addSDMClimateLayer(self.lmObj)
      elif isinstance(self.lmObj, OccurrenceLayer):
         self._addSDMOccurrenceSet(self.lmObj)
      elif isinstance(self.lmObj, Scenario):
         self._addSDMScenario(self.lmObj)
      elif isinstance(self.lmObj, SDMExperiment):
         self._addSDMExperiment(self.lmObj)
      elif isinstance(self.lmObj, SDMProjection):
         self._addSDMProjection(self.lmObj)
      elif isinstance(self.lmObj, RADBucket):
         self._addRADBucket(self.lmObj, self._getPALayersForBucket(self.lmObj))
      elif isinstance(self.lmObj, RADExperiment):
         self._addRADExperiment(self.lmObj)
      self.peruser.closeConnections()
      return self.doc
   
   # ......................................
   def _addSDMAlgorithm(self, alg):
      algEnt = prov.Entity("algorithm", others=[alg])
      self.doc.addEntity(algEnt)
      return algEnt
   
   # ......................................
   def _addSDMExperiment(self, exp):
      # Create a collection for the experiment
      expCol = prov.Collection(_makeIdentifier('sdm', 'experiments', exp.id))
      self.doc.addCollection(expCol)
      
      # Create a membership for the experiment collection
      expColMemb = prov.Membership(expCol)
      
      # Model
      mdl, mdlAgent, mdlActivity = self._addSDMModel(exp.model)
      expColMemb.addEntity(mdl)
      
      # Projections
      for prj in exp.projections:
         prjEntity = self._addSDMProjection(prj)
         expColMemb.addEntity(prjEntity)
         
      self.doc.addMembership(expColMemb)
      
      expPostContent = """\
                  <![CDATA[
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
                           <lm:occurrenceSetId>{occSetId}</lm:occurrenceSetId>
                           <lm:modelScenario>{mdlScnId}</lm:modelScenario>
{prjScns}
                           <lm:modelMask>{mdlMask}</lm:modelMask>
                           <lm:projectionMask>{prjMask}</lm:projectionMask>
                        </lm:experiment>
                     </lm:request>
                  ]]>
""".format(website=WEBSERVICES_ROOT,
           algoCode=exp.model.algorithmCode,
           algoParams='\n'.join(["                     <lm:{0}>{1}</lm:{0}".format(k, exp.model._algorithm.parameters[k]) for k in exp.model._algorithm.parameters.keys()]),
           occSetId=exp.model.occurrenceSet.id,
           mdlScnId=exp.model._scenario.id,
           prjScns='\n'.join(["               <lm:projectionScenario>{0}</lm:projectionScenario>".format(prj.id) for prj in exp.projections]),
           mdlMask=exp.model._mask.id,
           prjMask=exp.projections[0]._mask.id)
      
      expProcObject = {
         "HTTPmessage" : {
            "Request" : {
               "Method" : "POST",
               "Headers" : [
                  {
                   "name" : "Content-Type",
                   "value" : "application/xml"
                  }
               ],
               "MessageBody" : {
                  "LiteralContent" : expPostContent
               },
               "URI" : "%s/services/sdm/experiments" % WEBSERVICES_ROOT
            },
            "Representation" : {
                "HTTP-Version" : "1.1",
                "Status-Code" : "202",
                "MessageBody" : {
                   "OnlineResource" : "%s/xml" % exp.metadataUrl
                }
            }
         }
      }
      
      expProc = prov.Other(expProcObject, namespace="%s/lmProcess.xsd" % WEBSERVICES_ROOT)
      
      expSubmissionActivity = prov.Activity(
                            _makeIdentifier('sdm', 'experiments', exp.id, postfix='SubmissionActivity'),
                                            label="Submitted Lifemapper SDM Experiment %s" % exp.id,
                                            others=expProc)
      self.doc.addActivity(expSubmissionActivity)
   
      return expCol
   
   # ......................................
   def _addSDMProjection(self, prj):
      prjEnt = prov.Entity(_makeIdentifier('sdm', 'projections', prj.id),
                           label=prj.title,
                           location=prov.Location(prj.metadataUrl, locType="xsd:anyURI"))
      self.doc.addEntity(prjEnt)
      
      scn = self._addSDMScenario(prj.getScenario())

      act = prov.Activity(_makeIdentifier('sdm', 'projections', prj.id, postfix='Activity'),
                          label="Created Projection %s" % prj.id)
      self.doc.addActivity(act)
      gen = prov.Generation(prjEnt, act)
      self.doc.addGeneration(gen)

      mdl, softwareAgent, mdlActivity = self._addSDMModel(prj.getModel())

      self.doc.addUsage(prov.Usage(act, entity=mdl))
      self.doc.addUsage(prov.Usage(act, entity=scn))

      self.doc.addCommunication(prov.Communication(act, mdlActivity))
      self.doc.addAssociation(prov.Association(act, agent=softwareAgent))
      
      return prjEnt
   
   # ......................................
   def _addSDMClimateLayer(self, lyr):
      lyrEnt = prov.Entity(_makeIdentifier('sdm', 'layers', lyr.id),
                           label="Lifemapper Climate Layer %s - %s" % (lyr.id, lyr.title),
                           location=prov.Location(lyr.metadataUrl, locType="xsd:anyURI"))
      self.doc.addEntity(lyrEnt)
      return lyrEnt
   
   # ......................................
   def _addSDMScenario(self, scn):
      scnId = _makeIdentifier('sdm', 'scenarios', scn.id)
      scnCol = prov.Collection(scnId,
                               label=prov.Label("Lifemapper Scenario %s" % scn.id, lang="en"),
                               location=prov.Location(scn.metadataUrl, locType="xsd:anyURI"))
      scnMembership = prov.Membership(scnCol)
      for lyr in scn.layers:
         scnMembership.addEntity(self._addSDMClimateLayer(lyr))
      self.doc.addCollection(scnCol)
      self.doc.addMembership(scnMembership)
      return scnId
   
   # ......................................
   def _addSDMOccurrenceSet(self, occ):
      occEntity = prov.Entity(_makeIdentifier('sdm', 'occurrences', occ.id),
                              label=occ.title,
                              location=prov.Location(occ.metadataUrl, locType="xsd:anyURI"))
      self.doc.addEntity(occEntity)
      return occEntity
   
   # ......................................
   def _addSDMModel(self, model):
      # Algorithm
      alg = self._addSDMAlgorithm(model.getAlgorithm())
      
      # Occurrence Set
      occ = self._addSDMOccurrenceSet(model.occurrenceSet)
      
      # Model
      mdl = prov.Entity(_makeIdentifier('sdm', 'models', model.id),
                        label=prov.Label("Lifemapper Model %s" % model.id, lang="en"),
                        location=prov.Location("%s/model" % model.metadataUrl, locType="xsd:anyURI"))
      self.doc.addEntity(mdl)
      
      # Model Scenario
      mdlScn = self._addSDMScenario(model.getScenario())

      # Model activity
      mdlActivity = prov.Activity(_makeIdentifier('sdm', 'models', model.id, postfix='Activity'),
                               label="Created Model %s" % model.id)
      self.doc.addActivity(mdlActivity)

      # Model Generation
      mdlGen = prov.Generation(mdl, mdlActivity)
      self.doc.addGeneration(mdlGen)
      
      # Model Usage
      self.doc.addUsage(prov.Usage(mdlActivity, entity=occ))
      self.doc.addUsage(prov.Usage(mdlActivity, entity=alg))
      self.doc.addUsage(prov.Usage(mdlActivity, entity=mdlScn))
      
      # Agent
      if model.algorithmCode != "ATT_MAXENT":
         agent = prov.SoftwareAgent("openModeller")
      else:
         agent = prov.SoftwareAgent("MaxEnt")
      self.doc.addAgent(agent)
      
      # Association
      self.doc.addAssociation(prov.Association(mdlActivity, agent=agent))
      
      return mdl, agent, mdlActivity

   # ......................................
   def _addRADExperiment(self, exp):
      expCol = prov.Collection(_makeIdentifier('rad', 'experiments', exp.id))
      expMembership = prov.Membership(expCol)
      
      paLayersEntity = self._addRADPALayers(exp.orgLayerset)
      expMembership.addEntity(paLayersEntity)
      # Buckets
      for bkt in exp.bucketList:
         bktEnt = self._addRADBucket(bkt, paLayersEntity)
         expMembership.addEntity(bktEnt)
      # Ancillary layers
      self.doc.addCollection(expCol)
      self.doc.addMembership(expMembership)
      return expCol
   
   # ......................................
   def _addRADBucket(self, bucket, expPALayerSet):
      bktCol = prov.Collection(_makeIdentifier('rad', 'buckets', bucket.id),
                  label="Lifemapper Bucket %s" % bucket.id,
                  location=prov.Location(bucket.metadataUrl, 
                                         locType="xsd:anyURI"))
      bktMembership = prov.Membership(bktCol)

      # Shapegrid
      sgEnt = prov.Entity(_makeIdentifier('rad', 'buckets', bucket.id, postfix='Shapegrid'),
                          label="Lifemapper Bucket %s Shapegrid (Layer %s)" % \
                             (bucket.id, bucket.shapegrid.id),
                          location=prov.Location(bucket.shapegrid.metadataUrl,
                                                 locType="xsd:anyURI"))
      bktMembership.addEntity(sgEnt)
      self.doc.addEntity(sgEnt)

      # Full PAM
      fullPamEnt = self._addRADFullPAM(bucket.id, sgEnt, expPALayerSet)
      bktMembership.addEntity(fullPamEnt)
      
      # Original PamSum
      if bucket.pamSum is not None:
         origPS = self._addRADOriginalPamSum(bucket.pamSum, bucket.id, fullPamEnt)
         bktMembership.addEntity(origPS)
         
         randPSs = self._getRandomPamSums(bucket.getUserId(), bucket.parentId, bucket.id)
         
         # Randomized PamSums
         for ps in randPSs:
            if ps.randomMethod == RandomizeMethods.SWAP:
               bktMembership.addEntity(self._addRADRandomPamSumSwap(ps, bucket.id, origPS))
            elif ps.randomMethod == RandomizeMethods.SPLOTCH:
               bktMembership.addEntity(self._addRADRandomPamSumSplotch(ps, bucket.id, fullPamEnt))
            
      self.doc.addCollection(bktCol)
      self.doc.addMembership(bktMembership)
      return bktCol
   
   # ......................................
   def _addRADOriginalPamSum(self, ps, bktId, fullPAM):
      psEnt = prov.Entity(_makeIdentifier('rad', 'buckets', bktId, postfix='OriginalPamSum'),
                          label="Lifemapper Bucket %s Original PAM Sum" % bktId,
                          location = prov.Location(ps.metadataUrl,
                                                   locType="xsd:anyURI"))
      self.doc.addEntity(psEnt)
      
      # Original PAM Sum Activity
      origPSActivity = prov.Activity(_makeIdentifier('rad', 'buckets', bktId, postfix='CompressionActivity'),
                                     label="Lifemapper Bucket %s Compress Full PAM" % bktId)
      self.doc.addActivity(origPSActivity)
      
      # Usage
      self.doc.addUsage(prov.Usage(origPSActivity, entity=fullPAM))
      
      # Generation
      self.doc.addGeneration(prov.Generation(psEnt, origPSActivity))

      return psEnt
   
   # ......................................
   def _addRADRandomPamSumSplotch(self, ps, bktId, fullPam):
      psEnt = prov.Entity(_makeIdentifier('rad', 'pamsums', ps.id),
                          label="Lifemapper Bucket %s Splotch Randomized PAM Sum %s" % (bktId, ps.id),
                          location=prov.Location(ps.metadataUrl, locType="xsd:anyURI"))
      self.doc.addEntity(psEnt)
      
      # Randomize PAM Sum Activity
      psAct = prov.Activity(_makeIdentifier('rad', 'pamsums', ps.id, postfix='SplotchActivity'),
                            label="Lifemapper Randomize Pam Sum %s via Splotch Activity" % ps.id)
      self.doc.addActivity(psAct)
      
      # Usage
      self.doc.addUsage(prov.Usage(psAct, entity=fullPam))
      
      # Generation
      self.doc.addGeneration(prov.Generation(psEnt, psAct))
      
      return psEnt
   
   # ......................................
   def _addRADRandomPamSumSwap(self, ps, bktId, origPS):
      psEnt = prov.Entity(_makeIdentifier('rad', 'pamsums', ps.id),
                          label="Lifemapper Bucket %s Swap Randomized PamSum %s" % (bktId, ps.id),
                          location=prov.Location(ps.metadataUrl, locType="xsd:anyURI"))
      self.doc.addEntity(psEnt)
      
      # Randomize PAM Sum Activity
      psAct = prov.Activity(_makeIdentifier('rad', 'pamsums', ps.id, postfix='randomizeSwapActivity'),
                            label="Lifemapper Randomize Pam Sum %s via Swap Activity" % ps.id)
      self.doc.addActivity(psAct)
      
      # Usage
      self.doc.addUsage(prov.Usage(psAct, entity=origPS))
      
      # Generation
      self.doc.addGeneration(prov.Generation(psEnt, psAct))
      
      return psEnt
   
   # ......................................
   def _addRADFullPAM(self, bktId, sgEnt, expPALayerSet):
      fullPamEnt = prov.Entity(_makeIdentifier('rad', 'buckets', bktId, postfix='FullPam'),
                               label="Lifemapper Full PAM for Bucket %s" % bktId)
      self.doc.addEntity(fullPamEnt)
      
      # Full PAM Activity
      fpActivity = prov.Activity(_makeIdentifier('rad', 'buckets', bktId, postfix='CalculateActivity'),
                                 label="Lifemapper Bucket %s Full PAM Calculate Activity" % bktId)
      self.doc.addActivity(fpActivity)
      
      # Full PAM usage
      self.doc.addUsage(prov.Usage(fpActivity, entity=sgEnt))
      self.doc.addUsage(prov.Usage(fpActivity, entity=expPALayerSet))
      return fullPamEnt
   
   # ......................................
   def _addRADLayer(self, lyr):
      lyrEnt = prov.Entity(_makeIdentifier('rad', 'layers', lyr.id),
                           label="Lifemapper Layer %s - %s" % (lyr.id, lyr.title),
                           location=prov.Location(lyr.metadataUrl, locType="xsd:anyURI"))
      self.doc.addEntity(lyrEnt)
      return lyrEnt
   
   # ......................................
   def _addRADPALayers(self, paLayers):
      # Add PA Layers collection
      col = prov.Collection(_makeIdentifier('rad', 'experiments', paLayers.id, postfix='paLayers'),
               label=prov.Label(
                  "Lifemapper Experiment %s Presence Absence Layers" % paLayers.id))
      # Add Membership
      paLyrMembership = prov.Membership(col)
      
      for idx in xrange(len(paLayers.layers)):
         lyrEnt = self._addRADLayer(paLayers.layers[idx])
         paLyrEnt = prov.Entity(_makeIdentifier('rad', 'experiments', paLayers.id, postfix='Layer%s'%idx))
         self.doc.addEntity(paLyrEnt)
         deriv = prov.Derivation(paLyrEnt, lyrEnt)
         self.doc.addDerivation(deriv)
         paLyrMembership.addEntity(paLyrEnt)
      self.doc.addCollection(col)
      self.doc.addMembership(paLyrMembership)
      return col
   
   # ......................................
   def _getPALayersForBucket(self, bkt):
      return self.peruser.listPresenceAbsenceLayers(0, 1000, bkt.getUserId(), expId=bkt.parentId, atom=False)
   
   # ......................................
   def _getRandomPamSums(self, userId, expId, bktId):
      return self.peruser.listPamSums(0, 1000, userId, experimentId=expId, bucketId=bktId, isRandomized=True, randomMethod=RandomizeMethods.SWAP, atom=False)
   
# ...........................................................................
def _makeIdentifier(serviceFamily, serviceGroup, objId, prefix='lifemapper', postfix=None, delimiter='.'):
   items = [prefix] if prefix is not None else []
   items.extend([serviceFamily, serviceGroup, str(objId)])
   if postfix is not None:
      items.append(postfix)
   return delimiter.join(items)
   
