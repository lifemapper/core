"""
@summary: Module containing a formatter to convert a PROV object to RDF
@author: CJ Grady
@status: alpha
@version: 0.1
"""
import mx.DateTime

from rdflib import Literal, Namespace, Graph, RDF, RDFS, XSD, BNode
#import lmProv as prov
from LmCommon.common.lmconstants import LM_NAMESPACE, LM_PROC_NAMESPACE

import LmWebServer.lmProv.lmProv as prov
# .............................................................................
LM = Namespace(LM_NAMESPACE)
LM_PROC = Namespace(LM_PROC_NAMESPACE)
PROV = Namespace("http://www.w3.org/ns/prov#")
XSI = Namespace("http://www.w3.org/2001/XMLSchema-instance")
#XSD = Namespace("http://www.w3.org/2001/XMLScehma")

NAMESPACES = [
              (XSI, "xsi"),
              (PROV, "prov"),
              (LM_PROC, "lmProc"),
              (LM, "lm")
             ]

# .............................................................................
class ProvRDF(object):
   """
   @summary: PROV XML formatter for a PROV object
   """
   def __init__(self, provObj):
      self.provObj = provObj
      self._addedLM = False
   
   def format(self, parent=None, rdfFormat='xml'):
      if isinstance(self.provObj, prov.Document):
         graph = Graph()
         for ns, pf in NAMESPACES:
            graph.bind(pf, ns)
         ret = self._addDocument(graph, self.provObj)
      elif isinstance(self.provObj, prov.BundleConstructor):
         ret = self._addBundledContent(parent, self.provObj)
      else:
         raise Exception, "Root must be Document or Bundle Constructor"
      return graph.serialize(format=rdfFormat)
   
   def formatString(self, rdfFormat='xml'):
      return unicode(self.format(rdfFormat=rdfFormat))
   
   # ..........................................................................
   # Entities and Activities
   # ..........................................................................
   # ..........................................................................
   # Entity
   def _addEntity(self, graph, entity, tag="entity"):
      """
      @summary: Adds a PROV-XML Entity element
      @param parent: The parent element to add this element to
      @param entity: The PROV entity object
      @param tag: The name of the entity XML element, should not be changed by 
                     user
      """
      el = self._addProvElement(graph, tag, entity)
      if entity.value is not None:
         self._addValue(el, entity.value)
      return el

   # ..........................................................................
   # Activity
   def _addActivity(self, graph, activity):
      """
      @summary: Adds a PROV-XML activity element
      @param parent: The parent element to add this element to
      @param activity: A PROV Activity object
      """
      el = self._addProvElement(graph, "activity", activity)
      self._addProvTime(el, activity.startTime, "startTime")
      self._addProvTime(el, activity.endTime, "endTime")
      return el
   
   # ..........................................................................
   # Generation
   def _addGeneration(self, graph, generation):
      """
      @summary: Adds a PROV-XML wasGeneratedBy element
      @param parent: The parent element to add this element to
      @param generation: A PROV Generation object
      @todo: Add time
      """
      graph.add((LM[generation.entityRef], PROV["wasGeneratedBy"], LM[generation.activityRef]))
#      el = self._addProvElement(graph, "wasGeneratedBy", generation)
#      self._addProvReference(graph, "entity", generation.entityRef)
#      self._addProvReference(graph, "activity", generation.activityRef)
#      self._addProvTime(el, generation.time, "time")
#      return el
   
   # ..........................................................................
   # Usage
   def _addUsage(self, graph, usage):
      """
      @summary: Adds a PROV-XML used element
      @param parent: The parent element to add this element to
      @param usage: A PROV Usage object
      @todo: Add time
      """
      #usageB = BNode()
      #graph.add((LM[usage.activityRef], PROV['qualifiedUsage'], usageB))
      #graph.add((usageB, RDF.type, PROV['Usage']))
      #graph.add((usageB, PROV['entity'], LM[usage.entityRef]))

      graph.add((LM[usage.activityRef], PROV["used"], LM[usage.entityRef]))
#      el = self._addProvElement(graph, "used", usage)
#      self._addProvReference(el, "activity", usage.activityRef)
#      self._addProvReference(el, "entity", usage.entityRef)
#      self._addProvTime(el, usage.time, "time")
#      return el
   
   # ..........................................................................
   # Communication
   def _addCommunication(self, graph, communication):
      """
      @summary: Adds a PROV-XMl wasInformedBy element
      @param parent: The parent element to add this element to
      @param communication: A PROV Communication object
      """
      graph.add((LM[communication.informantActivityRef], PROV['wasInformedBy'], 
                 LM[communication.informedActivityRef]))
#      el = self._addProvElement(parent, "wasInformedBy", communication)
#      self._addProvReference(el, "informed", communication.informedActivityRef)
#      self._addProvReference(el, "informant", 
#                                            communication.informantActivityRef)
#      return el
   
   # ..........................................................................
   # Start
   def _addStart(self, graph, start):
      """
      @summary: Adds a PROV-XML wasStartedBy element
      @param parent: The parent element to add this element to
      @param start: A PROV Start object
      @todo: Add other properties
      """
      graph.add((LM[start.activityRef], PROV['wasStartedBy'], LM[start.starterRef]))
      qStart = BNode()
      graph.add((LM[start.activityRef], PROV['qualifiedStart'], qStart))
      graph.add((qStart, RDF.type, PROV['Start']))
      graph.add((qStart, PROV['entity'], LM[start.triggerRef]))
      self._addProvTime(qStart, start.time, 'atTime')
#      el = self._addProvElement(parent, "wasStartedBy", start)
#      self._addProvReference(el, "activity", start.activityRef)
#      self._addProvReference(el, "trigger", start.triggerRef)
#      self._addProvReference(el, "starter", start.starterRef)
#      self._addProvTime(el, start.time, "time")
#      return el
   
   # ..........................................................................
   # End
   def _addEnd(self, graph, end):
      """
      @summary: Adds a PROV-XML wasEndedBy element
      @param parent: The parent element to add this element to
      @param end: A PROV End object
      """
      graph.add((LM[end.activityRef], PROV['wasEndedBy'], LM[end.enderRef]))
#      el = self._addProvElement(parent, "wasEndedBy", end)
#      self._addProvReference(el, "activity", end.activityRef)
#      self._addProvReference(el, "trigger", end.triggerRef)
#      self._addProvReference(el, "ender", end.enderRef)
#      self._addProvTime(el, end.time, "time")
#      return el
   
   # ..........................................................................
   # Invalidation
   def _addInvalidation(self, graph, invalidation):
      """
      @summary: Adds a PROV-XML wasInvalidatedBy element
      @param parent: The parent element to add this element to
      @param invalidation: A PROV Invalidation object
      @todo: Add time
      """
      graph.add((LM[invalidation.entityRef], PROV['wasInvalidatedBy'], 
                 LM[invalidation.activityRef]))
#      el = self._addProvElement(parent, "wasInvalidatedBy", invalidation)
#      self._addProvReference(el, "entity", invalidation.entityRef)
#      self._addProvReference(el, "activity", invalidation.activityRef)
#      self._addProvTime(el, invalidation.time, "time")
#      return el
   
   # ..........................................................................
   
   # ..........................................................................
   # Derivations
   # ..........................................................................
   # ..........................................................................
   # Derivation
   def _addDerivation(self, graph, derivation, tag="wasDerivedFrom"):
      """
      @summary: Adds a PROV-XML derivation element
      @param parent: The parent element to add this element to
      @param derivation: A PROV Derivation object
      @param tag: The XML tag for this element, should not be changed by user
      @todo: Add fully qualified derivation
      """
      graph.add((LM[derivation.generatedEntityRef], PROV[tag], 
                 LM[derivation.activityRef]))
#      el = self._addProvElement(graph, tag, derivation)
#      self._addProvReference(el, "generatedEntity", 
#                                                 derivation.generatedEntityRef)
#      self._addProvReference(el, "usedEntity", derivation.usedEntityRef)
#      self._addProvReference(el, "activity", derivation.activityRef)
#      self._addProvReference(el, "generation", derivation.generationRef)
#      self._addProvReference(el, "used", derivation.usageRef)
#      return el
   
   # ..........................................................................
   # Revision
   def _addRevision(self, graph, derivation):
      """
      @summary: Adds a PROV-XML wasRevisionOf element
      @param parent: The parent element to add this element to
      @param derivation: A PROV Revision object
      @see: _addDerivation 
      """
      return self._addDerivation(graph, derivation, tag="wasRevisionOf")
   
   # ..........................................................................
   # Quotation
   def _addQuotation(self, graph, derivation):
      """
      @summary: Adds a PROV-XML wasQuotedFrom element
      @param parent: The parent element to add this element to
      @param derivation: A PROV Quotation object
      @see: _addDerivation
      """
      return self._addDerivation(graph, derivation, tag="wasQuotedFrom")
   
   # ..........................................................................
   # Primary Source
   def _addPrimarySource(self, graph, derivation):
      """
      @summary: Adds a PROV-XML hadPrimarySource element
      @param parent: The parent element to add this element to
      @param derivation: A PROV PrimarySource object
      @see: _addDerivation
      """
      return self._addDerivation(graph, derivation, tag="hadPrimarySource")
   
   # ..........................................................................
   
   # ..........................................................................
   # Agents, Responsibility, and Influence
   # ..........................................................................
   # ..........................................................................
   # Agent
   def _addAgent(self, graph, agent, tag="agent"):
      """
      @summary: Adds a PROV-XML agent element
      @param parent: The parent element to add this element to
      @param agent: A PROV Agent object
      @param tag: The XML tag for the element, should not be changed by the user
      """
      return self._addProvElement(graph, tag, agent)
   
   # ..........................................................................
   # Person
   def _addPerson(self, graph, agent):
      """
      @summary: Adds a PROV-XML person element
      @param parent: The parent element to add this element to
      @param agent: A PROV Person object
      @see: _addAgent
      """
      return self._addAgent(graph, agent, tag="person")
   
   # ..........................................................................
   # Organization
   def _addOrganization(self, graph, agent):
      """
      @summary: Adds a PROV-XML organization element
      @param parent: The parent element to add this element to
      @param agent: A PROV Organization element
      @see: _addAgent
      """
      return self._addAgent(graph, agent, tag="organization")
   
   # ..........................................................................
   # Software Agent
   def _addSoftwareAgent(self, graph, agent):
      """
      @summary: Adds a PROV-XML softwareAgent element
      @param parent: The parent element to add this element to
      @param agent: A PROV SoftwareAgent element
      @see: _addAgent
      """
      return self._addAgent(graph, agent, tag="softwareAgent")
   
   # ..........................................................................
   # Attribution
   def _addAttribution(self, graph, attribution):
      """
      @summary: Adds a PROV-XML wasAttributedTo element
      @param parent: The parent element to add this element to
      @param attribution: A PROV Attribution element
      """
      graph.add((LM[attribution.entityRef], PROV['wasAttributedTo'],
                 LM[attribution.agentRef]))
#      el = self._addProvElement(parent, "wasAttributedTo", attribution)
#      self._addProvReference(el, "entity", attribution.entityRef)
#      self._addProvReference(el, "agent", attribution.agentRef)
#      return el
   
   # ..........................................................................
   # Association
   def _addAssociation(self, graph, association):
      """
      @summary: Adds a PROV-XML wasAssociatedWith element
      @param parent: The parent element to add this element to
      @param association: A PROV Association object
      @todo: Add plan
      """
      graph.add((LM[association.activityRef], PROV['wasAssociatedWith'],
                 LM[association.agentRef]))
#      el = self._addProvElement(parent, "wasAssociatedWith", association)
#      self._addProvReference(el, "activity", association.activityRef)
#      self._addProvReference(el, "agent", association.agentRef)
#      self._addProvReference(el, "plan", association.planRef)
#      return el
   
   # ..........................................................................
   # Plan
   def _addPlan(self, graph, plan):
      """
      @summary: Adds a PROV-XML plan element
      @param parent: The parent element to add this element to
      @param plan: A PROV Plan object
      @see: _addEntity
      """
      return self._addEntity(graph, plan, tag="plan")
   
   # ..........................................................................
   # Delegation
   def _addDelegation(self, graph, delegation):
      """
      @summary: Adds a PROV-XML actedOnBehalfOf element
      @param parent: The parent element to add this element to
      @param delegation: A PROV Delegation object
      @todo: Add activity
      """
      graph.add((LM[delegation.delegateRef], PROV['actedOnBehalfOf'],
                 LM[delegation.responsibleRef]))
#      el = self._addProvElement(parent, "actedOnBehalfOf", delegation)
#      self._addProvReference(el, "delegate", delegation.delegateRef)
#      self._addProvReference(el, "responsible", delegation.responsibleRef)
#      self._addProvReference(el, "activity", delegation.activityRef)
#      return el
   
   # ..........................................................................
   # Influence
   def _addInfluence(self, graph, influence):
      """
      @summary: Adds a PROV-XML wasInfluencedBy element
      @param parent: The parent element to add this element to
      @param influence: A PROV Influence object
      """
      graph.add((LM[influence.influenceeRef], PROV['wasInfluencedBy'],
                 LM[influence.influencerRef]))
#      el = self._addProvElement(parent, "wasInfluencedBy", influence)
#      self._addProvReference(el, "influencee", influence.influenceeRef)
#      self._addProvReference(el, "influencer", influence.influencerRef)
#      self._addProvAttributes(el, influence)
#      return el
   
   # ..........................................................................
   
   # ..........................................................................
   # Bundles
   # ..........................................................................
   # ..........................................................................
   # Bundle
   def _addBundle(self, graph, bundle):
      """
      @summary: Adds a PROV-XML bundle element
      @param parent: The parent element to add this element to
      @param bundle: A PROV Bundle object
      @see: _addEntity
      """
      return self._addEntity(graph, bundle, tag="bundle")
   
   # ..........................................................................
   # Bundle Constructor
   # ..........................................................................
   def _addBundledContent(self, graph, bundledContent):
      """
      @summary: Adds a PROV-XML bundledContent element
      @param parent: The parent element to add this element to
      @param bundledContent: A PROV BundleConstructor object
      @see: _processBundle
      """
      el = self._addProvElement(graph, "bundledContent", bundledContent, 
                                                           addAttributes=False)
      self._processBundle(graph, bundledContent)
      return el
      
   # ..........................................................................
   # Alternate Entities
   # ..........................................................................
   # ..........................................................................
   # Specialization
   def _addSpecialization(self, graph, specialization):
      """
      @summary: Adds a PROV-XML specializationOf element
      @param parent: The parent element to add this element to
      @param specialization: A PROV Specialization object
      """
      graph((LM[specialization.specificEntityRef], PROV['specializationOf'],
             LM[specialization.generalEntityRef]))
#      el = self._addProvElement(parent, "specializationOf", specialization, 
#                                                           addAttributes=False)
#      self._addProvReference(el, "specificEntity", 
#                                              specialization.specificEntityRef)
#      self._addProvReference(el, "generalEntity", 
#                                               specialization.generalEntityRef)
#      return el
   
   # ..........................................................................
   # Alternate
   def _addAlternate(self, graph, alternate):
      """
      @summary: Adds a PROV-XML alternateOf element
      @param parent: The parent element to add this element to
      @param alternate: A PROV Alternate object
      """
      graph.add((LM[alternate.alternate1Ref], PROV['alternateOf'],
                 LM[alternate.alternate2Ref]))
#      el = self._addProvElement(parent, "alternateOf", alternate, 
#                                                           addAttributes=False)
#      self._addProvReference(el, "alternate1", alternate.alternate1Ref)
#      self._addProvReference(el, "alternate2", alternate.alternate2Ref)
#      return el
   
   # ..........................................................................
   # Collections
   # ..........................................................................
   # ..........................................................................
   # Collection
   def _addCollection(self, graph, collection):
      """
      @summary: Adds a PROV-XML collection element
      @param parent: The parent element to add this element to
      @param collection: A PROV Collection object
      @see: _addEntity
      """
      return self._addEntity(graph, collection, tag="collection")
    
   # ..........................................................................
   # Empty Collection
   def _addEmptyCollection(self, graph, collection):
      """
      @summary: Adds a PROV-XML emptyCollection element
      @param parent: The parent element to add this element to
      @param collection: A PROV EmptyCollection object
      @see: _addEntity
      """
      return self._addEntity(graph, collection, tag="emptyCollection")
   
   # ..........................................................................
   # Membership
   def _addMembership(self, graph, membership):
      """
      @summary: Adds a PROV-XML hadMember element
      @param parent: The parent element to add this element to
      @param membership: A PROV Membership object
      """
#      el = self._addProvElement(parent, "hadMember", membership, 
#                                                           addAttributes=False)
#      self._addProvReference(el, "collection", membership.collectionRef)
      for entityRef in membership.entityRefs:
         graph.add((LM[membership.collectionRef], PROV['hadMember'],
                    LM[entityRef]))
#         self._addProvReference(el, "entity", entityRef)
#      return el
   
   # ..........................................................................
   
   # ..........................................................................
   # Attributes
   # ..........................................................................
   def _addProvAttributes(self, parent, provObject):
      """
      @summary: Adds PROV-XML attributes to an element
      @param parent: The parent element to add this element to
      @param provObject: A PROV object that may have attributes
      """
      for label in provObject.label:
         self._addLabel(parent, label)
      for location in provObject.location:
         self._addLocation(parent, location)
      for provType in provObject.provType:
         self._addProvType(parent, provType)
      for role in provObject.role:
         self._addRole(parent, role)
      for other in provObject.others:
         self._addOther(parent, other)
   
   # ..........................................................................
   # Label
   def _addLabel(self, parent, label):
      """
      @summary: Adds a PROV label element
      @param parent: The parent element to add this element to
      @param label: A PROV Label object or label string
      """
      attrib = {}
      try:
         if label.lang is not None:
            attrib["xml:lang"] =label.lang
         val = label.label
      except:
         val = label
      parent.add(RDFS.label, Literal(val))
#      return SubElement(parent, "label", attrib=attrib, value=val)
      
   # ..........................................................................
   # Location
   def _addLocation(self, parent, location):
      """
      @summary: Adds a PROV location element
      @param parent: The parent element to add this element to
      @param location: A PROV Location object or location string
      """
      attrib = {}
      try:
         attrib["{%s}type" % XSI] = location.locType
         val = location.location
      except:
         attrib["{%s}type" % XSI] = "xsd:string"
         val = location
      parent.add(PROV['atLocation'], Literal(val))
#      return SubElement(parent, "location", attrib=attrib, value=val)
      
   # ..........................................................................
   # Role
   def _addRole(self, parent, role):
      """
      @summary: Adds a PROV role element
      @param parent: The parent element to add this element to
      @param role: A PROV Role object or role string
      @todo: Handle roles
      """
      pass
#      try:
#         attrib = {"{%s}type" % XSI_NS: role.roleType}
#         val = role.role
#      except:
#         attrib = {"{%s}type" % XSI_NS: "xsd:QName"}
#         val = role
#      return SubElement(parent, "role", attrib=attrib, value=val)
      
   # ..........................................................................
   # Type
   def _addProvType(self, parent, provType):
      """
      @summary: Adds a PROV type element
      @param parent: The parent element to add this element to
      @param provType: A PROV ProvType object or prov type string
      """
      try:
         attrib = {"{%s}type" % XSI: provType.provTypeType}
         val = provType.provType
      except:
         attrib = {"{%s}type" % XSI: "xsd:QName"}
         val = provType
      parent.add(RDF.type, PROV[provType])
#      return SubElement(parent, "type", attrib=attrib, value=val)
      
   # ..........................................................................
   # Value
   def _addValue(self, parent, value):
      """
      @summary: Adds a PROV value element
      @param parent: The parent element to add this element to
      @param value: A PROV Value object or value string
      """
      try:
         attrib = {"{%s}type" % XSI: value.valueType}
         val = value.value
      except:
         attrib = {"{%s}type" % XSI: "xsd:string"}
         val = value
      return parent.add(PROV['value'], val)
      
   # ..........................................................................
   
   # ..........................................................................
   # Document
   # ..........................................................................
   def _addDocument(self, graph, provDocument, attrib={}):
      """
      @summary: Adds a PROV document element
      @param provDocument: A PROV Document element
      @param attrib: Any additional attributes to add to the element
      @see: _processBundle 
      """
      doc = graph.resource('Document')
      return self._processBundle(graph, provDocument)
      #return doc
   
   # ..........................................................................
   # Other
   # ..........................................................................
   def _addOther(self, parent, other):
      """
      @summary: Adds other elements to the parent
      @param parent: The parent element to add this element to
      @param other: An object to serialize and add as an element to the parent
      @todo: Handle others
      """
      pass
      #self._addedLM = True
      #serialize(other.obj, parent=parent, namespace=other.namespace)
   
   # ..........................................................................
   def _processBundle(self, graph, bundledContent):
      """
      @summary: Processes PROV bundled content and creates elements
      @param parent: The parent element to add this element to
      @param bundledContent: A PROV BundledContent object
      """
      for key in bundledContent.entities.keys():
         self._addEntity(graph, bundledContent.entities[key])
      for key in bundledContent.collections.keys():
         collection = bundledContent.collections[key]
         if isinstance(collection, prov.EmptyCollection):
            self._addEmptyCollection(graph, collection)
         else:
            self._addCollection(graph, collection)
      for key in bundledContent.memberships.keys():
         self._addMembership(graph, bundledContent.memberships[key])
      for key in bundledContent.activities.keys():
         self._addActivity(graph, bundledContent.activities[key])
      for key in bundledContent.generations.keys():
         self._addGeneration(graph, bundledContent.generations[key])
      for key in bundledContent.usages.keys():
         self._addUsage(graph, bundledContent.usages[key])
      for key in bundledContent.communications.keys():
         self._addCommunication(graph, bundledContent.communications[key])
      for key in bundledContent.starts.keys():
         self._addStart(graph, bundledContent.starts[key])
      for key in bundledContent.ends.keys():
         self._addEnd(graph, bundledContent.ends[key])
      for key in bundledContent.invalidations.keys():
         self._addInvalidation(graph, bundledContent.invalidations[key])
      for key in bundledContent.derivations.keys():
         deriv = bundledContent.derivations[key]
         if isinstance(deriv, prov.PrimarySource):
            self._addPrimarySource(graph, deriv)
         elif isinstance(deriv, prov.Quotation):
            self._addQuotation(graph, deriv)
         elif isinstance(deriv, prov.Revision):
            self._addRevision(graph, deriv)
         else:
            self._addDerivation(graph, deriv)
      for key in bundledContent.agents.keys():
         agent = bundledContent.agents[key]
         if isinstance(agent, prov.Organization):
            self._addOrganization(graph, agent)
         elif isinstance(agent, prov.Person):
            self._addPerson(graph, agent)
         elif isinstance(agent, prov.SoftwareAgent):
            self._addSoftwareAgent(graph, agent)
         else:
            self._addAgent(graph, agent)
      for key in bundledContent.attributions.keys():
         self._addAttribution(graph, bundledContent.attributions[key])
      for key in bundledContent.associations.keys():
         self._addAssociation(graph, bundledContent.associations[key])
      for key in bundledContent.delegations.keys():
         self._addDelegation(graph, bundledContent.delegations[key])
      for key in bundledContent.influences.keys():
         self._addInfluence(graph, bundledContent.influences[key])
      try: # Only for documents
         for bundle in bundledContent.bundles:
            self._addBundle(graph, bundle)
      except:
         pass
      for key in bundledContent.alternates.keys():
         self._addAlternate(graph, bundledContent.alternates[key])
      for key in bundledContent.specializations.keys():
         self._addSpecialization(graph, bundledContent.specializations[key])
   
   # ..........................................................................
   # Helpers
   # ..........................................................................
#   def _addProvReference(self, parent, tag, identifier):
#      """
#      @summary: Adds a PROV reference element to an element
#      @param parent: The element to add the reference to
#      @param name: The XML tag for the new element
#      @param identifier: The value of the identifier reference
#      """
#      if identifier is not None:
#         return SubElement(parent, tag, attrib={"{%s}ref" % PROV_NS: identifier})
#      else:
#         return None
#   
   # ..........................................................................
   def _addProvTime(self, graph, timeObj, tag):
      """
      @summary: Adds a PROV time element to an element
      @param parent: The element to add the time element to
      @param timeObj: The time object to add
      @param tag: The XML tag for the new element
      """
      if timeObj is not None:
         formattedTime = self._formatTime(timeObj)
         return graph.add(timeObj, PROV[tag], Literal(formattedTime, datatype=XSD.datetime))
      else:
         return None
   
   def _addProvElement(self, graph, tag, provObj, addAttributes=True):
      """
      @summary: Adds a PROV element to an element
      @param parent: The element to add the PROV element to
      @param tag: The XML tag for the new element
      @param provObj: A PROV object
      @param addAttributes: (optional) Boolean indicating if the function 
                               should attempt to add PROV attributes to the new 
                               element
      """
      el = graph.resource(LM[provObj.identifier])
      el.add(RDF.type, PROV[tag])
      if addAttributes:
         self._addProvAttributes(el, provObj)
      return el
   
   # ..........................................................................
   def _formatTime(self, time):
      """
      @summary: Formats a time mjd into the proper format for PROV-XML
      @param time: The time mjd to format
      """
      if time is None:
         dTime = mx.DateTime.gmt()
      else:
         dTime = mx.DateTime.DateTimeFromMJD(time)
      return dTime.strftime('%Y-%m-%dT%H:%M:%S')
