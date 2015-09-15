"""
@summary: This module will take a PROV bundle and convert it to PROV-XML
@author: CJ Grady
@version: 0.1
@status: alpha
@see: http://www.w3.org/TR/2013/NOTE-prov-xml-20130430/

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

@todo: There is a lot of code duplication, most methods could probably just
          be parameterized versions of the same function.
@todo: Bundles need work
"""
import mx.DateTime

from LmCommon.common.lmconstants import LM_NAMESPACE, LM_PROC_NAMESPACE
from LmCommon.common.lmXml import Element, QName, register_namespace, \
                                  serialize, setDefaultNamespace, SubElement, \
                                  tostring
import LmWebServer.lmProv.lmProv as prov

# .............................................................................
LM_NS = LM_NAMESPACE
LM_PROC_NS = LM_PROC_NAMESPACE
PROV_NS = "http://www.w3.org/ns/prov#"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"
XSD_NS = "http://www.w3.org/2001/XMLScehma"

NAMESPACES = [
              (XSI_NS, "xsi"),
              (XSD_NS, "xsd"),
              (PROV_NS, "prov"),
              (LM_PROC_NS, "lmProc"),
              (LM_NS, "lm")
             ]

# .............................................................................
class ProvXml(object):
   """
   @summary: PROV XML formatter fora PROV object
   """
   def __init__(self, provObj):
      self.provObj = provObj
      self._addedLM = False
   
   def format(self, parent=None):
      if isinstance(self.provObj, prov.Document):
         for ns, pf in NAMESPACES:
            register_namespace(ns, pf)
         attrib = {"xmlns:xsd": XSD_NS}
         ret = self._addDocument(self.provObj, attrib=attrib)
         if not self._addedLM:
            ret.attrib["xmlns:lm"] = LM_NS
      elif isinstance(self.provObj, prov.BundleConstructor):
         ret = self._addBundledContent(parent, self.provObj)
      else:
         raise Exception, "Root must be Document or Bundle Constructor"
      return ret
   
   def formatString(self):
      return tostring(self.format())
   
   # ..........................................................................
   # Entities and Activities
   # ..........................................................................
   # ..........................................................................
   # Entity
   def _addEntity(self, parent, entity, tag="entity"):
      """
      @summary: Adds a PROV-XML Entity element
      @param parent: The parent element to add this element to
      @param entity: The PROV entity object
      @param tag: The name of the entity XML element, should not be changed by 
                     user
      """
      el = self._addProvElement(parent, tag, entity)
      if entity.value is not None:
         self._addValue(el, entity.value)
      return el

   # ..........................................................................
   # Activity
   def _addActivity(self, parent, activity):
      """
      @summary: Adds a PROV-XML activity element
      @param parent: The parent element to add this element to
      @param activity: A PROV Activity object
      """
      el = self._addProvElement(parent, "activity", activity)
      self._addProvTime(el, activity.startTime, "startTime")
      self._addProvTime(el, activity.endTime, "endTime")
      return el
   
   # ..........................................................................
   # Generation
   def _addGeneration(self, parent, generation):
      """
      @summary: Adds a PROV-XML wasGeneratedBy element
      @param parent: The parent element to add this element to
      @param generation: A PROV Generation object
      """
      el = self._addProvElement(parent, "wasGeneratedBy", generation)
      self._addProvReference(el, "entity", generation.entityRef)
      self._addProvReference(el, "activity", generation.activityRef)
      self._addProvTime(el, generation.time, "time")
      return el
   
   # ..........................................................................
   # Usage
   def _addUsage(self, parent, usage):
      """
      @summary: Adds a PROV-XML used element
      @param parent: The parent element to add this element to
      @param usage: A PROV Usage object
      """
      el = self._addProvElement(parent, "used", usage)
      self._addProvReference(el, "activity", usage.activityRef)
      self._addProvReference(el, "entity", usage.entityRef)
      self._addProvTime(el, usage.time, "time")
      return el
   
   # ..........................................................................
   # Communication
   def _addCommunication(self, parent, communication):
      """
      @summary: Adds a PROV-XMl wasInformedBy element
      @param parent: The parent element to add this element to
      @param communication: A PROV Communication object
      """
      el = self._addProvElement(parent, "wasInformedBy", communication)
      self._addProvReference(el, "informed", communication.informedActivityRef)
      self._addProvReference(el, "informant", 
                                            communication.informantActivityRef)
      return el
   
   # ..........................................................................
   # Start
   def _addStart(self, parent, start):
      """
      @summary: Adds a PROV-XML wasStartedBy element
      @param parent: The parent element to add this element to
      @param start: A PROV Start object
      """
      el = self._addProvElement(parent, "wasStartedBy", start)
      self._addProvReference(el, "activity", start.activityRef)
      self._addProvReference(el, "trigger", start.triggerRef)
      self._addProvReference(el, "starter", start.starterRef)
      self._addProvTime(el, start.time, "time")
      return el
   
   # ..........................................................................
   # End
   def _addEnd(self, parent, end):
      """
      @summary: Adds a PROV-XML wasEndedBy element
      @param parent: The parent element to add this element to
      @param end: A PROV End object
      """
      el = self._addProvElement(parent, "wasEndedBy", end)
      self._addProvReference(el, "activity", end.activityRef)
      self._addProvReference(el, "trigger", end.triggerRef)
      self._addProvReference(el, "ender", end.enderRef)
      self._addProvTime(el, end.time, "time")
      return el
   
   # ..........................................................................
   # Invalidation
   def _addInvalidation(self, parent, invalidation):
      """
      @summary: Adds a PROV-XML wasInvalidatedBy element
      @param parent: The parent element to add this element to
      @param invalidation: A PROV Invalidation object
      """
      el = self._addProvElement(parent, "wasInvalidatedBy", invalidation)
      self._addProvReference(el, "entity", invalidation.entityRef)
      self._addProvReference(el, "activity", invalidation.activityRef)
      self._addProvTime(el, invalidation.time, "time")
      return el
   
   # ..........................................................................
   
   # ..........................................................................
   # Derivations
   # ..........................................................................
   # ..........................................................................
   # Derivation
   def _addDerivation(self, parent, derivation, tag="wasDerivedFrom"):
      """
      @summary: Adds a PROV-XML derivation element
      @param parent: The parent element to add this element to
      @param derivation: A PROV Derivation object
      @param tag: The XML tag for this element, should not be changed by user
      """
      el = self._addProvElement(parent, tag, derivation)
      self._addProvReference(el, "generatedEntity", 
                                                 derivation.generatedEntityRef)
      self._addProvReference(el, "usedEntity", derivation.usedEntityRef)
      self._addProvReference(el, "activity", derivation.activityRef)
      self._addProvReference(el, "generation", derivation.generationRef)
      self._addProvReference(el, "used", derivation.usageRef)
      return el
   
   # ..........................................................................
   # Revision
   def _addRevision(self, parent, derivation):
      """
      @summary: Adds a PROV-XML wasRevisionOf element
      @param parent: The parent element to add this element to
      @param derivation: A PROV Revision object
      @see: _addDerivation 
      """
      return self._addDerivation(parent, derivation, tag="wasRevisionOf")
   
   # ..........................................................................
   # Quotation
   def _addQuotation(self, parent, derivation):
      """
      @summary: Adds a PROV-XML wasQuotedFrom element
      @param parent: The parent element to add this element to
      @param derivation: A PROV Quotation object
      @see: _addDerivation
      """
      return self._addDerivation(parent, derivation, tag="wasQuotedFrom")
   
   # ..........................................................................
   # Primary Source
   def _addPrimarySource(self, parent, derivation):
      """
      @summary: Adds a PROV-XML hadPrimarySource element
      @param parent: The parent element to add this element to
      @param derivation: A PROV PrimarySource object
      @see: _addDerivation
      """
      return self._addDerivation(parent, derivation, tag="hadPrimarySource")
   
   # ..........................................................................
   
   # ..........................................................................
   # Agents, Responsibility, and Influence
   # ..........................................................................
   # ..........................................................................
   # Agent
   def _addAgent(self, parent, agent, tag="agent"):
      """
      @summary: Adds a PROV-XML agent element
      @param parent: The parent element to add this element to
      @param agent: A PROV Agent object
      @param tag: The XML tag for the element, should not be changed by the user
      """
      return self._addProvElement(parent, tag, agent)
   
   # ..........................................................................
   # Person
   def _addPerson(self, parent, agent):
      """
      @summary: Adds a PROV-XML person element
      @param parent: The parent element to add this element to
      @param agent: A PROV Person object
      @see: _addAgent
      """
      return self._addAgent(parent, agent, tag="person")
   
   # ..........................................................................
   # Organization
   def _addOrganization(self, parent, agent):
      """
      @summary: Adds a PROV-XML organization element
      @param parent: The parent element to add this element to
      @param agent: A PROV Organization element
      @see: _addAgent
      """
      return self._addAgent(parent, agent, tag="organization")
   
   # ..........................................................................
   # Software Agent
   def _addSoftwareAgent(self, parent, agent):
      """
      @summary: Adds a PROV-XML softwareAgent element
      @param parent: The parent element to add this element to
      @param agent: A PROV SoftwareAgent element
      @see: _addAgent
      """
      return self._addAgent(parent, agent, tag="softwareAgent")
   
   # ..........................................................................
   # Attribution
   def _addAttribution(self, parent, attribution):
      """
      @summary: Adds a PROV-XML wasAttributedTo element
      @param parent: The parent element to add this element to
      @param attribution: A PROV Attribution element
      """
      el = self._addProvElement(parent, "wasAttributedTo", attribution)
      self._addProvReference(el, "entity", attribution.entityRef)
      self._addProvReference(el, "agent", attribution.agentRef)
      return el
   
   # ..........................................................................
   # Association
   def _addAssociation(self, parent, association):
      """
      @summary: Adds a PROV-XML wasAssociatedWith element
      @param parent: The parent element to add this element to
      @param association: A PROV Association object
      """
      el = self._addProvElement(parent, "wasAssociatedWith", association)
      self._addProvReference(el, "activity", association.activityRef)
      self._addProvReference(el, "agent", association.agentRef)
      self._addProvReference(el, "prov", association.planRef)
      return el
   
   # ..........................................................................
   # Plan
   def _addPlan(self, parent, plan):
      """
      @summary: Adds a PROV-XML plan element
      @param parent: The parent element to add this element to
      @param plan: A PROV Plan object
      @see: _addEntity
      """
      return self._addEntity(parent, plan, tag="plan")
   
   # ..........................................................................
   # Delegation
   def _addDelegation(self, parent, delegation):
      """
      @summary: Adds a PROV-XML actedOnBehalfOf element
      @param parent: The parent element to add this element to
      @param delegation: A PROV Delegation object
      """
      el = self._addProvElement(parent, "actedOnBehalfOf", delegation)
      self._addProvReference(el, "delegate", delegation.delegateRef)
      self._addProvReference(el, "responsible", delegation.responsibleRef)
      self._addProvReference(el, "activity", delegation.activityRef)
      return el
   
   # ..........................................................................
   # Influence
   def _addInfluence(self, parent, influence):
      """
      @summary: Adds a PROV-XML wasInfluencedBy element
      @param parent: The parent element to add this element to
      @param influence: A PROV Influence object
      """
      el = self._addProvElement(parent, "wasInfluencedBy", influence)
      self._addProvReference(el, "influencee", influence.influenceeRef)
      self._addProvReference(el, "influencer", influence.influencerRef)
      self._addProvAttributes(el, influence)
      return el
   
   # ..........................................................................
   
   # ..........................................................................
   # Bundles
   # ..........................................................................
   # ..........................................................................
   # Bundle
   def _addBundle(self, parent, bundle):
      """
      @summary: Adds a PROV-XML bundle element
      @param parent: The parent element to add this element to
      @param bundle: A PROV Bundle object
      @see: _addEntity
      """
      return self._addEntity(parent, bundle, tag="bundle")
   
   # ..........................................................................
   # Bundle Constructor
   # ..........................................................................
   def _addBundledContent(self, parent, bundledContent):
      """
      @summary: Adds a PROV-XML bundledContent element
      @param parent: The parent element to add this element to
      @param bundledContent: A PROV BundleConstructor object
      @see: _processBundle
      """
      el = self._addProvElement(parent, "bundledContent", bundledContent, 
                                                           addAttributes=False)
      self._processBundle(el, bundledContent)
      return el
      
   # ..........................................................................
   # Alternate Entities
   # ..........................................................................
   # ..........................................................................
   # Specialization
   def _addSpecialization(self, parent, specialization):
      """
      @summary: Adds a PROV-XML specializationOf element
      @param parent: The parent element to add this element to
      @param specialization: A PROV Specialization object
      """
      el = self._addProvElement(parent, "specializationOf", specialization, 
                                                           addAttributes=False)
      self._addProvReference(el, "specificEntity", 
                                              specialization.specificEntityRef)
      self._addProvReference(el, "generalEntity", 
                                               specialization.generalEntityRef)
      return el
   
   # ..........................................................................
   # Alternate
   def _addAlternate(self, parent, alternate):
      """
      @summary: Adds a PROV-XML alternateOf element
      @param parent: The parent element to add this element to
      @param alternate: A PROV Alternate object
      """
      el = self._addProvElement(parent, "alternateOf", alternate, 
                                                           addAttributes=False)
      self._addProvReference(el, "alternate1", alternate.alternate1Ref)
      self._addProvReference(el, "alternate2", alternate.alternate2Ref)
      return el
   
   # ..........................................................................
   # Collections
   # ..........................................................................
   # ..........................................................................
   # Collection
   def _addCollection(self, parent, collection):
      """
      @summary: Adds a PROV-XML collection element
      @param parent: The parent element to add this element to
      @param collection: A PROV Collection object
      @see: _addEntity
      """
      return self._addEntity(parent, collection, tag="collection")
    
   # ..........................................................................
   # Empty Collection
   def _addEmptyCollection(self, parent, collection):
      """
      @summary: Adds a PROV-XML emptyCollection element
      @param parent: The parent element to add this element to
      @param collection: A PROV EmptyCollection object
      @see: _addEntity
      """
      return self._addEntity(parent, collection, tag="emptyCollection")
   
   # ..........................................................................
   # Membership
   def _addMembership(self, parent, membership):
      """
      @summary: Adds a PROV-XML hadMember element
      @param parent: The parent element to add this element to
      @param membership: A PROV Membership object
      """
      el = self._addProvElement(parent, "hadMember", membership, 
                                                           addAttributes=False)
      self._addProvReference(el, "collection", membership.collectionRef)
      for entityRef in membership.entityRefs:
         self._addProvReference(el, "entity", entityRef)
      return el
   
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
            attrib["xml:lang"] = label.lang
         val = label.label
      except:
         val = label
      return SubElement(parent, "label", attrib=attrib, value=val)
      
   # ..........................................................................
   # Location
   def _addLocation(self, parent, location):
      """
      @summary: Adds a PROV location element
      @param parent: The parent element to add this element to
      @param location: A PROV Location object or location string
      """
      try:
         attrib = {QName(XSI_NS, type): location.locType}
         val = location.location
      except:
         attrib = {QName(XSI_NS, type): "xsd:string"}
         val = location
      return SubElement(parent, "location", attrib=attrib, value=val)
      
   # ..........................................................................
   # Role
   def _addRole(self, parent, role):
      """
      @summary: Adds a PROV role element
      @param parent: The parent element to add this element to
      @param role: A PROV Role object or role string
      """
      try:
         attrib = {QName(XSI_NS, "type"): role.roleType}
         val = role.role
      except:
         attrib = {QName(XSI_NS, "type") : "xsd:QName"}
         val = role
      return SubElement(parent, "role", attrib=attrib, value=val)
      
   # ..........................................................................
   # Type
   def _addProvType(self, parent, provType):
      """
      @summary: Adds a PROV type element
      @param parent: The parent element to add this element to
      @param provType: A PROV ProvType object or prov type string
      """
      try:
         attrib = {QName(XSI_NS, "type") : provType.provTypeType}
         val = provType.provType
      except:
         attrib = {QName(XSI_NS, "type"): "xsd:QName"}
         val = provType
      return SubElement(parent, "type", attrib=attrib, value=val)
      
   # ..........................................................................
   # Value
   def _addValue(self, parent, value):
      """
      @summary: Adds a PROV value element
      @param parent: The parent element to add this element to
      @param value: A PROV Value object or value string
      """
      try:
         attrib = {QName(XSI_NS, "type"): value.valueType}
         val = value.value
      except:
         attrib = {QName(XSI_NS, "type"): "xsd:string"}
         val = value
      return SubElement(parent, "value", attrib=attrib, value=val)
      
   # ..........................................................................
   
   # ..........................................................................
   # Document
   # ..........................................................................
   def _addDocument(self, provDocument, attrib={}):
      """
      @summary: Adds a PROV document element
      @param provDocument: A PROV Document element
      @param attrib: Any additional attributes to add to the element
      @see: _processBundle 
      """
      setDefaultNamespace(PROV_NS)
      el = Element("document", attrib=attrib)
      self._processBundle(el, provDocument)
      return el
   
   # ..........................................................................
   # Other
   # ..........................................................................
   def _addOther(self, parent, other):
      """
      @summary: Adds other elements to the parent
      @param parent: The parent element to add this element to
      @param other: An object to serialize and add as an element to the parent
      """
      self._addedLM = True
      serialize(other.obj, parent=parent, namespace=other.namespace)
   
   # ..........................................................................
   def _processBundle(self, parent, bundledContent):
      """
      @summary: Processes PROV bundled content and creates elements
      @param parent: The parent element to add this element to
      @param bundledContent: A PROV BundledContent object
      """
      for key in bundledContent.entities.keys():
         self._addEntity(parent, bundledContent.entities[key])
      for key in bundledContent.collections.keys():
         collection = bundledContent.collections[key]
         if isinstance(collection, prov.EmptyCollection):
            self._addEmptyCollection(parent, collection)
         else:
            self._addCollection(parent, collection)
      for key in bundledContent.memberships.keys():
         self._addMembership(parent, bundledContent.memberships[key])
      for key in bundledContent.activities.keys():
         self._addActivity(parent, bundledContent.activities[key])
      for key in bundledContent.generations.keys():
         self._addGeneration(parent, bundledContent.generations[key])
      for key in bundledContent.usages.keys():
         self._addUsage(parent, bundledContent.usages[key])
      for key in bundledContent.communications.keys():
         self._addCommunication(parent, bundledContent.communications[key])
      for key in bundledContent.starts.keys():
         self._addStart(parent, bundledContent.starts[key])
      for key in bundledContent.ends.keys():
         self._addEnd(parent, bundledContent.ends[key])
      for key in bundledContent.invalidations.keys():
         self._addInvalidation(parent, bundledContent.invalidations[key])
      for key in bundledContent.derivations.keys():
         deriv = bundledContent.derivations[key]
         if isinstance(deriv, prov.PrimarySource):
            self._addPrimarySource(parent, deriv)
         elif isinstance(deriv, prov.Quotation):
            self._addQuotation(parent, deriv)
         elif isinstance(deriv, prov.Revision):
            self._addRevision(parent, deriv)
         else:
            self._addDerivation(parent, deriv)
      for key in bundledContent.agents.keys():
         agent = bundledContent.agents[key]
         if isinstance(agent, prov.Organization):
            self._addOrganization(parent, agent)
         elif isinstance(agent, prov.Person):
            self._addPerson(parent, agent)
         elif isinstance(agent, prov.SoftwareAgent):
            self._addSoftwareAgent(parent, agent)
         else:
            self._addAgent(parent, agent)
      for key in bundledContent.attributions.keys():
         self._addAttribution(parent, bundledContent.attributions[key])
      for key in bundledContent.associations.keys():
         self._addAssociation(parent, bundledContent.associations[key])
      for key in bundledContent.delegations.keys():
         self._addDelegation(parent, bundledContent.delegations[key])
      for key in bundledContent.influences.keys():
         self._addInfluence(parent, bundledContent.influences[key])
      try: # Only for documents
         for bundle in bundledContent.bundles:
            self._addBundle(parent, bundle)
      except:
         pass
      for key in bundledContent.alternates.keys():
         self._addAlternate(parent, bundledContent.alternates[key])
      for key in bundledContent.specializations.keys():
         self._addSpecialization(parent, bundledContent.specializations[key])
   
   # ..........................................................................
   # Helpers
   # ..........................................................................
   def _addProvReference(self, parent, tag, identifier):
      """
      @summary: Adds a PROV reference element to an element
      @param parent: The element to add the reference to
      @param name: The XML tag for the new element
      @param identifier: The value of the identifier reference
      """
      if identifier is not None:
         return SubElement(parent, tag, 
                           attrib={QName(PROV_NS, "ref"): identifier})
      else:
         return None
   
   # ..........................................................................
   def _addProvTime(self, parent, timeObj, tag):
      """
      @summary: Adds a PROV time element to an element
      @param parent: The element to add the time element to
      @param timeObj: The time object to add
      @param tag: The XML tag for the new element
      """
      if timeObj is not None:
         formattedTime = self._formatTime(timeObj)
         return SubElement(parent, tag, value=formattedTime)
      else:
         return None
   
   def _addProvElement(self, parent, tag, provObj, addAttributes=True):
      """
      @summary: Adds a PROV element to an element
      @param parent: The element to add the PROV element to
      @param tag: The XML tag for the new element
      @param provObj: A PROV object
      @param addAttributes: (optional) Boolean indicating if the function 
                               should attempt to add PROV attributes to the new 
                               element
      """
      attrib = {}
      try:
         if provObj.identifier is not None:
            attrib[QName(PROV_NS, "id")] = provObj.identifier
      except:
         pass
      
      el = SubElement(parent, tag, attrib=attrib)
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
