"""
@summary: Classes and functions used for creating PROV content
@author: CJ Grady
@status: alpha
@version: 1.0
@see: http://www.w3.org/TR/2013/REC-prov-dm-20130430
@note: PROV element descriptions pulled from W3 standard

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

@todo: Seems to work well for our specific case but needs unit testing
@todo: Test 'others'
@todo: Should 'key' a private property?
"""
from types import ListType

# .............................................................................
def getReference(item):
   """
   @summary: If the item is an object instead of a reference, return the 
                identifier, else just return the string
   @todo: Consider setting the representation for all PROV objects to the 
             identifier and just returning that
   """
   try:
      return item.identifier
   except:
      return item

# .............................................................................
def makeList(item):
   """
   @summary: Make sure the item is a list
   """
   if isinstance(item, ListType):
      return item
   else:
      return [item]
   
# .............................................................................
class _ProvBase(object):
   """
   @summary: Base class for PROV since most of the elements have these 
                attributes in common
   """
   def __init__(self, identifier=None, label=[], location=[], role=[], 
                      provType=[], others=[]):
      """
      @param identifier: A string identifier
      @param label: A label or list of labels for the element.  Can either be 
                       strings or Label objects
      @param location: A location or list of locations for the element.  Can 
                          either be strings or Location objects
      @param role: A role or list of roles for the element.  Can either be 
                      strings or Role objects
      @param provType: A type or list of types for the element.  Can either be
                          strings or ProvType objects
      @param others: A list of other attributes for the object
      @note: The variable 'key' is used at the Bundle / Document level to make
                sure things are not duplicated.
      """
      self.identifier = identifier
      self.label = makeList(label)
      self.location = makeList(location)
      self.role = makeList(role)
      self.provType = makeList(provType)
      self.others = makeList(others)
      self.key = identifier

# .............................................................................
class Entity(_ProvBase):
   """
   @summary: An entity is a physical, digital, conceptual, or other kind of 
                thing with some fixed aspects; entities may be real or imaginary
   @see: _ProvBase
   """
   # .................................
   def __init__(self, identifier, label=[], location=[], provType=[], 
                      value=None, others=[]):
      """
      @param value: (optional) A value for the entity
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              location=location, provType=provType, 
                              others=others)
      self.value = value
      
# .............................................................................
class Activity(_ProvBase):
   """
   @summary: An activity is something that occurs over a period of time and 
                acts upon or with entities; it may include consuming, 
                processing, transforming, modifying, relocating, using, or 
                generating entities.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, identifier, startTime=None, endTime=None, label=[], 
                      location=[], provType=[], others=[]):
      """
      @param startTime: (optional) A time for the start of the activity
      @param endTime: (optional) A time for the end of the activity
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              location=location, provType=provType, 
                              others=others)
      self.startTime = startTime
      self.endTime = endTime

# .............................................................................
class Generation(_ProvBase):
   """
   @summary: Generation is the completion of production of a new entity by an
                activity.  This entity did not exist before generation and 
                becomes available for usage after this generation.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, entity, activity=None, time=None, identifier=None, 
                      label=[], location=[], role=[], provType=[], others=[]):
      """
      @param entity: The entity created (Entity object or identifier for it)
      @param activity: (optional) The activity, or reference to the activity, 
                          that created the entity
      @param time: (optional) The generation time for the entity
      @note: At least one of the following must be provided - 
                identifier, activity, time, others
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              location=location, role=role, provType=provType, 
                              others=others)
      self.entityRef = getReference(entity)
      self.activityRef = getReference(activity)
      self.time = time
      if self.identifier is None:
         self.key = "%s-%s-%s" % (self.entityRef, self.activityRef, self.time)
      
      if self.identifier and self.activityRef is None and self.time is None \
             and len(self.others) == 0:
         raise Exception, \
            "Generation must have an id, activity, time, or attributes"
      
# .............................................................................
class Usage(_ProvBase):
   """
   @summary: Usage is the beginning of utilizing an entity by an activity.  
                Before usage, the activity had not begun to utilize this entity
                and could not have been affected by the entity.  (Note: This
                definition is formulated for a given usage; it is permitted for
                an activity to have used a same entity multiple times.)
   @see: _ProvBase
   """
   # .................................
   def __init__(self, activity, entity=None, time=None, identifier=None, 
                      label=[], location=[], role=[], provType=[], others=[]):
      """
      @param activity: The activity, or reference to the activity, that used  
                          the entity
      @param entity: (optional) The entity, or reference to the entity, being 
                        used
      @param time: (optional) Usage time at which the entity started to be used
      @note: At least one of the following must be provided - 
                identifier, entity, time, others
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              location=location, role=role, provType=provType, 
                              others=others)
      self.activityRef = getReference(activity)
      self.entityRef = getReference(entity)
      self.time = time
      
      if self.identifier is None:
         self.key = "%s-%s-%s" % (self.entityRef, self.activityRef, self.time)

      if self.identifier and self.activityRef is None and self.time is None \
            and len(self.others) == 0:
         raise Exception, "Usage must have an id, entity, time, or attributes"

# .............................................................................
class Communication(_ProvBase):
   """
   @summary: Communication is the exchange of some unspecified entity by two
                activities, one activity using some entity generated by the 
                other.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, informantActivity, informedActivity, identifier=None, 
                   label=[], provType=[], others=[]):
      """
      @param informantActivity: The identifier of the informant activity
      @param informedActivity: The identifier of the informed activity
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              provType=provType, others=others)
      self.informantActivityRef = getReference(informantActivity)
      self.informedActivityRef = getReference(informedActivity)

      if self.identifier is None:
         self.key = "%s-%s" % (self.informantActivityRef, 
                               self.informedActivityRef)
      
# .............................................................................
class Start(_ProvBase):
   """
   @summary: Start is when an activity is deemed to have been started by an 
                entity, known as a trigger.  The activity did not exist before 
                its start.  Any usage, generation, or invalidation involving an 
                activity follows the activity's start.  A start may refer to a 
                trigger entity that set off the activity, or to an activity, 
                known as starter, that generated the trigger.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, activity, triggerEntity=None, starterActivity=None, 
                      time=None, identifier=None, label=[], location=None, 
                      role=[], provType=[], others=[]):
      """
      @param activity: The activity, or reference to the activity, that was 
                          started
      @param triggerEntity: (optional) The entity, or reference to the entity, 
                               that triggered the activity
      @param starterActivity: (optional) The activity, or reference to the 
                                 activity, that generated the (possibly 
                                 unspecified) triggerEntity
      @param time: (optional) The time at which the activity was started
      @note: At least one of the following must be provided - 
                identifier, triggerEntity, starterActivity, time, others
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              location=location, role=role, provType=provType, 
                              others=others)
      self.activityRef = getReference(activity)
      self.triggerRef = getReference(triggerEntity)
      self.starterRef = getReference(starterActivity)
      self.time = time
      
      if self.identifier is None:
         self.key = "%s-%s-%s-%s" % (self.activityRef, self.triggerRef, 
                                     self.starterRef, self.time)

      if self.identifier and self.triggerRef is None \
            and self.starterRef is None and self.time is None \
            and len(self.others) == 0:
         raise Exception, \
            "Start must have an id, trigger, starter, time, or attributes"

# .............................................................................
class End(_ProvBase):
   """
   @summary: End is when an activity is deemed to have been ended by an entity,
                known as a trigger.  The activity no longer exists after its
                end.  Any usage, generation, or invalidation involving an 
                activity precedes the activity's end.  An end may refer to a 
                trigger entity that terminated the activity, or to an activity,
                known as ender that generated the trigger.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, activity, triggerEntity=None, enderActivity=None, 
                      time=None, identifier=None, label=[], location=None, 
                      role=[], provType=[], others=[]):
      """
      @param activity: The activity, or reference to the activity, that was 
                          ended
      @param triggerEntity: (optional) The entity, or reference to the entity, 
                               triggering the activity ending
      @param enderActivity: (optional) The activity, or reference to the 
                               activity, that generated the (possibly 
                               unspecified) triggerEntity
      @param time: (optional) The time at which the activity was ended
      @note: At least one of the following must be provided -
                identifier, triggerEntity, enderActivity, time, others
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              location=location, role=role, provType=provType, 
                              others=others)
      self.activityRef = getReference(activity)
      self.triggerRef = getReference(triggerEntity)
      self.enderRef = getReference(enderActivity)
      self.time = time
      
      if self.identifier is None:
         self.key = "%s-%s-%s-%s" % (self.activityRef, self.triggerRef, 
                                     self.enderRef, self.time)

      if self.identifier is None and self.triggerRef is None \
            and self.enderRef is None and self.time is None \
            and len(self.others) == 0:
         raise Exception, \
            "End must have an id, trigger, ender, time, or attributes"

# .............................................................................
class Invalidation(_ProvBase):
   """
   @summary: Invalidation is the start of the destruction, cessation, or expiry
                of an existing entity by an activity.  The entity is no longer
                available for use (or further invalidation) after invalidation.
                Any generation or usage of an entity precedes its invalidation.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, entity, activity=None, time=None, identifier=None, 
                      label=[], location=[], role=[], provType=[], others=[]):
      """
      @param entity: The entity, or reference to the entity, that was 
                        invalidated
      @param activity: (optional) The activity, or reference to the activity, 
                          that invalidated the entity
      @param time: (optional) The invalidation time for the entity
      @note: At least one of the following must be provided -
                identifier, activity, time, others
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              location=location, role=role, provType=provType, 
                              others=others)
      self.entityRef = getReference(entity)
      self.activityRef = getReference(activity)
      self.time = time
      
      if self.identifier is None:
         self.key = "%s-%s-%s" % (self.entityRef, self.activityRef, self.time)

      if self.identifier is None and self.activityRef is None \
            and self.time is None and len(self.others) == 0:
         raise Exception, \
            "Invalidation must have either an identifier, activity, time, or attributes"
      
# .............................................................................
class Derivation(_ProvBase):
   """
   @summary: A derivation is a transformation of an entity into another, an 
                update of an entity resulting in a new one, or the construction 
                of a new entity based on a pre-existing entity.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, generatedEntity, usedEntity, activity=None, 
                      generation=None, usage=None, identifier=None, label=[], 
                      provType=[], others=[]):
      """
      @param generatedEntity: The entity, or reference to the entity, that was 
                                 generated by the derivation
      @param usedEntity: The entity, or reference to the entity, that was used 
                            by the derivation
      @param activity: (optional) The activity, or reference to the activity, 
                          that used and generated the entities
      @param generation: (optional) The generation, or reference to the 
                            generation, involving the usedEntity and activity
      @param usage: (optional) The usage, or reference to the usage, involving 
                       the used entity and the activity 
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              provType=provType, others=others)
      self.generatedEntityRef = getReference(generatedEntity)
      self.usedEntityRef = getReference(usedEntity)
      self.activityRef = getReference(activity)
      self.generationRef = getReference(generation)
      self.usageRef = getReference(usage)
      
      if self.identifier is None:
         self.key = "%s-%s-%s-%s-%s" % (self.generatedEntityRef, 
                                        self.usedEntityRef, self.activityRef, 
                                        self.generationRef, self.usageRef)
      
# .............................................................................
class Revision(Derivation):
   """
   @summary: A Revision is a derivation for which the resulting entity is a 
                revised version of some original.
   @see: Derivation
   """
   pass

# .............................................................................
class Quotation(Derivation):
   """
   @summary: A Quotation is the repeat of (some or all of) an entity, such as
                text or image, by someone who may or may not be its original
                author.
   @see: Derivation
   """
   pass

# .............................................................................
class PrimarySource(Derivation):
   """
   @summary: A PrimarySource for a topic refers to something produced by some 
                agent with direct experience and knowledge about the topic, at
                the time of the topic's study, without benefit from hindsight.
   @see: Derivation
   """
   pass

# .............................................................................
class Agent(_ProvBase):
   """
   @summary: An Agent is something that bears some of the responsibility for an 
                activity taking place, for the existence of an entity, or for 
                another agent's activity.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, identifier, label=[], location=[], provType=[], 
                      others=[]):
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              location=location, provType=provType, 
                              others=others)

# .............................................................................
class SoftwareAgent(Agent):
   """
   @summary: A SoftwareAgent is running software.
   @see: Agent
   """
   pass

# .............................................................................
class Organization(Agent):
   """
   @summary: An Organization is a social or legal institution such as a 
                company, society, etc.
   @see: Agent
   """
   pass

# .............................................................................
class Person(Agent):
   """
   @summary: Person agents are people.
   @see: Agent
   """
   pass

# .............................................................................
class Attribution(_ProvBase):
   """
   @summary: Attribution is the ascribing of an entity to an agent.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, entity, agent, identifier=None, label=[], provType=[], 
                      others=[]):
      """
      @param entity: The entity, or reference to the entity
      @param agent: The agent, or reference to the agent, whom the entity is 
                       ascribed to, and therefore bears some responsibility for 
                       its existence
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              provType=provType, others=others)
      self.entityRef = getReference(entity)
      self.agentRef = getReference(agent)
      
      if self.identifier is None:
         self.key = "%s-%s" % (self.entityRef, self.agentRef)

# .............................................................................
class Association(_ProvBase):
   """
   @summary: An activity Association is an assignment of responsibility to an 
                agent for an activity, indicating that the agent had a role in 
                the activity.  It further allows for a plan to be specified, 
                which is the plan intended by the agent to achieve some goals 
                in the context of this activity.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, activity, agent=None, plan=None, identifier=None, 
                      label=[], role=[], provType=[], others=[]):
      """
      @param activity: The activity, or reference to the activity
      @param agent: (optional) The agent, or reference to the agent, associated 
                       with the activity
      @param plan: (optional) The plan, or reference to the plan, the agent 
                      relied on in the context of this activity.
      @note: At least one of the following must be provided -
                identifier, agent, plan, others
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, role=role, 
                              provType=provType, others=others)
      self.activityRef = getReference(activity)
      self.agentRef = getReference(agent)
      self.planRef = getReference(plan)
      
      if self.identifier is None:
         self.key = "%s-%s-%s" % (self.activityRef, self.agentRef, self.planRef)

      if self.identifier is None and self.agentRef is None \
            and self.planRef is None and len(self.others) == 0:
         raise Exception, \
            "Association must have an identifier, agent, plan, or attributes"

# .............................................................................
class Plan(Entity):
   """
   @summary: A Plan is an entity that represents a set of actions or steps 
                intended by one or more agents to achieve some goals.
   @see: Entity
   """
   pass

# .............................................................................
class Delegation(_ProvBase):
   """
   @summary: Delegation is the assignment of authority and responsibility to an 
                agent (by itself or by another agent) to carry out a specific 
                activity as a delegate or representative, while the agent it 
                acts on behalf of retains some responsibility for the outcome 
                of the delegated work.  For example, a student acted on behalf 
                of his or her supervisor, who acted on behalf of the department 
                chair, who acted on behalf of the university; all those agents 
                are responsible in some way for the activity that took place 
                but we do not say explicitly who bears responsibility and to 
                what degree.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, delegate, responsible, activity=None, identifier=None,
                      label=[], provType=[], others=[]):
      """
      @param delegate: The agent, or reference to the agent, associated with an 
                          activity, acting on behalf of the responsible agent
      @param responsible: The agent, or reference to the agent, on behalf or 
                             which the delegate agent acted
      @param activity: (optional) The activity, or reference to the activity, 
                          for which the delegation link holds
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              provType=provType, others=others)
      self.delegateRef = getReference(delegate)
      self.responsibleRef = getReference(delegate)
      self.activityRef = getReference(activity)
      
      if self.identifier is None:
         self.key = "%s-%s-%s" % (self.delegateRef, self.responsibleRef, 
                                  self.activityRef)
      
# .............................................................................
class Influence(_ProvBase):
   """
   @summary: Influence is the capacity of an entity, activity, or agent to have 
                an effect on the character, development, or behavior or another 
                by means of usage, start, end, generation, invalidation, 
                communication, derivation, attribution, association, or 
                delegation
   @see: _ProvBase
   """
   # .................................
   def __init__(self, influencee, influencer, identifier=None, label=[], 
                      provType=[], others=[]):
      """
      @param influencee: An entity, activity, agent, or reference to an entity, 
                            activity, or agent
      @param influencer: An entity, activity, agent, or reference to an entity,
                            activity, or agent, that is an ancestor that the 
                            influencee depends on
      """
      _ProvBase.__init__(self, identifier=identifier, label=label, 
                              provType=provType, others=others)
      self.influenceeRef = getReference(influencee)
      self.influencerRef = getReference(influencer)
      
      if self.identifier is None:
         self.key = "%s-%s" % (self.influenceeRef, self.influencerRef)
      
# .............................................................................
class Bundle(Entity):
   """
   @summary: A bundle is a named set of provenance descriptions, and is itself
               an entity, so allowing provenance of provenance to be expressed.
   @see: Entity
   """
   pass

# .............................................................................
class Specialization(_ProvBase):
   """
   @summary: An entity that is a Specialization of another shares all aspects 
                of the latter, and additionally presents more specific aspects 
                of the same thing as the latter.  In particular, the lifetime 
                of the entity being specialized contains that of any 
                specialization
   @see: _ProvBase
   """
   # .................................
   def __init__(self, specificEntity, generalEntity):
      """
      @param specificEntity: The entity, or reference to the entity, that is a 
                                specialization of the general entity
      @param generalEntity: The entity, or reference to the entity, that is 
                               being specialized.
      """
      _ProvBase.__init__(self)
      self.specificEntityRef = getReference(specificEntity)
      self.generalEntityRef = getReference(generalEntity)
      
      if self.identifier is None:
         self.key = "%s-%s" % (self.specificEntityRef, self.generalEntityRef)

# .............................................................................
class Alternate(_ProvBase):
   """
   @summary: Two Alternate entities present aspects of the same thing.  These
                aspects may be the same or different, and the alternate 
                entities may or may not overlap in time.
   @see: _ProvBase
   """
   # .................................
   def __init__(self, alternateEntity1, alternateEntity2):
      """
      @param alternateEntity1: The first entity, or reference to the first 
                                  entity
      @param alternateEntity2: The second entity, or reference to the second
                                  entity
      """
      _ProvBase.__init__(self)
      self.alternate1Ref = getReference(alternateEntity1)
      self.alternate2Ref = getReference(alternateEntity2)
      
      self.key = "%s-%s" % (self.alternate1Ref, self.alternate2Ref)

# .............................................................................
class Collection(Entity):
   """
   @summary: A Collection is an entity that provides a structure to some 
                constituents that must themselves be entities.  These 
                constituents are said to be member of the collections.
   @see: Entity
   """
   pass

# .............................................................................
class EmptyCollection(Collection):
   """
   @summary: An EmptyCollection is a collection without members
   @see: Collection
   """
   pass

# .............................................................................
class Membership(_ProvBase):
   """
   @summary: A Membership relation is defined for stating the members of a 
                collection
   @see: _ProvBase
   """
   # .................................
   def __init__(self, collection, entities=[]):
      """
      @param collection: The collection to add members to
      @param entities: A list of entities, or references to entities, to add 
                          as members of the collection
      """
      self.collectionRef = getReference(collection)
      self.entityRefs = []
      for entity in entities:
         self.entityRefs.append(getReference(entity))
      self.key = self.collectionRef
   
   # .................................
   def addEntity(self, entity):
      """
      @summary: Adds an entity to the membership
      @param entity: The entity to add to the membership
      """
      self.entityRefs.append(getReference(entity))

# .............................................................................
class Label(object):
   """
   @summary: A human-readable representation of a PROV object
   """
   # .................................
   def __init__(self, label, lang=None):
      """
      @param label: The human-readable string representing a PROV object
      @param lang: (optional) The language of the label
      """
      self.label = label
      self.lang = lang
      
# .............................................................................
class Location(object):
   """
   @summary: A Location can be an identifiable geographic place (ISO 19112), 
                but it can also be a non-geographic place such as a directory,
                row, or column.  As such, there are numerous ways in which 
                location can be expressed, such as by coordinate, address, 
                landmark, and so forth.  This document does not specify how to
                concretely express locations, but instead provide a mechanism
                to introduce locations, by means of a reserved attribute.
   """
   # .................................
   def __init__(self, location, locType=None):
      """
      @param location: The location to associate with a PROV object
      @param locType: (optional) A type associated with this location
      """
      self.location = location
      self.locType = locType
      
# .............................................................................
class Role(object):
   """
   @summary: A Role is the function of an entity or agent with respect to an 
                activity, in the context of a Usage, Generation, Invalidation, 
                Association, Start, and End.
   """
   # .................................
   def __init__(self, role, roleType=None):
      """
      @param role: The role to associate with a PROV entity or agent
      @param roleType: (optional) A type associated with this role
      """
      self.role = role
      self.roleType = roleType
      
# .............................................................................
class ProvType(object):
   """
   @summary: ProvType provides further typing information
   """
   # .................................
   def __init__(self, provType, provTypeType=None):
      """
      @param provType: The type of this PROV object
      @param provTypeType: (optional) The type of the type for this PROV object 
                              (such as xsd:QName)
      """
      self.provType = provType
      self.provTypeType = provTypeType
      
# .............................................................................
class Value(object):
   """
   @summary: A value that is a direct representation of an entity
   """
   # .................................
   def __init__(self, value, valueType=None):
      """
      @param value: The value of the entity
      @param valueType: (optional) The type of the value
      """
      self.value = value
      self.valueType = valueType

# .............................................................................
class Other(object):
   """
   @summary: An Other object is something to be added that is outside of the 
                base PROV data model.
   """
   # .................................
   def __init__(self, otherObject, namespace=None):
      """
      @param otherObject: The object to be added to the PROV document
      @param namespace: (optional) The namespace of the other object
      """
      self.obj = otherObject
      self.namespace = namespace
      
# .............................................................................
class _BundledContent(object):
   """
   @summary: A PROV container class that should is extended by Document and 
                Bundle
   """
   # .................................
   def __init__(self):
      self.provClassTypes = {
         'activity' : self.addActivity,
         'agent' : self.addAgent,
         'organization' : self.addAgent,
         'person' : self.addAgent,
         'softwareagent' : self.addAgent,
         'alternate' : self.addAlternate,
         'association' : self.addAssociation,
         'attribution' : self.addAttribution,
         'entity' : self.addEntity,
         'collection' : self.addCollection,
         'emptycollection' : self.addCollection,
         'communication' : self.addCommunication,
         'delegation' : self.addDelegation,
         'derivation' : self.addDerivation,
         'primarysource' : self.addDerivation,
         'quotation' : self.addDerivation,
         'revision' : self.addDerivation,
         'end' : self.addEnd,
         'generation' : self.addGeneration,
         'influence' : self.addInfluence,
         'invalidation' : self.addInvalidation,
         'membership' : self.addMembership,
         'plan' : self.addEntity,
         'specialization' : self.addSpecialization,
         'start' : self.addStart,
         'usage' : self.addUsage
       }

      self.entities = {}
      self.activities = {}
      self.generations = {}
      self.usages = {}
      self.communications = {}
      self.starts = {}
      self.ends = {}
      self.invalidations = {}
      
      # Derivations
      self.derivations = {}
      
      # Agents, Responsibility, Influence
      self.agents = {}
      self.attributions = {}
      self.associations = {}
      self.delegations = {}
      self.influences = {}
      
      # Alternates
      self.alternates = {}
      self.specializations = {}
      
      # Collections
      self.collections = {}
      self.memberships = {}
   
   # .................................
   def _addItemIfNotPresent(self, itemGroup, item):
      """
      @summary: Checks to see if an item is present in the specified group and
                   adds it if it is not.
      @note: This is used to prevent duplication in content
      @param itemGroup: The dictionary of items to check through
      @param item: The new item to try to add
      """
      if not itemGroup.has_key(item.key):
         itemGroup[item.key] = item

   # .................................
   def addActivity(self, activity):
      """
      @summary: Adds an activity to the bundled content
      """
      self._addItemIfNotPresent(self.activities, activity)
   
   # .................................
   def addAgent(self, agent):
      """
      @summary: Adds an agent to the bundled content
      """
      self._addItemIfNotPresent(self.agents, agent)

   # .................................
   def addAlternate(self, alternate):
      """
      @summary: Adds an alternate to the bundled content
      """
      self._addItemIfNotPresent(self.alternates, alternate)
   
   # .................................
   def addAssociation(self, association):
      """
      @summary: Adds an association to the bundled content
      """
      self._addItemIfNotPresent(self.associations, association)
   
   # .................................
   def addAttribution(self, attribution):
      """
      @summary: Adds an attribution to the bundled content
      """
      self._addItemIfNotPresent(self.attributions, attribution)
   
   # .................................
   def addCollection(self, collection):
      """
      @summary: Adds a collection to the bundled content
      """
      self._addItemIfNotPresent(self.collections, collection)
   
   # .................................
   def addCommunication(self, communication):
      """
      @summary: Adds a communication to the bundled content
      """
      self._addItemIfNotPresent(self.communications, communication)
   
   # .................................
   def addDelegation(self, delegation):
      """
      @summary: Adds a delegation to the bundled content
      """
      self._addItemIfNotPresent(self.delegations, delegation)
   
   # .................................
   def addDerivation(self, derivation):
      """
      @summary: Adds a derivation to the bundled content
      """
      self._addItemIfNotPresent(self.derivations, derivation)
   
   # .................................
   def addEnd(self, end):
      """
      @summary: Adds an end to the bundled content
      """
      self._addItemIfNotPresent(self.ends, end)

   # .................................
   def addEntity(self, entity):
      """
      @summary: Adds an entity to the bundled content
      """
      self._addItemIfNotPresent(self.entities, entity)
   
   # .................................
   def addGeneration(self, generation):
      """
      @summary: Adds a generation to the bundled content
      """
      self._addItemIfNotPresent(self.generations, generation)
   
   # .................................
   def addInfluence(self, influence):
      """
      @summary: Adds an influence to the bundled content
      """
      self._addItemIfNotPresent(self.influences, influence)
   
   # .................................
   def addInvalidation(self, invalidation):
      """
      @summary: Adds an invalidation to the bundled content
      """
      self._addItemIfNotPresent(self.invalidations, invalidation)
      
   # .................................
   def addMembership(self, membership):
      """
      @summary: Adds a membership to the bundled content
      """
      self._addItemIfNotPresent(self.memberships, membership)
   
   # .................................
   def addSpecialization(self, specialization):
      """
      @summary: Adds a specialization to the bundled content
      """
      self._addItemIfNotPresent(self.specializations, specialization)
   
   # .................................
   def addStart(self, start):
      """
      @summary: Adds a start to the bundled content
      """
      self._addItemIfNotPresent(self.starts, start)
   
   # .................................
   def addUsage(self, usage):
      """
      @summary: Adds an usage to the bundled content
      """
      self._addItemIfNotPresent(self.usages, usage)

   # .................................
   def add(self, provObject):
      """
      @summary: Adds an object to the document based on its type
      """
      k = provObject.__class__.__name__.lower()
      if self.provClassTypes.has_key(k):
         fn = self.provClassTypes[k] # Get add function
         fn(provObject) # Add object to appropriate place
      else:
         raise Exception, "%s object cannot be added to %s root" % (k, 
                                                      self.__class__.__name__)
      
# .............................................................................
class Document(_BundledContent):
   """
   @summary: A Document is a top level PROV object that acts as a container for
                other objects
   @see: _BundledContent
   """
   # .................................
   def __init__(self, others=[]):
      _BundledContent.__init__(self)
      self.bundles = []
      self.others = list(others)
      self.provClassTypes['bundle'] = self.addBundle
   
   # .................................
   def addBundle(self, bundle):
      """
      @summary: Adds a bundle to the Document
      """
      self.bundles.append(bundle)

# .............................................................................
class BundleConstructor(_BundledContent):
   """
   @summary: A BundleConstructor allows the content and the name of a bundle to
                be specified
   @see: _BundledContent
   """
   # .................................
   def __init__(self, identifier, others=[]):
      self.identifier = identifier
      self.others = list(others)
