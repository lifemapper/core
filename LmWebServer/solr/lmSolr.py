"""
@summary: This module contains functions used to search a solr instance
@author: CJ Grady
"""
from ast import literal_eval
from types import NoneType
import urllib2

from LmCommon.common.lmAttObject import LmAttObj
from LmCommon.common.lmconstants import LM_NAMESPACE, LM_NS_PREFIX, ENCODING
from LmCommon.common.lmXml import (Element, register_namespace, 
                        setDefaultNamespace, SubElement, tostring, PI)

from LmServer.base.utilities import escapeString, ObjectAttributeIterator
import LmServer.common.jsonTree as lmJson

SERVER = "localhost:8983/solr/"
COLLECTION = "lmArchive"
HINT_COLLECTION = "lmSpeciesHint"

# .............................................................................
def formatHit(hit):
   """
   @summary: Transforms the hit dictionary into something that we can merge
   @todo: Need a safer method of constructing this.  May not matter if it will
             be constructed on the server side.  Might consider organizing 
             based on the key name.  For example "model*" would match model.
   """
   mdlKey = "{algo} - {mdlId}".format(algo=hit['algorithmCode'], 
                                      mdlId=hit['modelId'])
   prjKey = hit['projectionId']
   
   hitDict = {
              "userId" : hit['userId'],
              "displayName" : hit['displayName'],
              "epsgCode" : hit['epsgCode'],
              "occurrenceSetId" : hit['occurrenceSetId'],
              "occurrenceSetMetadataUrl" : hit['occurrenceSetMetadataUrl'],
              "occurrenceSetBBox" : hit['occurrenceSetBBox'],
              "occurrenceSetModTime" : hit['occurrenceSetModTime'],
              "occurrenceSetDownloadUrl" : hit['occurrenceSetDownloadUrl'],
              "numPoints" : hit['numberOfOccurrencePoints'],
              "models" : {
               mdlKey : {
                "modelId" : hit['modelId'],
                "modelMetadataUrl" : hit['modelMetadataUrl'],
                "modelRulesetUrl" : hit['modelRulesetUrl'],
                "modelModTime" : hit['modelModTime'],
                "modelScenarioCode" : hit['modelScenarioCode'],
                "modelScenarioId" : hit['modelScenarioId'],
                "modelScenarioUrl" : hit['modelScenarioUrl'],
                "algorithmCode" : hit['algorithmCode'],
                "algorithmParameters" : hit['algorithmParameters'],
                "projections" : {
                 prjKey : {
                  "projectionId" : hit['projectionId'],
                  "projectionMetadataUrl" : hit['projectionMetadataUrl'],
                  "projectionBBox" : hit['projectionBBox'],
                  "projectionDownloadUrl" : hit['projectionDownloadUrl'],
                  "projectionModTime" : hit['projectionModTime'],
                  "projectionScenarioCode" : hit['projectionScenarioCode'],
                  "projectionScenarioId" : hit['projectionScenarioId'],
                  "projectionScenarioUrl" : hit['projectionScenarioUrl']
                 },
                },
               },
              },
             }
   return hitDict

# .............................................................................
def mergeHits(hit1, hit2):
   """
   @summary: Merges two (formatted) hits into one
   """
   # ............................
   def mergeModels(mdl1, mdl2):
      """
      @summary: Merge two model dictionaries
      @note: This is really just merging the projections dictionary
      """
      for prjKey in mdl2['projections'].keys():
         mdl1['projections'][prjKey] = mdl2['projections'][prjKey]
      return mdl1

   # ............................
   for mdlKey in hit2['models'].keys():
      if hit1['models'].has_key(mdlKey):
         hit1['models'][mdlKey] = mergeModels(hit1['models'][mdlKey],
                                              hit2['models'][mdlKey])
      else:
         hit1['models'][mdlKey] = hit2['models'][mdlKey]
   return hit1

# .............................................................................
def createHitObjects(hitsDict):
   hitObjs = []
   
   for hKey in sorted(hitsDict.keys()):
      h = hitsDict[hKey]
      oKeys = h.keys()
      oKeys.remove('models')
      occAtts = {k: h[k] for k in oKeys}
      
      newObj = LmAttObj(attrib=occAtts, name="OccurrenceSet")
      
      newObj.models = []
      
      for mKey in sorted(h['models'].keys()):
         mKeys = h['models'][mKey].keys()
         mKeys.remove('projections')
         mdlAtts = {k: h['models'][mKey][k] for k in mKeys}
         
         mdl = LmAttObj(attrib=mdlAtts, name="Model")
         mdl.projections = []
         
         for pKey in h['models'][mKey]['projections'].keys():
            pKeys = h['models'][mKey]['projections'][pKey].keys()
            prjAtts = {k: h['models'][mKey]['projections'][pKey][k] for k in pKeys}
            mdl.projections.append(LmAttObj(attrib=prjAtts, name="Projection"))
         newObj.models.append(mdl)
      hitObjs.append(newObj)
   return hitObjs

# .............................................................................
def searchArchive(name):
   """
   @summary: Search the archive
   @todo: This is pretty brittle.  These constants should be read from config 
             and this function could use quite a bit of bullet-proofing
   """
   url = "http://{server}{collection}/select?q=displayName%3A{name}&wt=python&indent=true".format(
           server=SERVER, collection=COLLECTION, name=name).replace(' ', '%20')
   res = urllib2.urlopen(url)
   resp = res.read()
   rDict = literal_eval(resp)
   
   hits = {}
   for h in rDict['response']['docs']:
      hKey = '{displayName} - {occId}'.format(displayName=h['displayName'], 
                                              occId=h['occurrenceSetId'])
      if hits.has_key(hKey):
         hits[hKey] = mergeHits(hits[hKey], formatHit(h))
      else:
         hits[hKey] = formatHit(h)
   
   #print hits
   # Objectify
   hitObjs = createHitObjects(hits)
   #f = StyledXmlFormatter(hitObjs)
   #return unicode(f.format())
   return formatXml(ObjectAttributeIterator("hits", hitObjs))


# .............................................................................
def _addObjectToTree(el, obj):
   """
   @summary: Adds an object to the ElementTree element
   @todo: Merge this with the XML formatter code.  I just copied it out of that
             module and made some small modificaitons so that it would not add
             extra fluff
   """
   for name, value in obj:
      if isinstance(value, ObjectAttributeIterator):
         attribs = dict([(k, value.attributes[k]) for k in value.attributes.keys()])
         subEl = SubElement(el, name, namespace=LM_NAMESPACE, attrib=attribs)
         _addObjectToTree(subEl, value)
      elif not isinstance(value, NoneType):
         if name is not None:
            if value is None or value == "":
               SubElement(el, name, namespace=LM_NAMESPACE)
            else:
               SubElement(el, name, value=escapeString(value, 'xml'), 
                          namespace=LM_NAMESPACE)

# .............................................................................
def formatXml(obj):
   """
   @todo: Merge this with the XML formatter code.  I just copied it out of that
             module and made some small modificaitons so that it would not add
             extra fluff
   """
   register_namespace(LM_NS_PREFIX, LM_NAMESPACE)
   setDefaultNamespace(LM_NAMESPACE)
   
   el = Element("response")

   pis = []
   pis.append(PI("xml", 'version="1.0" encoding="{}"'.format(ENCODING)))
   
   attribs = dict([(k, obj.attributes[k]) for k in obj.attributes.keys()])
   objEl = SubElement(el, obj.name, attrib=attribs)
   _addObjectToTree(objEl, obj)

   return '%s\n%s' % ('\n'.join([tostring(pi) for pi in pis]), tostring(el))

# .............................................................................
# .............................................................................
def searchHintIndex(name, retFormat, numColumns, maxReturned):
   """
   @summary: Search the hint service
   @todo: This is pretty brittle.  These constants should be read from config 
             and this function could use quite a bit of bullet-proofing
   """
   url = "http://{server}{collection}/select?q=displayName%3A{name}&wt=python&indent=true&sort=binomial+asc".format(
      server=SERVER, collection=HINT_COLLECTION, name=name).replace(' ', '%20')
   res = urllib2.urlopen(url)
   resp = res.read()
   rDict = literal_eval(resp)
   
   hits = rDict['response']['docs']
   
   if retFormat.lower() == "json":
      ret = transformHintsForJson(hits, columns=numColumns)
   elif retFormat.lower() == "newjson":
      ret = transformHintsForJsonNew(hits)
   else:
      ret = transformHintsForAutocomplete(hits, maximum=maxReturned)
      
   return ret

# .............................................................................
def transformHintsForAutocomplete(hits, maximum=24):
   """
   @summary: Transforms the results of a search for autocomplete
   @param hits: A list of Lucene search hits
   @param maximum: (optional) Return a maximum of this number of results
   """
   if hits is None or len(hits) == 0:
      return "No Suggestions\t0\t0"
   else:
      if len(hits) < maximum:
         maximum = len(hits)
         
      return ''.join(["%s\t%s\t%s\n" % (hits[x]["displayName"], 
           hits[x]["occurrenceSetId"], 
           str(hits[x]["numberOfOccurrencePoints"])) for x in xrange(maximum)])
   
# .............................................................................
def transformHintsForJson(hits, columns=1):
   """
   @summary: Transforms the results of a search into a JSON document
   @param hits: A list of Lucene search hits
   @param columns: (optional) Return this many columns of results
   @deprecated: This will be replaced in the future with the new version
   """
   jTree = lmJson.JsonObject()
   if hits is None:
      return "Search too broad, please enter additional characters"
   elif len(hits) > 0:
      if len(hits) < columns:
         columns = len(hits)
      
      i = 0 if len(hits) % columns == 0 else 1
      subListLength = len(hits) / columns + i
   
      colsAry = jTree.addArray("columns")
      for y in xrange(columns):
         ary = colsAry.addArray("")
         for x in xrange(y*subListLength, (y+1)*subListLength):
            try:
               o = ary.addObject("")
               o.addValue("className", 
                       "%sSpeciesRow" % str(x % 2 == 0 and 'even' or 'odd'))
               o.addValue("name", hits[x]["displayName"])
               o.addValue("numPoints", str(hits[x]["numberOfOccurrencePoints"]))
               o.addValue("numModels", str(hits[x]["numberOfModels"]))
               o.addValue("occurrenceSet", str(hits[x]["occurrenceSetId"]))
               o.addValue("binomial", hits[x]["binomial"])
               o.addValue("downloadUrl", hits[x]["occurrenceSetDownloadUrl"])
            except Exception, e:
               print str(e)
               #pass
      jTree.addValue("colWidth", "%s%%" % str(100/columns))
      return lmJson.tostring(jTree)
   else:
      return "None of the species currently in the Lifemapper database match"

# .............................................................................
def transformHintsForJsonNew(hits):
   """
   @summary: Transforms the results of a search into a JSON document
   @param hits: A list of Lucene search hits
   @note: This method does not include the extra "column" information and 
             will replace the _transformForJson method
   """
   jTree = lmJson.JsonObject()
   if hits is None:
      return "Search too broad, please enter additional characters"
   elif len(hits) > 0:
      ary = jTree.addArray("hits")
      for i in hits:
         try:
            o = ary.addObject("")
            o.addValue("name", i["displayName"])
            o.addValue("numPoints", str(i["numberOfOccurrencePoints"]))
            o.addValue("numModels", str(i["numberOfModels"]))
            o.addValue("occurrenceSet", str(i["occurrenceSetId"]))
            o.addValue("binomial", i["binomial"])
            o.addValue("downloadUrl", i["occurrenceSetDownloadUrl"])
         except:
            pass
      return lmJson.tostring(jTree)
   else:
      return "None of the species currently in the Lifemapper database match"
   
