"""
@summary: This module wraps interactions with Solr
@author: CJ Grady
@version: 1.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2017, University of Kansas Center for Research

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
from ast import literal_eval
from mx.DateTime import DateTimeFromMJD
import subprocess
import urllib2

from LmServer.common.lmconstants import (SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS, 
                                         SOLR_POST_COMMAND, SOLR_SERVER, 
                                         SOLR_SNIPPET_COLLECTION)
from LmServer.common.lmconstants import SnippetFields

# .............................................................................
def buildSolrDocument(docPairs):
   """
   @summary: Builds a document for a Solr POST from the key, value pairs
   @param docPairs: A list of lists of [field name, value] pairs -- 
                       [[(field name, value)]]
   """
   if not docPairs:
      raise Exception, "Must provide at least one pair for Solr POST"
   
   # We want to allow multiple documents.  Make sure that field pairs is a list
   #    of lists of tuples
   elif not isinstance(docPairs[0][0], (list, tuple)):
      docPairs = [docPairs]
   
   docLines = ['<add>']
   for fieldPairs in docPairs:
      docLines.append('   <doc>')
      for fName, fVal in fieldPairs:
         # Only add the field if the value is not None
         if fVal is not None: 
            docLines.append('      <field name="{}">{}</field>'.format(fName, 
                                                   fVal.replace('&', '&amp;')))
      docLines.append('   </doc>')
   docLines.append('</add>')
   return '\n'.join(docLines)

# .............................................................................
def postSolrDocument(collection, docFilename):
   """
   @summary: Posts a document to a Solr index
   @param collection: The name of the Solr core (index) to add this document to
   @param docFilename: The file location of the document to post
   """
   cmd = '{cmd} -c {collection} -out no {filename}'.format(
      cmd=SOLR_POST_COMMAND, collection=collection, filename=docFilename)
   subprocess.call(cmd, shell=True)
   
# .............................................................................
def _query(collection, qParams=None, fqParams=None,
            otherParams='wt=python&indent=true'):
   """
   @summary: Perform a query on a Solr index
   @param collection: The Solr collection (index / core) to query
   @param qParams: Parameters to include in the query section of the Solr query
   @param fqParams: Parameters to include in the filter section of the query
   @param otherParams: Other parameters to pass to Solr
   """
   queryParts = []
   if qParams:
      qParts = []
      for k, v in qParams:
         if v is not None:
            if isinstance(v, list):
               if len(v) > 1:
                  qParts.append('{}:({})'.format(k, ' '.join(v)))
               else:
                  qParts.append('{}:{}'.format(k, v[0]))
            else:
               qParts.append('{}:{}'.format(k, v))
      # If we have at least one query parameter
      if qParts:
         queryParts.append('q={}'.format('+AND+'.join(qParts)))
   
   if fqParams:
      fqParts = []
      for k, v in fqParams:
         if v is not None:
            if isinstance(v, list):
               if len(v) > 1:
                  fqParts.append('{}:({})'.format(k, ' '.join(v)))
               else:
                  fqParts.append('{}:{}'.format(k, v[0]))
            else:
               fqParts.append('{}:{}'.format(k, v))
      # If we have at least one filter parameter
      if fqParts:
         queryParts.append('fq={}'.format('+AND+'.join(fqParts)))
   
   if otherParams is not None:
      queryParts.append(otherParams)
   
   url = '{}{}/select?{}'.format(SOLR_SERVER, collection, '&'.join(queryParts))
   res = urllib2.urlopen(url)
   resp = res.read()
   
   return resp

# .............................................................................
def queryArchiveIndex(algorithmCode=None, bbox=None, gridSetId=None, 
                      modelScenarioCode=None, pointMax=None, pointMin=None, 
                      projectionScenarioCode=None, squid=None, taxKingdom=None, 
                      taxPhylum=None, taxClass=None, taxOrder=None, 
                      taxFamily=None, taxGenus=None, taxSpecies=None, 
                      userId=None):
   """
   @summary: Query the PAV archive Solr index
   
   """
   qParams = [
      (SOLR_FIELDS.ALGORITHM_CODE, algorithmCode),
      (SOLR_FIELDS.GRIDSET_ID, gridSetId),
      (SOLR_FIELDS.USER_ID, userId),
      (SOLR_FIELDS.MODEL_SCENARIO_CODE, modelScenarioCode),
      (SOLR_FIELDS.PROJ_SCENARIO_CODE, projectionScenarioCode),
      (SOLR_FIELDS.SQUID, squid),
      (SOLR_FIELDS.TAXON_KINGDOM, taxKingdom),
      (SOLR_FIELDS.TAXON_PHYLUM, taxPhylum),
      (SOLR_FIELDS.TAXON_CLASS, taxClass),
      (SOLR_FIELDS.TAXON_ORDER, taxOrder),
      (SOLR_FIELDS.TAXON_FAMILY, taxFamily),
      (SOLR_FIELDS.TAXON_GENUS, taxGenus),
      (SOLR_FIELDS.TAXON_SPECIES, taxSpecies),
   ]
   
   if pointMax is not None or pointMin is not None:
      pmax = pointMax
      pmin = pointMin
      
      if pointMax is None:
         pmax = '*'
      
      if pointMin is None:
         pmin = '*'
         
      qParams.append((SOLR_FIELDS.POINT_COUNT, '%5B{}%20TO%20{}%5D'.format(
                     pmin, pmax)))
   
   fqParams = []
   if bbox is not None:
      minx, miny, maxx, maxy = bbox.split(',')
      fqParams.append((SOLR_FIELDS.PRESENCE, '%5B{},{}%20{},{}%5D'.format(
                                                      miny, minx, maxy, maxx)))
   
   rDict = literal_eval(_query(SOLR_ARCHIVE_COLLECTION, qParams=qParams, 
                               fqParams=fqParams))
   return rDict['response']['docs']

# .............................................................................
def querySnippetIndex(ident1=None, provider=None, collection=None, 
                      catalogNumber=None, operation=None, afterTime=None,
                      beforeTime=None, ident2=None, url=None, who=None,
                      agent=None, why=None):
   """
   @summary: Query the snippet Solr index
   @param ident1: An identifier for the primary object (probably occurrence 
                     point)
   @param provider: The occurrence point provider
   @param collection: The collection the point belongs to
   @param catalogNumber: The catalog number of the occurrence point
   @param operation: A LmServer.common.lmconstants.SnippetOperations
   @param afterTime: Return hits after this time (MJD format)
   @param beforeTime: Return hits before this time (MJD format)
   @param ident2: A identifier for the secondary object (occurrence set or 
                     projection)
   @param url: A url for the resulting object
   @param who: Who initiated the action
   @param agent: The agent that initiated the action
   @param why: Why the action was initiated
   """
   qParams = [
      (SnippetFields.AGENT, agent),
      (SnippetFields.CATALOG_NUMBER, catalogNumber),
      (SnippetFields.COLLECTION, collection),
      (SnippetFields.IDENT_1, ident1),
      (SnippetFields.IDENT_2, ident2),
      (SnippetFields.OPERATION, operation),
      (SnippetFields.PROVIDER, provider),
      (SnippetFields.URL, url),
      (SnippetFields.WHO, who),
      (SnippetFields.WHY, why)
   ]
   
   fqParams = []
   if afterTime is not None or beforeTime is not None:
      if afterTime is not None:
         aTime = DateTimeFromMJD(afterTime).strftime('%Y-%m-%dT%H:%M:%SZ')
      else:
         aTime = '*'

      if beforeTime is not None:
         bTime = DateTimeFromMJD(beforeTime).strftime('%Y-%m-%dT%H:%M:%SZ')
      else:
         bTime = '*'
   
      fqParams.append((SnippetFields.OP_TIME, '%5B{}%20TO%20{}%5D'.format(aTime, 
                                                                      bTime)))
   
   
   rDict = literal_eval(_query(SOLR_SNIPPET_COLLECTION, qParams=qParams, 
                               fqParams=fqParams))
   return rDict['response']['docs']
