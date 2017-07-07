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
import subprocess
import urllib2

from LmServer.common.lmconstants import SOLR_POST_COMMAND, SOLR_SERVER,\
   SOLR_FIELDS, SOLR_ARCHIVE_COLLECTION
from ast import literal_eval

# .............................................................................
def buildSolrDocument(fieldPairs):
   """
   @summary: Builds a document for a Solr POST from the key, value pairs
   @param fieldPairs: A list of [field name, value] pairs
   """
   if not fieldPairs:
      raise Exception, "Must provide at least one pair for Solr POST"
   
   docLines = ['<add>']
   docLines.append('   <doc>')
   for fName, fVal in fieldPairs:
      # Only add the field if the value is not None
      if fVal is not None: 
         docLines.append('      <field name="{}">{}</field>'.format(fName, 
                                                                    fVal))
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
