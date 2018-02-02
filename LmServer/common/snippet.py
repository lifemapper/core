"""
@summary: This module contains the snippet shooter class and related classes
             used for sending snippets to a server
@author: CJ Grady
@version: 1.0
@status: alpha
@license: gpl2
@copyright: Copyright (C) 2018, University of Kansas Center for Research

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
from mx.DateTime import DateTimeFromMJD, gmt
import os
from random import randint

from LmBackend.common.lmobj import LMError, LMObject
from LmServer.common.lmconstants import (SOLR_SERVER, SOLR_SNIPPET_COLLECTION,
   UPLOAD_PATH, SnippetFields)
from LmServer.common.solr import buildSolrDocument, postSolrDocument

# =============================================================================
class SnippetShooter(LMObject):
   """
   @summary: This class creates snippets and "shoots" them to an awaiting 
                snippet server
   """
   # ............................
   def __init__(self, snippetServer=SOLR_SERVER, 
                snippetCollection=SOLR_SNIPPET_COLLECTION):
      """
      @summary: Constructor
      @param snippetServer: A snippet server that will accept these snippets.
                               Currently not used
      @param snippetCollection: A solr collection for these snippets
      """
      self.server = snippetServer
      self.collection = snippetCollection
      
      self.snippets = []
   
   # ............................
   def addSnippets(self, obj1, operation, opTime=None, obj2ident=None, 
                   url=None, who=None, agent=None, why=None):
      """
      @summary: Adds snippets for posting to the snippet shooter's list
      @param obj1: Required, this will start out as only occurrence sets but 
             should be expanded later to include other object types
      @param operation: See LmServer.common.lmconstants.SnippetOperations 
             for available operations
      @param opTime: (optional) MJD time that this operation took place
      @param obj2ident: (optional) Identifier of the secondary object
      @param url: (optional) A URL related to this snippet
      @param who: (optional) A string representing who initiated this action
      @param agent: (optional) The agent that this action was initiated through
             examples could be LmCompute, web client, or similar
      @param why: (optional) Why this action was initiated (archive, user 
             request, etc) 
      """
#       if not isinstance(obj1, OccurrenceLayer):
      try:
         obj1.getScientificName()
      except:
         raise LMError('Do no know how to create snippets for: {}'.format(
            str(obj1.__class__)))
      else:
         if len(obj1.features) == 0:
            # Try to read the data if no features
            obj1.readData(doReadData=True)

         if len(obj1.features) == 0:
            raise LMError(
               'Occurrence set must have features to create snippets for')
         
         if opTime is None:
            opTime = gmt().mjd
         opTimeStr = DateTimeFromMJD(opTime).strftime('%Y-%m-%dT%H:%M:%SZ')
         
         for feat in obj1.features:
            try:
               catNum = feat.catnum
               provider = feat.provider
               col = feat.coll_code
               ident = '{}:{}:{}'.format(provider, col, catNum)
               opId = '{}:{}:{}'.format(ident, operation, opTimeStr)
               self.snippets.append([
                  (SnippetFields.AGENT, agent),
                  (SnippetFields.CATALOG_NUMBER, catNum),
                  (SnippetFields.COLLECTION, col),
                  (SnippetFields.ID, opId),
                  (SnippetFields.IDENT_1, ident),
                  (SnippetFields.IDENT_2, obj2ident),
                  (SnippetFields.OP_TIME, opTimeStr),
                  (SnippetFields.OPERATION, operation),
                  (SnippetFields.PROVIDER, provider),
                  (SnippetFields.URL, url),
                  (SnippetFields.WHO, who),
                  (SnippetFields.WHY, why),
               ])
            except:
               # If we don't know what to create a snippet for, skip
               pass
   
   # ............................
   def shootSnippets(self, solrPostFilename=None):
      """
      @summary: Shoots the snippets to the snippet collection
      @param solrPostFilename: If provided, write out the Solr post document 
                                  here
      """
      # Build the Solr document
      solrPostStr = buildSolrDocument(self.snippets)
      
      deletePostFilename = False
      # Write to temp file
      if solrPostFilename is None:
         #TODO: Fill in
         solrPostFilename = os.path.join(UPLOAD_PATH, 'snippetPost-{}'.format(randint(0, 10000)))
         deletePostFilename = True

      with open(solrPostFilename, 'w') as outF:
         outF.write(solrPostStr)
      
      # Shoot snippets
      if len(self.snippets) > 0:
         postSolrDocument(self.collection, solrPostFilename)

      if deletePostFilename:
         os.remove(solrPostFilename)
         
      # Reset snippet list
      self.snippets = []
   
