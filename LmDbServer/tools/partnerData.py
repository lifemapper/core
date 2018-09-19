"""
@summary: Module containing functions for API Queries
@status: beta

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
import idigbio
import json
import os
import sys
import unicodecsv
import urllib2

from LmCommon.common.lmconstants import IDIGBIO_QUERY
from LmBackend.common.lmobj import LMError

DEV_SERVER = 'http://141.211.236.35:10999'
INDUCED_SUBTREE_BASE_URL = '{}/induced_subtree'.format(DEV_SERVER)
OTTIDS_FROM_GBIFIDS_URL = '{}/ottids_from_gbifids'.format(DEV_SERVER)

# .............................................................................
class LABEL_FORMAT(object):
   """
   @TODO: pull this from rpm of `ot_service_wrapper`
   @summary: This class represents label format constants that can be used 
                when calling the induced subtree function
   """
   NAME = 'name'
   ID = 'id'
   NAME_AND_ID = 'name_and_id'

# .............................................................................
def get_ottids_from_gbifids(gbif_ids):
   """
   @summary: Calls the Open Tree 'ottids_from_gbifids' service to retrieve a 
                mapping dictionary from the Open Tree service where each key is 
                one of the provided GBIF identifiers and the value is the 
                corresponding OpenTree id.
   @note: Any GBIF ID that was not found will have a value of None
   @param gbif_ids: A list of GBIF identifiers.  They will be converted to
                       integers in the request.
   """
   # Ids need to be integers
   processed_ids = [int(gid) for gid in gbif_ids]
      
   request_body = {
      "gbif_ids" : processed_ids
   }
   
   headers = {
      'Content-Type' : 'application/json'
   }
   req = urllib2.Request(OTTIDS_FROM_GBIFIDS_URL, 
                         data=json.dumps(request_body), headers=headers)
   
   resp = json.load(urllib2.urlopen(req))
   unmatchedIds = resp['unmatched_gbif_ids']
   
   id_map = resp["gbif_ott_id_map"]
   
   for gid in unmatchedIds:
      id_map[gid] = None
   
   return id_map

# .............................................................................
def induced_subtree(ott_ids, label_format=LABEL_FORMAT.NAME):
   """
   @summary: Calls the Open Tree 'induced_subtree' service to retrieve a tree,
                in Newick format, containing the nodes represented by the 
                provided Open Tree IDs
   @param ott_ids: A list of Open Tree IDs.  These will be converted to into
                      integers
   @param label_format: The label string format to use when creating the tree
                           on the server 
   """
   # Ids need to be integers
   processed_ids = [int(ottid) for ottid in ott_ids]
   request_body = {
      "ott_ids" : processed_ids,
      "label_format" : label_format
   }
   
   headers = {
      'Content-Type' : 'application/json'
   }
   req = urllib2.Request(INDUCED_SUBTREE_BASE_URL, 
                         data=json.dumps(request_body), headers=headers)
   
   resp_str = urllib2.urlopen(req).read()
   return json.loads(resp_str)

# .............................................................................
class PartnerQuery(object):
   """
   Class to query iDigBio for species data and OTOL for phylogenetic trees 
   using 'ACCEPTED' TaxonIDs from the GBIF Backbone Taxonomy
   """
   def __init__(self):
      """
      @summary Constructor for the APIQuery class
      """
      pass

   # .............................................................................
   def getCSVWriter(self, datafile, delimiter, doAppend=True):
      '''
      @summary: Get a CSV writer that can handle encoding
      '''
      unicodecsv.field_size_limit(sys.maxsize)
      if doAppend:
         mode = 'ab'
      else:
         mode = 'wb'
         
      try:
         f = open(datafile, mode) 
         writer = unicodecsv.writer(f, delimiter=delimiter, encoding='utf-8')
   
      except Exception, e:
         raise Exception('Failed to read or open {}, ({})'
                         .format(datafile, str(e)))
      return writer, f
   
   
   # .............................................................................
   def getIdigbioRecords(self, gbifTaxonId, fields, writer):
      """
      @param gbifTaxonIds: one GBIF TaxonId or a list
      """
      api = idigbio.json()
      limit = 100
      offset = 0
      total = 0
      while offset <= total:
         try:
            output = api.search_records(rq={'taxonid':str(gbifTaxonId)}, 
                                        limit=limit, offset=offset)
         except:
            print 'Failed on {}'.format(gbifTaxonId)
         else:
            total = output['itemCount']
            items = output['items']
            total += len(items)
            print("Retrieved {}/{} records for gbif taxonid {}"
                  .format(len(items), total, gbifTaxonId))
            for itm in items:
               vals = []
               for fldname in fields:
                  try:
                     vals.append(itm['indexTerms']['indexData'][fldname])
                  except:
                     try:
                        vals.append(itm['indexTerms'][fldname])
                     except:
                        vals.append('')
                     
               writer.writerow(vals)
            offset += limit
                     
   
   # .............................................................................
   def assembleIdigbioData(self, gbifTaxonIds, outfname):
      try:
         list(gbifTaxonIds)
      except:
         gbifTaxonIds = [gbifTaxonIds]
         
      if os.path.exists(outfname):
         raise LMError('Output file {} already exists'.format(outfname))
      
      fields = IDIGBIO_QUERY.RETURN_FIELDS.keys()
      writer, f = self._getCSVWriter(outfname, '\t', doAppend=False)
      writer.writerow(fields)
      for gid in gbifTaxonIds:
         self.getIdigbioRecords(gid, fields, writer)
      
            
   # .............................................................................
   def assembleOTOLData(self, gbifTaxonIds):
      gbifOTT = get_ottids_from_gbifids(gbifTaxonIds)
      return gbifOTT
            

  
# .............................................................................
def testBoth():
   gbifids = ['3752543', '3753319', '3032690', '3752610', '3755291', '3754671', 
              '8109411', '3753512', '3032647', '3032649', '3032648', '8365087', 
              '4926214', '7516328', '7588669', '7554971', '3754743', '3754395', 
              '3032652', '3032653', '3032654', '3032655', '3032656', '3032658', 
              '3032662', '7551031', '8280496', '7462054', '3032651', '3755546', 
              '3032668', '3032665', '3032664', '3032667', '3032666', '3032661', 
              '3032660', '3754294', '3032687', '3032686', '3032681', '3032680', 
              '3032689', '3032688', '3032678', '3032679', '3032672', '3032673', 
              '3032670', '3032671', '3032676', '3032674', '3032675']
   pquery = PartnerQuery()
   pquery.assembleIdigbioData(gbifids, 'testIdigbioData.csv')
   gbifOTT = pquery.assembleOTOLData(gbifids)
            
            
         
# .............................................................................
# .............................................................................
if __name__ == '__main__':
   testBoth()
   pass

         
"""
import idigbio
import json
import os
import sys
import unicodecsv
import urllib2

from LmCommon.common.lmconstants import IDIGBIO_QUERY
from LmBackend.common.lmobj import LMError

DEV_SERVER = 'http://141.211.236.35:10999'
INDUCED_SUBTREE_BASE_URL = '{}/induced_subtree'.format(DEV_SERVER)
OTTIDS_FROM_GBIFIDS_URL = '{}/ottids_from_gbifids'.format(DEV_SERVER)


gbifids = ['3752543', '3753319', '3032690', '3752610', '3755291', '3754671', 
              '8109411', '3753512', '3032647', '3032649', '3032648', '8365087', 
              '4926214', '7516328', '7588669', '7554971', '3754743', '3754395', 
              '3032652', '3032653', '3032654', '3032655', '3032656', '3032658', 
              '3032662', '7551031', '8280496', '7462054', '3032651', '3755546', 
              '3032668', '3032665', '3032664', '3032667', '3032666', '3032661', 
              '3032660', '3754294', '3032687', '3032686', '3032681', '3032680', 
              '3032689', '3032688', '3032678', '3032679', '3032672', '3032673', 
              '3032670', '3032671', '3032676', '3032674', '3032675']
              
              

def getCSVWriter(datafile, delimiter, doAppend=True):
   unicodecsv.field_size_limit(sys.maxsize)
   if doAppend:
      mode = 'ab'
   else:
      mode = 'wb'      
   try:
      f = open(datafile, mode) 
      writer = unicodecsv.writer(f, delimiter=delimiter, encoding='utf-8')
   except Exception, e:
      raise Exception('Failed to read or open {}, ({})'
                      .format(datafile, str(e)))
   return writer, f


# .............................................................................
def getIdigbioRecords(gbifTaxonId, fields, writer):
   api = idigbio.json()
   limit = 100
   offset = 0
   total = 0
   while offset <= total:
      try:
         output = api.search_records(rq={'taxonid':str(gbifTaxonId)}, 
                                     limit=limit, offset=offset)
      except:
         print 'Failed on {}'.format(gbifTaxonId)
      else:
         total = output['itemCount']
         items = output['items']
         total += len(items)
         print("Retrieved {}/{} records for gbif taxonid {}"
               .format(len(items), total, gbifTaxonId))
         for itm in items:
            vals = []
            for fldname in fields:
               try:
                  vals.append(itm['indexTerms']['indexData'][fldname])
               except:
                  try:
                     vals.append(itm['indexTerms'][fldname])
                  except:
                     vals.append('')
                  
            writer.writerow(vals)
         offset += limit
                  

# .............................................................................
def assembleIdigbioData(gbifTaxonIds, outfname):
   try:
      list(gbifTaxonIds)
   except:
      gbifTaxonIds = [gbifTaxonIds]
   if os.path.exists(outfname):
      raise LMError('Output file {} already exists'.format(outfname))
   fields = IDIGBIO_QUERY.RETURN_FIELDS.keys()
   writer, f = getCSVWriter(outfname, '\t', doAppend=False)
   writer.writerow(fields)
   for gid in gbifTaxonIds:
      getIdigbioRecords(gid, fields, writer)
"""