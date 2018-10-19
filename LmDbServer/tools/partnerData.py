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
from LmBackend.common.lmobj import LMError
try:
   from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except:
   OFTInteger = 0 
   OFTReal = 2 
   OFTString = 4
   OFTBinary = 8

import idigbio
import json
import os
import sys
import unicodecsv
import urllib2

from LmCommon.common.lmconstants import (IDIGBIO_QUERY, IDIGBIO, DWC_QUALIFIER, 
                                         DWCNames)
from LmCommon.common.occparse import OccDataParser
from LmServer.common.log import ScriptLogger

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
   @return: a dictionary with each key is an 'ACCEPTED' TaxonId from the GBIF 
            Backbone Taxonomy and the value is the corresponding OpenTree id.
   """
   if not(isinstance(gbif_ids, list)):
      gbif_ids = [gbif_ids]
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
   def __init__(self, logger=None):
      """
      @summary Constructor for the PartnerQuery class
      """
      self.name = self.__class__.__name__.lower()
      if logger is None:
         logger = ScriptLogger(self.name)
      self.log = logger
      unicodecsv.field_size_limit(sys.maxsize)
      self.encoding = 'utf-8'
      self.delimiter = '\t'

   # .............................................................................
   def _getCSVWriter(self, datafile, doAppend=True):
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
         writer = unicodecsv.writer(f, delimiter=self.delimiter, 
                                    encoding=self.encoding)
   
      except Exception, e:
         raise Exception('Failed to read or open {}, ({})'
                         .format(datafile, str(e)))
      return writer, f

   # .............................................................................
   def _convertType(self, ogrtype):
      if ogrtype == OFTInteger:
         return 'int'
      elif ogrtype == OFTString:
         return 'str'
      elif ogrtype == OFTReal:
         return 'float'
      else:
         raise LMError('Unknown field type {}'.format(ogrtype))
   
   # .............................................................................
   def writeIdigbioMetadata(self, origFldnames, metaFname):
      newMeta = {}
      for colIdx in range(len(origFldnames)):
         ofname = origFldnames[colIdx]
         (shortname, ogrtype) = IDIGBIO_QUERY.RETURN_FIELDS[ofname]
         valdict = {'name': shortname, 
                    'type': self._convertType(ogrtype)}
         if ofname == IDIGBIO.QUALIFIER + IDIGBIO.ID_FIELD:
            valdict['role'] = 'uniqueid'
         elif ofname == IDIGBIO.GBIFID_FIELD:
            valdict['role'] = 'groupby'
         elif ofname == DWC_QUALIFIER + DWCNames.DECIMAL_LONGITUDE['FULL']:
            valdict['role'] = 'longitude'
         elif ofname == DWC_QUALIFIER + DWCNames.DECIMAL_LATITUDE['FULL']:
            valdict['role'] = 'latitude'
         elif ofname == DWC_QUALIFIER + DWCNames.SCIENTIFIC_NAME['FULL']:            
            valdict['role'] = 'taxaname'
         newMeta[str(colIdx)] = valdict
      with open(metaFname, 'w') as outf:
         json.dump(newMeta, outf)
      return newMeta
   
   # .............................................................................
   def getIdigbioRecords(self, gbifTaxonId, fields, writer):
      """
      @param gbifTaxonIds: one GBIF TaxonId or a list
      """
      api = idigbio.json()
      limit = 100
      offset = 0
      currcount = 0
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
            currcount += len(items)
#             print("  Retrieved {}/{} records for gbif taxonid {}"
#                   .format(len(items), total, gbifTaxonId))
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
      print('Retrieved {} of {} reported records for {}'.format(currcount, total, gbifTaxonId))
      return currcount
   
   # .............................................................................
   def assembleIdigbioData(self, gbifTaxonIds, ptFname, metaFname):      
      if not(isinstance(gbifTaxonIds, list)):
         gbifTaxonIds = [gbifTaxonIds]
         
      for fname in (ptFname, metaFname):
         if os.path.exists(fname):
            print('Deleting existing file {} ...'.format(fname))
            os.remove(fname)
         
      summary = {}
      writer, f = self._getCSVWriter(ptFname, doAppend=False)
      
      # Make sure metadata reflects data column order 
      # by pulling and using same fieldnames  
      origFldnames = IDIGBIO_QUERY.RETURN_FIELDS.keys()
      origFldnames.sort()

      # do not write header, put column indices in metadata
#       writer.writerow(origFldnames)
      meta = self.writeIdigbioMetadata(origFldnames, metaFname)
      
      for gid in gbifTaxonIds:
         ptCount = self.getIdigbioRecords(gid, origFldnames, writer)
         summary[gid] = ptCount
      return summary, meta
   
   # .............................................................................
   def summarizeIdigbioData(self, ptFname, metaFname):
      summary = {}
      if not(os.path.exists(ptFname)):
         print ('Point data {} does not exist')
      elif not(os.path.exists(metaFname)):
         print ('Metadata {} does not exist')
      else:
         occParser = OccDataParser(self.log, ptFname, metaFname, 
                                        delimiter=self.delimiter,
                                        pullChunks=True)
         occParser.initializeMe()       
         summary = occParser.readAllChunks()

#       fieldNames = self.occParser.header
      return summary
            
   # .............................................................................
   def assembleOTOLData(self, gbifTaxonIds):
      gbifOTT = get_ottids_from_gbifids(gbifTaxonIds)
      tree = induced_subtree(gbifOTT)
      return tree
            

  
# .............................................................................
def testBoth(dataname):
   ptFname = dataname + '.csv'
   metaFname = dataname + '.json'
   gbifids = ['3752543', '3753319', '3032690', '3752610', '3755291', '3754671', 
              '8109411', '3753512', '3032647', '3032649', '3032648', '8365087', 
              '4926214', '7516328', '7588669', '7554971', '3754743', '3754395', 
              '3032652', '3032653', '3032654', '3032655', '3032656', '3032658', 
              '3032662', '7551031', '8280496', '7462054', '3032651', '3755546', 
              '3032668', '3032665', '3032664', '3032667', '3032666', '3032661', 
              '3032660', '3754294', '3032687', '3032686', '3032681', '3032680', 
              '3032689', '3032688', '3032678', '3032679', '3032672', '3032673', 
              '3032670', '3032671', '3032676', '3032674', '3032675']
   iquery = PartnerQuery()
   if os.path.exists(ptFname) and os.path.exists(metaFname):
      summary = iquery.summarizeIdigbioData(ptFname, metaFname)
      for gbifid, (name, total) in summary.iteritems():
         print ('Found gbifid {} with name {} and {} records'.format(gbifid, name, total))
   else:
      iquery.assembleIdigbioData(gbifids, ptFname, metaFname)
   tree = iquery.assembleOTOLData(gbifids)
   print ('Now what?')
            
            
         
# .............................................................................
# .............................................................................
if __name__ == '__main__':
   ptdataname = 'testIdigbioData'
   testBoth(ptdataname)
   pass

         
"""
from LmBackend.common.lmobj import LMError
try:
   from osgeo.ogr import OFTInteger, OFTReal, OFTString, OFTBinary
except:
   OFTInteger = 0 
   OFTReal = 2 
   OFTString = 4
   OFTBinary = 8

import idigbio
import json
import os
import sys
import unicodecsv
import urllib2

from LmCommon.common.lmconstants import (IDIGBIO_QUERY, IDIGBIO, DWC_QUALIFIER, 
                                         DWCNames)
from LmCommon.common.occparse import OccDataParser
from LmServer.common.log import ScriptLogger
from LmDbServer.tools.partnerData import PartnerQuery

DEV_SERVER = 'http://141.211.236.35:10999'
INDUCED_SUBTREE_BASE_URL = '{}/induced_subtree'.format(DEV_SERVER)
OTTIDS_FROM_GBIFIDS_URL = '{}/ottids_from_gbifids'.format(DEV_SERVER)
logger = ScriptLogger('partnerData.test')
delimiter = '\t'
dataname  = '/tmp/idigTest'

ptFname = dataname + '.csv'
metaFname = dataname + '.json'
gbifids = ['3752543', '3753319', '3032690', '3752610', '3755291', '3754671', 
           '8109411', '3753512', '3032647', '3032649', '3032648', '8365087', 
           '4926214', '7516328', '7588669', '7554971', '3754743', '3754395', 
           '3032652', '3032653', '3032654', '3032655', '3032656', '3032658', 
           '3032662', '7551031', '8280496', '7462054', '3032651', '3755546', 
           '3032668', '3032665', '3032664', '3032667', '3032666', '3032661', 
           '3032660', '3754294', '3032687', '3032686', '3032681', '3032680', 
           '3032689', '3032688', '3032678', '3032679', '3032672', '3032673', 
           '3032670', '3032671', '3032676', '3032674', '3032675']
iquery = PartnerQuery()
if os.path.exists(ptFname) and os.path.exists(metaFname):
   iquery.summarizeIdigbioData(ptFname, metaFname)
else:
   iquery.assembleIdigbioData(gbifids, ptFname, metaFname)
tree = iquery.assembleOTOLData(gbifids)
print ('Now what?')


self = OccDataParser(logger, ptFname, jsonFname, 
                               delimiter=delimiter,
                               pullChunks=True)
self.initializeMe()       
sm = self.readAllChunks()

op2 = OccDataParser(logger, ptFname, metaFname, 
                               delimiter=delimiter,
                               pullChunks=True)
op2.initializeMe()       
sm2 = op2.readAllChunks()

"""