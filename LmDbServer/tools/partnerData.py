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
import os
import sys
import unicodecsv


from LmCommon.common.lmconstants import (IDIGBIO, IDIGBIO_QUERY)
from LmBackend.common.lmobj import LMError

# .............................................................................
def getCSVWriter(datafile, delimiter, doAppend=True):
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
def getIdigbioRecords(gbifTaxonId, fields, writer):
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
      
            
            
         
# .............................................................................
def testIdigbioClient():
   gbifids = [2435099, 1000329, 1000410, 1000431, 1000432, 1000443, 1000447, 1000454, 
              1000461, 1000464, 1000483, 1000484, 1000488, 1000511, 1000515, 
              1000519, 1000525, 1000541, 1000543, 1000546, 1000575]    
   fields = IDIGBIO_QUERY.RETURN_FIELDS.keys()
   api = idigbio.json()
   for gid in gbifids:
      # direct query
      try:
         output = api.search_records(rq={'taxonid':str(gid)}, 
                                     limit=100, offset=0)
      except:
         print 'Failed on {}'.format(gid)
      else:
         items = output['items']
         print("Retrieved {}/{} records for gbif taxonid {}"
               .format(len(items), output['itemCount'], gid))
         if gid == gbifids[0] and len(items) > 0:
            itm = items[0]
            for fldname in itm['indexTerms']['indexData']:
               if fldname in fields:
                  print fldname
            
            
         
# .............................................................................
# .............................................................................
if __name__ == '__main__':
   testIdigbioClient()
   pass

         
"""
import json
import requests
from types import (BooleanType, DictionaryType, TupleType)
import urllib
# import xml.etree.ElementTree as ET

from LmCommon.common.lmconstants import (BISON, BISON_QUERY, GBIF, ITIS, 
                                         IDIGBIO, IDIGBIO_QUERY, 
                                         URL_ESCAPES, HTTPStatus, DWCNames)
from LmCommon.common.lmXml import *
from LmCommon.common.apiquery import *

gbifids = [2435099, 1000329, 1000410, 1000431, 1000432, 1000443, 1000447, 1000454, 
           1000461, 1000464, 1000483, 1000484, 1000488, 1000511, 1000515, 
           1000519, 1000525, 1000541, 1000543, 1000546, 1000575]    
fields = IDIGBIO_QUERY.RETURN_FIELDS.keys()
api = idigbio.json()
for gid in gbifids:
   try:
      output = api.search_records(rq={'taxonid':str(gid)}, 
                                  limit=100, offset=0)
   except:
      print 'Failed on {}'.format(gid)
   else:
      items = output['items']
      print("Retrieved {}/{} records for gbif taxonid {}"
            .format(len(items), output['itemCount'], gid))
            
if len(items) > 0:
   itm = items[0]
   for fldname in itm['indexTerms']['indexData']:
      if fldname in fields:
         print 'x', fldname
      else:
         print ' ', fldname

if len(items) > 0:
   itm = items[0]
   for fld in fields:
      try:
         val = itm['indexTerms']['indexData'][fld]
      except:
         val = ''
"""