"""
@summary: Create a package to be returned to a client based on a gridset
@author: CJ Grady
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
@todo: Move this as necessary
@todo: Probably want to split of EML generating code to separate module(s)
"""
import argparse
from collections import defaultdict
import json
import os
import zipfile

from LmCommon.common.lmconstants import LMFormat, MatrixType
from LmCommon.common.lmXml import tostring
from LmCommon.common.matrix import Matrix

from LmServer.common.lmconstants import TEMP_PATH
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

from LmWebServer.formatters.emlFormatter import makeEml
from LmWebServer.formatters.geoJsonFormatter import geoJsonify_flo

# .............................................................................
def createHeaderLookup(headers, squids=False, scribe=None, userId=None):
   """
   @summary: Generate a header lookup to be included in the package metadata
   """
   def getHeaderDict(header, idx):
      return {
         'header' : header,
         'index' : idx
      }
      
   def getSquidHeaderDict(header, idx, scribe, userId):
      taxon = scribe.getTaxon(squid=header, userId=userId)
      ret = getHeaderDict(header, idx)
      
      for attrib, key in [('scientificName', 'scientific_name'),
                          ('canonicalName', 'canonical_name'),
                          ('rank', 'taxon_rank'),
                          ('kingdom', 'taxon_kingdom'),
                          ('phylum', 'taxon_phylum'),
                          ('txClass', 'taxon_class'),
                          ('txOrder', 'taxon_order'),
                          ('family', 'taxon_family'),
                          ('genus', 'taxon_genus')
                         ]:
         val = getattr(taxon, attrib)
         if val is not None:
            ret[key] = val
      return ret
   
      
   if squids and scribe and userId:
      return [getSquidHeaderDict(
                  headers[i], i, scribe, userId) for i in xrange(len(headers))]
   else:
      return [getHeaderDict(headers[i], i) for i in xrange(len(headers))]

# .............................................................................
def mung(data):
   """
   @summary: Replace a list of values with a map from the non-zero values to
                the indexes at which they occur
   """
   munged = defaultdict(list)
   for i, datum in enumerate(data):
      if datum != 0:
         munged[datum].append(i)
   return munged

# .............................................................................
def assemble_package_for_gridset(gridset, outfile, scribe, userId):
   """
   @summary: Creates an output zip file from the gridset
   """
   print('Assembling package: {}'.format(outfile))
   print('Creating EML')
   gsEml = tostring(makeEml(gridset))
   with zipfile.ZipFile(outfile, 
                        mode='w', 
                        compression=zipfile.ZIP_DEFLATED,
                        allowZip64=True) as outZip:
      print('Write out EML')
      outZip.writestr('gridset_{}.eml'.format(gridset.getId()), gsEml)
      print('Write tree')
      outZip.write(gridset.tree.getDLocation(), 
                   os.path.basename(gridset.tree.getDLocation()))
      print('Getting shapegrid')
      sg = gridset.getShapegrid()
      matrices = gridset.getMatrices()
      i = 0
      print('{} matrices'.format(len(matrices)))
      for mtx in matrices:
         i += 1
         print('Matrix: ({} of {}) {}'.format(i, len(matrices), 
                                              mtx.getDLocation()))
         print(' - Loading matrix')
         mtxObj = Matrix.load(mtx.getDLocation())
         print(' - Loaded')

         # Need to get geojson where we can
         if mtx.matrixType in [MatrixType.PAM, MatrixType.ROLLING_PAM]:
            mtxFn = '{}{}'.format(
               os.path.splitext(
                  os.path.basename(mtx.getDLocation()))[0], 
                                  LMFormat.GEO_JSON.ext)
            
            print(' - Creating SQUID lookup')
            hlfn = 'squidLookup.json'
            outZip.writestr(hlfn, json.dumps(createHeaderLookup(
                                                mtxObj.getColumnHeaders(), 
                                                squids=True, 
                                                scribe=scribe, userId=userId),
                                             indent=3))
            
            # Make a temporary file
            tempFn = os.path.join(TEMP_PATH, mtxFn)
            print(' - Temporary file name: {}'.format(tempFn))
            with open(tempFn, 'w') as tempF:
               print(' - Getting GeoJSON')
               geoJsonify_flo(tempF, sg.getDLocation(), matrix=mtxObj, 
                           mtxJoinAttrib=0, ident=0, headerLookupFilename=hlfn,
                           transform=mung)
            
         elif mtx.matrixType == MatrixType.ANC_PAM:
            mtxFn = '{}{}'.format(
               os.path.splitext(
                  os.path.basename(mtx.getDLocation()))[0], 
                                  LMFormat.GEO_JSON.ext)
            
            print(' - Creating node lookup')
            hlfn = 'nodeLookup.json'
            outZip.writestr(hlfn, json.dumps(createHeaderLookup(
                                                   mtxObj.getColumnHeaders()),
                                             indent=3))
            
            # Make a temporary file
            tempFn = os.path.join(TEMP_PATH, mtxFn)
            print(' - Temporary file name: {}'.format(tempFn))
            with open(tempFn, 'w') as tempF:
               print(' - Getting GeoJSON')
               geoJsonify_flo(tempF, sg.getDLocation(), matrix=mtxObj, 
                           mtxJoinAttrib=0, ident=0, headerLookupFilename=hlfn,
                           transform=mung)
            
         elif mtx.matrixType in [MatrixType.SITES_COV_OBSERVED, 
                            MatrixType.SITES_COV_RANDOM, 
                            MatrixType.SITES_OBSERVED, MatrixType.SITES_RANDOM]:
            mtxFn = '{}{}'.format(
               os.path.splitext(
                  os.path.basename(mtx.getDLocation()))[0], 
                                  LMFormat.GEO_JSON.ext)
            
            # Make a temporary file
            tempFn = os.path.join(TEMP_PATH, mtxFn)
            print(' - Temporary file name: {}'.format(tempFn))
            with open(tempFn, 'w') as tempF:
               print(' - Getting GeoJSON')
               geoJsonify_flo(tempF, sg.getDLocation(), matrix=mtxObj, 
                              mtxJoinAttrib=0, ident=0)
         else:
            print(' - Write non Geo-JSON matrix')
            mtxFn = '{}{}'.format(
               os.path.splitext(
                  os.path.basename(mtx.getDLocation()))[0], 
                                  LMFormat.CSV.ext)
            # Make a temporary file
            tempFn = os.path.join(TEMP_PATH, mtxFn)
            print(' - Temporary file name: {}'.format(tempFn))
            with open(tempFn, 'w') as tempF:
               print(' - Getting CSV')
               mtxObj.writeCSV(tempF)

         print(' - Zipping {}'.format(tempFn))
         outZip.write(tempFn, mtxFn)
            
         print(' - Delete temp file')
         os.remove(tempFn)

# ..........................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
                description='This script creates a package of gridset outputs')
   
   parser.add_argument('gsId', type=int, help='The gridset id number')
   parser.add_argument('out_file', type=str, 
                       help='The file location to write the output package')
   
   args = parser.parse_args()
   
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   gs = scribe.getGridset(gridsetId=args.gsId, fillMatrices=True)
   
   assemble_package_for_gridset(gs, args.out_file, scribe, gs.getUserId())

   scribe.closeConnections()
