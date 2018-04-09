"""
@summary: Module containing functions for creating output package files
@author: CJ Grady
@version: 2.0
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
import cherrypy
from collections import defaultdict
import json
import os
from StringIO import StringIO
import zipfile

from LmCommon.common.lmconstants import LMFormat, MatrixType, JobStatus
from LmCommon.common.matrix import Matrix
from LmCommon.common.lmXml import tostring

from LmServer.common.log import LmPublicLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.gridset import Gridset

from LmWebServer.formatters.emlFormatter import makeEml
from LmWebServer.formatters.geoJsonFormatter import geoJsonify_flo


# .............................................................................
# TODO: Move to lmconstants
GRIDSET_DIR = 'gridset'
MATRIX_DIR = os.path.join(GRIDSET_DIR, 'matrix')
# TODO: Assmble from constants
STATIC_PACKAGE_DIR = '/opt/lifemapper/LmWebServer/assets/gridset_package'
DYN_PACKAGE_DIR = 'package'
   

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
def gridsetPackageFormatter(gsObj, includeCSV=False, includeSDM=False, 
                            stream=False):
   """
   @summary: Create a Gridset download package for the user to explore locally
   """
   if isinstance(gsObj, Gridset):
      
      # Get scribe and user id (used for creating lookup)
      scribe = BorgScribe(LmPublicLogger())
      userId = gsObj.getUserId()
      
      contentFLO = StringIO()
      with zipfile.ZipFile(contentFLO, mode='w', 
                    compression=zipfile.ZIP_DEFLATED, allowZip64=True) as zipF:
         for f_dir, _, fns in os.walk(STATIC_PACKAGE_DIR):
            for fn in fns:
               # Get relative and absolute paths for packaging
               a_path = os.path.join(f_dir, fn)
               r_path = a_path.replace(STATIC_PACKAGE_DIR, '')
               zipF.write(a_path, r_path)
         
         # Write gridset objects
         gsEml = tostring(makeEml(gsObj))
         zipF.writestr(os.path.join(GRIDSET_DIR, 'gridset_{}.eml'.format(
            gsObj.getId())), gsEml)
         
         # Write tree
         if gsObj.tree is not None:
            with open(gsObj.tree.getDLocation()) as treeIn:
               treeStr = treeIn.read()
               
               zipF.writestr(os.path.join(DYN_PACKAGE_DIR, 'tree.js'),
                             'var taxonTree = `{}`;'.format(treeStr))
               
               # TODO: Write tree?
               zipF.writestr(os.path.join(GRIDSET_DIR, 'tree.tre'), treeStr)
            treeStr = None
         
         # Matrices
         sg = gsObj.getShapegrid()
         matrices = gsObj.getMatrices()
         
         for mtx in matrices:
            if mtx.status == JobStatus.COMPLETE:
               mtxObj = Matrix.load(mtx.getDLocation())
               
               if mtx.matrixType in [MatrixType.PAM, MatrixType.ROLLING_PAM]:
                  
                  # Write SQUID lookup
                  squidLookupFn = os.path.join(DYN_PACKAGE_DIR, 'squidLookup.json')
                  zipF.writestr(squidLookupFn, 'var squidLookup =\n{}'.format(
                                                json.dumps(createHeaderLookup(
                                                      mtxObj.getColumnHeaders(), 
                                                      squids=True, 
                                                      scribe=scribe, 
                                                      userId=userId),
                                                   indent=3)))
                  
                  mtxStr = StringIO()
                  geoJsonify_flo(mtxStr, sg.getDLocation(), matrix=mtxObj, 
                                 mtxJoinAttrib=0, ident=0, 
                                 headerLookupFilename=squidLookupFn, 
                                 transform=mung)
                  mtxStr.seek(0)
                  
                  pamPkgFn = os.path.join(DYN_PACKAGE_DIR, 'pam.js')
                  zipF.writestr(pamPkgFn, "var pam = JSON.parse('{}');".format(
                                                               mtxStr.getvalue()))
                  # Save memory
                  mtxStr = None
                  
                  csvMtxFn = os.path.join(MATRIX_DIR, 'pam_{}{}'.format(
                                                mtx.getId(), LMFormat.CSV.ext))
                  
               elif mtx.matrixType == MatrixType.ANC_PAM:
                  
                  # Write node lookup
                  nodeLookupFn = os.path.join(DYN_PACKAGE_DIR, 'nodeLookup.js')
                  zipF.writestr(nodeLookupFn, 'var nodeLookup = \n{}'.format(
                                                json.dumps(createHeaderLookup(
                                                      mtxObj.getColumnHeaders()),
                                                   indent=3)))
   
                  mtxStr = StringIO()
                  geoJsonify_flo(mtxStr, sg.getDLocation(), matrix=mtxObj, 
                                 mtxJoinAttrib=0, ident=0, 
                                 headerLookupFilename=nodeLookupFn, 
                                 transform=mung)
                  mtxStr.seek(0)
                  
                  ancPamPkgFn = os.path.join(DYN_PACKAGE_DIR, 'ancPam.js')
                  zipF.writestr(ancPamPkgFn, "var ancPam = JSON.parse(`{});`".format(
                                                               mtxStr.getvalue()))
                  # Save memory
                  mtxStr = None
                  
                  csvMtxFn = os.path.join(MATRIX_DIR, 'ancPam_{}{}'.format(
                                                mtx.getId(), LMFormat.CSV.ext))
                  
   
               elif mtx.matrixType in [MatrixType.SITES_COV_OBSERVED, 
                                       MatrixType.SITES_COV_RANDOM, 
                                       MatrixType.SITES_OBSERVED, 
                                       MatrixType.SITES_RANDOM]:
   
                  if mtx.matrixType == MatrixType.SITES_COV_OBSERVED:
                     mtxName = 'sitesCovarianceObserved'
                  elif mtx.matrixType == MatrixType.SITES_COV_RANDOM:
                     mtxName = 'sitesCovarianceRandom'
                  elif mtx.matrixType == MatrixType.SITES_OBSERVED:
                     mtxName = 'sitesObserved'
                  else:
                     mtxName = 'sitesRandom'        
   
                  mtxStr = StringIO()
                  # TODO: Determine if we need to mung this data
                  geoJsonify_flo(mtxStr, sg.getDLocation(), matrix=mtxObj, 
                                 mtxJoinAttrib=0, ident=0)
                  mtxStr.seek(0)
                  
                  mtxPkgFn = os.path.join(DYN_PACKAGE_DIR, '{}.js'.format(
                                                                        mtxName))
                  zipF.writestr(mtxPkgFn, "var {} = JSON.parse(`{}`);".format(
                                                      mtxName, mtxStr.getvalue()))
                  # Save memory
                  mtxStr = None
                  
                  csvMtxFn = os.path.join(MATRIX_DIR, '{}_{}{}'.format(mtxName,
                                                mtx.getId(), LMFormat.CSV.ext))
                  
                  
               elif mtx.matrixType == MatrixType.MCPA_OUTPUTS:
                  csvMtxStr = StringIO()
                  mtxObj.writeCSV(csvMtxStr)
                  csvMtxStr.seek(0)
                  mcpaPkgFn = os.path.join(DYN_PACKAGE_DIR, 'mcpaMatrix.js')
                  zipF.writestr(mcpaPkgFn, 'var mcpaMatrix = `{}`;'.format(
                                                            csvMtxStr.getvalue()))
                  
                  csvMtxFn = os.path.join(MATRIX_DIR, 'mcpa_{}{}'.format(
                                                mtx.getId(), LMFormat.CSV.ext))
                  
               else:
                  csvMtxFn = os.path.join(MATRIX_DIR, os.path.splitext(
                        os.path.basename(mtx.getDLocation()))[0], 
                                        LMFormat.CSV.ext)
   
               # Write the Matrix CSV file if desired
               if includeCSV:
                  csvMtxStr = StringIO()
                  mtxObj.writeCSV(csvMtxStr)
                  csvMtxStr.seek(0)
                  zipF.writestr(csvMtxFn, csvMtxStr.getvalue())
         
         # TODO: Write SDMs
      contentFLO.seek(0)
   else:
      raise Exception, 'Only Gridsets can be formatted as package'

   # TODO: Use gridset name instead?
   packageFn = 'gridset-{}-package.zip'.format(gsObj.getId())
   cherrypy.response.headers['Content-Disposition'
                             ] = 'attachment; filename="{}"'.format(packageFn)
   cherrypy.response.headers['Content-Type'] = LMFormat.ZIP.getMimeType()

   # If we should stream the output, use the CherryPy file generator      
   if stream:
      return cherrypy.lib.file_generator(contentFLO)
   else:
      # Just return the content, but close the file
      cnt = contentFLO.read()
      contentFLO.close()
      return cnt

