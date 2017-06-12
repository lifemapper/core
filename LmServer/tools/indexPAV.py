"""
@summary: This script inserts a PAV into the Solr index
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
#TODO: Keys for index names

import argparse
import os
import subprocess
import tempfile

# TODO: Different logger
from LmCommon.common.matrix import Matrix
from LmCommon.compression.binaryList import compress
from LmServer.common.lmconstants import (SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS, 
                                         SOLR_POST_COMMAND)
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
def getPostDocument(pav, prj, occ, pam, pavFname):
   """
   @summary: Create the Solr document to be posted
   @param pav: A PAV matrix column object
   @param prj: A SDM Projection object
   @param occ: An occurrence layer object
   @return: A string that can be posted to the Solr index
   """
   sg = pav.shapegrid
   mdlScn = prj.modelScenario
   prjScn = prj.projScenario
   pavMtx = Matrix.load(pavFname)
   
   fields = [
      (SOLR_FIELDS.ID, pav.getId()),
      (SOLR_FIELDS.USER_ID, pav.getUserId()),
      (SOLR_FIELDS.DISPLAY_NAME, occ.displayName),
      (SOLR_FIELDS.SQUID, pav.squid),
      #(SOLR_FIELDS.TAXON_KINGDOM, taxKingdom),
      #(SOLR_FIELDS.TAXON_PHYLUM, taxPhylum),
      #(SOLR_FIELDS.TAXON_CLASS, taxClass),
      #(SOLR_FIELDS.TAXON_ORDER, taxOrder),
      #(SOLR_FIELDS.TAXON_FAMILY, taxFamily),
      #(SOLR_FIELDS.TAXON_GENUS, taxGenus),
      #(SOLR_FIELDS.TAXON_SPECIES, taxSpecies),
      (SOLR_FIELDS.ALGORITHM_CODE, prj.algorithmCode),
      (SOLR_FIELDS.ALGORITHM_PARAMETERS, prj.dumpAlgorithmParametersAsString()),
      (SOLR_FIELDS.POINT_COUNT, occ.queryCount),
      (SOLR_FIELDS.OCCURRENCE_ID, occ.getId()),
      (SOLR_FIELDS.OCCURRENCE_DATA_URL, occ.getDataUrl()),
      (SOLR_FIELDS.OCCURRENCE_META_URL, occ.metadataUrl),
      (SOLR_FIELDS.OCCURRENCE_MOD_TIME, occ.modTime), # May need to convert
      (SOLR_FIELDS.MODEL_SCENARIO_CODE, mdlScn.code),
      (SOLR_FIELDS.MODEL_SCENARIO_ID, mdlScn.getId()),
      (SOLR_FIELDS.MODEL_SCENARIO_URL, mdlScn.metadataUrl),
      (SOLR_FIELDS.MODEL_SCENARIO_GCM, mdlScn.gcmCode),
      (SOLR_FIELDS.MODEL_SCENARIO_DATE_CODE, mdlScn.dateCode),
      (SOLR_FIELDS.MODEL_SCENARIO_ALT_PRED_CODE, mdlScn.altpredCode),
      (SOLR_FIELDS.PROJ_SCENARIO_CODE, prjScn.code),
      (SOLR_FIELDS.PROJ_SCENARIO_ID, prjScn.getId()),
      (SOLR_FIELDS.PROJ_SCENARIO_URL, prjScn.metadataUrl),
      (SOLR_FIELDS.PROJ_SCENARIO_GCM, prjScn.gcmCode),
      (SOLR_FIELDS.PROJ_SCENARIO_DATE_CODE, prjScn.dateCode),
      (SOLR_FIELDS.PROJ_SCENARIO_ALT_PRED_CODE, prjScn.altpredCode),
      (SOLR_FIELDS.PROJ_ID, prj.getId()),
      (SOLR_FIELDS.PROJ_META_URL, prj.metadataUrl),
      (SOLR_FIELDS.PROJ_DATA_URL, prj.getDataUrl()),
      (SOLR_FIELDS.PROJ_MOD_TIME, prj.modTime),
      (SOLR_FIELDS.PAV_META_URL, pav.metadataUrl),
      #(SOLR_FIELDS.PAV_DATA_URL, pav.getDataUrl()),
      (SOLR_FIELDS.EPSG_CODE, prj.epsgcode),
      (SOLR_FIELDS.GRIDSET_META_URL, pam.gridsetUrl),
      (SOLR_FIELDS.GRIDSET_ID, pam.gridsetId),
      (SOLR_FIELDS.SHAPEGRID_ID, sg.getId()),
      (SOLR_FIELDS.SHAPEGRID_META_URL, sg.metadataUrl),
      (SOLR_FIELDS.SHAPEGRID_DATA_URL, sg.getDataUrl()),
      # Compress the PAV and store the string
      (SOLR_FIELDS.COMPRESSED_PAV, compress(pavMtx.data))
   ]

   docLines = ['   <doc>']
       
   for fName, val in fields:
      if val is not None:
         docLines.append('      <field name="{}">{}</field>'.format(fName, val))
   
   # Process presence centroids
   rowHeaders = pavMtx.getRowHeaders()
   
   for i in xrange(pavMtx.data.shape[0]):
      if pavMtx.data[i]:
         _, x, y = rowHeaders[i]
         docLines.append('      <field name="{}">{},{}</field>'.format(
            SOLR_FIELDS.PRESENCE, y, x))
   
   docLines.append('   </doc>\n')
   
   doc = '\n'.join(docLines)

   return doc

# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
      prog='Lifemapper Solr index POST for Presence Absence Vectors',
      description='This script adds a PAV to the Lifemapper Solr index',
      version='1.0')
   
   parser.add_argument('pavFilename', type=str, help='The PAV file to use')
   parser.add_argument('pavId', type=int, help='The matrix column id')
   parser.add_argument('projectionId', type=int, help='The projection id')
   parser.add_argument('pamId', type=int, help='The PAM id')
   parser.add_argument('pavIdxFilename', type=str, 
                       help='A temporary file to be used for Solr POST')
   
   args = parser.parse_args()
   
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   pav = scribe.getMatrixColumn(mtxcolId=args.pavId)
   prj = scribe.getSDMProject(args.projectionId)
   occ = prj.occurrenceSet
   pam = scribe.getMatrix(mtxId=args.pamId)
   
   # Get all information for POST
   doc = getPostDocument(pav, prj, occ, pam, args.pavFilename)
   
   with open(args.pavIdxFilename, 'w') as outF:
      outF.write('<add>\n')
      outF.write(doc)
      outF.write('</add>')

   cmd = '{cmd} -c {collection} -out no {filename}'.format(
               cmd=SOLR_POST_COMMAND, collection=SOLR_ARCHIVE_COLLECTION, 
               filename=args.pavIdxFilename)
   subprocess.call(cmd, shell=True)
   
   scribe.closeConnections()
   
   
