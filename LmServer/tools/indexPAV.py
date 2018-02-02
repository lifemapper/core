"""
@summary: This script inserts a PAV into the Solr index
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
import argparse
from mx.DateTime import DateTimeFromMJD

# TODO: Different logger
from LmCommon.common.matrix import Matrix
from LmCommon.compression.binaryList import compress
from LmServer.common.lmconstants import (SOLR_ARCHIVE_COLLECTION, SOLR_FIELDS)
from LmServer.common.log import ConsoleLogger
from LmServer.common.solr import buildSolrDocument, postSolrDocument
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
def getPostDocument(pav, prj, occ, pam, sciName, pavFname):
   """
   @summary: Create the Solr document to be posted
   @param pav: A PAV matrix column object
   @param prj: A SDM Projection object
   @param occ: An occurrence layer object
   @return: A string that can be posted to the Solr index
   """
   sg = pam.getShapegrid()
   mdlScn = prj.modelScenario
   prjScn = prj.projScenario
   pavMtx = Matrix.load(pavFname)
   try:
      sp = sciName.scientificName.split(' ')[1]
   except:
      sp = None
   
   # Mod times
   occModTime = prjModTime = None
   
   if occ.modTime is not None:
      occModTime = DateTimeFromMJD(occ.modTime).strftime('%Y-%m-%dT%H:%M:%SZ')
   
   if prj.modTime is not None:
      prjModTime = DateTimeFromMJD(prj.modTime).strftime('%Y-%m-%dT%H:%M:%SZ')

   # Taxonomy fields
   txKingdom = None
   txPhylum = None
   txClass = None
   txOrder = None
   txFamily = None
   txGenus = None
   
   try:
      txKingdom = sciName.kingdom
      txPhylum = sciName.phylum
      txClass = sciName.txClass
      txOrder = sciName.txOrder
      txFamily = sciName.family
      txGenus = sciName.genus
   except:
      pass
   
   
   fields = [
      (SOLR_FIELDS.ID, pav.getId()),
      (SOLR_FIELDS.USER_ID, pav.getUserId()),
      (SOLR_FIELDS.DISPLAY_NAME, occ.displayName),
      (SOLR_FIELDS.SQUID, pav.squid),
      (SOLR_FIELDS.TAXON_KINGDOM, txKingdom),
      (SOLR_FIELDS.TAXON_PHYLUM, txPhylum),
      (SOLR_FIELDS.TAXON_CLASS, txClass),
      (SOLR_FIELDS.TAXON_ORDER, txOrder),
      (SOLR_FIELDS.TAXON_FAMILY, txFamily),
      (SOLR_FIELDS.TAXON_GENUS, txGenus),
      (SOLR_FIELDS.TAXON_SPECIES, sp),
      (SOLR_FIELDS.ALGORITHM_CODE, prj.algorithmCode),
      (SOLR_FIELDS.ALGORITHM_PARAMETERS, prj.dumpAlgorithmParametersAsString()),
      (SOLR_FIELDS.POINT_COUNT, occ.queryCount),
      (SOLR_FIELDS.OCCURRENCE_ID, occ.getId()),
      (SOLR_FIELDS.OCCURRENCE_DATA_URL, occ.getDataUrl()),
      (SOLR_FIELDS.OCCURRENCE_META_URL, occ.metadataUrl),
      (SOLR_FIELDS.OCCURRENCE_MOD_TIME, occModTime),
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
      (SOLR_FIELDS.PROJ_MOD_TIME, prjModTime),
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

   # Process presence centroids
   rowHeaders = pavMtx.getRowHeaders()
   
   for i in xrange(pavMtx.data.shape[0]):
      if pavMtx.data[i]:
         _, x, y = rowHeaders[i]
         fields.append((SOLR_FIELDS.PRESENCE, '{},{}'.format(y, x)))

   doc = buildSolrDocument(fields)

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
   sciName = scribe.getTaxon(squid=pav.squid)
   
   # Get all information for POST
   doc = getPostDocument(pav, prj, occ, pam, sciName, args.pavFilename)
   
   with open(args.pavIdxFilename, 'w') as outF:
      outF.write(doc)

   postSolrDocument(SOLR_ARCHIVE_COLLECTION, args.pavIdxFilename)

   scribe.closeConnections()
   
   
