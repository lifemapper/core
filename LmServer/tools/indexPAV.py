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
import argparse
import os
import subprocess
import tempfile

# TODO: Different logger
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmCommon.common.matrix import Matrix

SOLR_POST_COMMAND = '/opt/solr/bin/post'
COLLECTION = 'lmArchive'

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
   
   fields = [
      ('id', pav.getId()),
      ('userId', pav.getUserId()),
      ('displayName', occ.displayName),
      ('squid', pav.squid),
      #('taxonKingdom', taxKingdom),
      #('taxonPhylum', taxPhylum),
      #('taxonClass', taxClass),
      #('taxonOrder', taxOrder),
      #('taxonFamily', taxFamily),
      #('taxonGenus', taxGenus),
      #('taxonSpecies', taxSpecies),
      ('algorithmCode', prj.algorithmCode),
      ('algorithmParameters', prj.dumpAlgorithmParametersAsString()),
      ('pointCount', occ.queryCount),
      ('occurrenceId', occ.getId()),
      ('occurrenceDataUrl', occ.getDataUrl()),
      ('occurrenceMetaUrl', occ.metadataUrl),
      ('occurrenceModTime', occ.modTime), # May need to convert
      ('modelScenarioCode', mdlScn.code),
      ('modelScenarioId', mdlScn.getId()),
      ('modelScenarioUrl', mdlScn.metadataUrl),
      ('modelScenarioGCM', mdlScn.gcmCode),
      ('modelScenarioDateCode', mdlScn.dateCode),
      ('modelScenarioAltPredCode', mdlScn.altpredCode),
      ('sdmProjScenarioCode', prjScn.code),
      ('sdmProjScenarioId', prjScn.getId()),
      ('sdmProjScenarioUrl', prjScn.metadataUrl),
      ('sdmProjScenarioGCM', prjScn.gcmCode),
      ('sdmProjScenarioDateCode', prjScn.dateCode),
      ('sdmProjScenarioAltPredCode', prjScn.altpredCode),
      ('sdmProjId', prj.getId()),
      ('sdmProjMetaUrl', prj.metadataUrl),
      ('sdmProjDataUrl', prj.getDataUrl()),
      ('sdmProjModTime', prj.modTime),
      ('pavMetaUrl', pav.metadataUrl),
      #('pavDataUrl', pav.getDataUrl()),
      ('epsgCode', prj.epsgcode),
      ('gridSetMetaUrl', pam.gridsetUrl),
      ('gridSetId', pam.gridsetId),
      ('shapegridId', sg.getId()),
      ('shapegridMetaUrl', sg.metadataUrl),
      ('shapegridDataUrl', sg.getDataUrl())
   ]

   docLines = ['   <doc>']
       
   for fName, val in fields:
      if val is not None:
         docLines.append('      <field name="{}">{}</field>'.format(fName, val))
   
   # Process presence centroids
   pavMtx = Matrix.load(pavFname)
   rowHeaders = pavMtx.getRowHeaders()
   
   for i in xrange(pavMtx.data.shape[0]):
      if pavMtx.data[i]:
         _, x, y = rowHeaders[i]
         docLines.append('      <field name="presence">{},{}</field>'.format(y, x))
   
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
               cmd=SOLR_POST_COMMAND, collection=COLLECTION, 
               filename=args.pavIdxFilename)
   subprocess.call(cmd, shell=True)
   
   scribe.closeConnections()
   
   
