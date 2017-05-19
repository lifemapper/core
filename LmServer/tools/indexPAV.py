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

# TODO: Different logger
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
def getPostDocument(pav, prj, occ):
   """
   @summary: Create the Solr document to be posted
   @return: A string that can be posted to the Solr index
   """
   fields = [
      ('id', pav.getId()),
      ('userId', pav.getUserId()),
      ('displayName', occ.displayName),
      ('squid', squid),
      ('taxonKingdom', taxKingdom),
      ('taxonPhylum', taxPhylum),
      ('taxonClass', taxClass),
      ('taxonOrder', taxOrder),
      ('taxonFamily', taxFamily),
      ('taxonGenus', taxGenus),
      ('taxonSpecies', taxSpecies),
      ('algorithmCode', algoCode),
      ('algorithmParameters', algoParams),
      ('pointCount', occ.queryCount),
      ('occurrenceId', occ.getId()),
      ('occurrenceDataUrl', occDataUrl),
      ('occurrenceMetaUrl', occMetaUrl),
      ('occurrenceModTime', occModTime),
      ('modelScenarioCode', mdlScnCode),
      ('modelScenarioId', mdlScnId),
      ('modelScenarioUrl', ),
      ('modelScenarioGCM', ),
      ('modelScenarioDateCode', ),
      ('modelScenarioAltPredCode', ),
      ('sdmProjScenarioCode', ),
      ('sdmProjScenarioId', ),
      ('sdmProjScenarioUrl', ),
      ('sdmProjScenarioGCM', ),
      ('sdmProjScenarioDateCode', ),
      ('sdmProjScenarioAltPredCode', ),
      ('sdmProjId', ),
      ('sdmProjMetaUrl', ),
      ('sdmProjDataUrl', ),
      ('sdmProjModTime', ),
      ('pavMetaUrl', ),
      ('pavDataUrl', ),
      ('epsgCode', ),
      ('gridSetMetaUrl', ),
      ('shapegridId', ),
      ('shapegridMetaUrl', ),
      ('shapegridDataUrl', )
   ]

   docLines = ['<doc>']
       
   # Need to process presence centroids
   'presence'
   
   for fName, val in fields:
      if val is not None:
         docLines.append('   <field name="{}">{}</field>'.format(fName, val))
   
   # TODO: Process presence centroids
   # for x in y
   #    docLines.append('   <field name="presence">{}</field>')
   
   docLines.append('</doc>')
   
   doc = '\n'.join(docLines)

   return doc


# .............................................................................
if __name__ == '__main__':
   parser = argparse.ArgumentParser(
      prog='Lifemapper Solr index POST for Presence Absence Vectors',
      description='This script adds a PAV to the Lifemapper Solr index',
      version='1.0')
   
   parser.add_argument('pavId', type=int, help='The matrix column id')
   parser.add_argument('pavFilename', type=str, help='The PAV file to use')
   
   args = parser.parse_args()
   
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   # TODO: Get all information for POST
   
   
   scribe.closeConnections()
   
   