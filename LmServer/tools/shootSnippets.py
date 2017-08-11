"""
@summary: This script produces and shoots snippets
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
@todo: Expand to use more than occurrence sets for the initial object
"""
import argparse

from LmServer.common.log import ConsoleLogger
from LmServer.common.lmconstants import SnippetOperations, SnippetShooter
from LmServer.db.borgscribe import BorgScribe


# .............................................................................
if __name__ == '__main__':
   
   parser = argparse.ArgumentParser(
                        description='This script produces and shoots snippets')
   parser.add_argument('occurrenceId', type=int, 
                   help='The id of the occurrence set to produce snippets for')
   parser.add_argument('operation', type=str, help='The operation performed',
                       choices=[SnippetOperations.ADDED_TO,
                                SnippetOperations.DOWNLOADED,
                                SnippetOperations.USED_IN,
                                SnippetOperations.VIEWED])
   parser.add_argument('postFilename', type=str, 
                          help='A file location to write out the snippet post')
   
   # Optional arguments
   parser.add_argument('-o2ident', type=str, dest='obj2ident', 
          help='Identifier for an optional target object (projection perhaps)')
   parser.add_argument('-url', type=str, dest='url',
                                      help='A URL associated with this action')
   parser.add_argument('-who', type=str, dest='who',
                                              help='Who initiated this action')
   parser.add_argument('-agent', type=str, dest='agent',
                                 help='The agent used to initiate this action')
   parser.add_argument('-why', type=str, dest='why', 
                                          help='Why was this action initiated')
   
   
   #obj1, operation, opTime=None, obj2ident=None, 
   #                url=None, who=None, agent=None, why=None
                   
   args = parser.parse_args()
   
   shooter = SnippetShooter()
   
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   occ = scribe.getOccurrenceSet(args.occurrenceId)
   occ.readData(doReadData=True)
   scribe.closeConnections()
   
   # If we provide a second object identifer, only use the url argument
   # If we don't have a second object, fallback to the occ url if no url 
   #    argument
   url = args.url

   if args.obj2ident is not None:
      modTime = None # Will get current time
   else:
      modTime = occ.modTime
      if url is None:
         url = occ.metadataUrl
   
   # Add snippets to shooter   
   shooter.addSnippets(occ, args.operation, opTime=modTime, 
                       obj2ident=args.obj2ident, url=url, who=args.who, 
                       agent=args.agent, why=args.why)
   # Shoot snippets
   shooter.shootSnippets(solrPostFilename=args.postFilename)
   