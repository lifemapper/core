"""
@summary: This script adds SQUIDs to the tips of a tree
@author: CJ Grady
@version: 4.0.0
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
import argparse

from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA, PhyloTreeKeys
from LmCommon.trees.lmTree import LmTree

from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe

# .............................................................................
if __name__ == "__main__":
   # Set up the argument parser
   parser = argparse.ArgumentParser(
      description="This script adds SQUIDs to the tips of a tree")
   
   parser.add_argument("treeFile", type=str, help="File location of LM tree")
   parser.add_argument("userName", type=str, 
                       help="The user this tree belongs to")
   parser.add_argument('outTreeFile', type=str, 
               help="Write the modified tree to this file location")
   
   
   args = parser.parse_args()
   
   # Load tree
   userId = args.userName
   tree = LmTree(args.treeFile, DEFAULT_TREE_SCHEMA)
   
   # Do stuff
   scribe = BorgScribe(ScriptLogger('squidInc'))
   scribe.openConnections()
   
   squidDict = {}
   
   for label in tree.getLabels():
      # TODO: Do we always need to do this?
      taxLabel = label.replace(' ', '_')
      sno = scribe.getTaxon(userId=userId, taxonName=taxLabel)
      if sno is not None:
         squidDict[label] = sno.squid
   
   tree.annotateTree(PhyloTreeKeys.SQUID, squidDict)
   
   scribe.closeConnections()
   
   # Write tree
   tree.writeTree(args.outTreeFile)
   
