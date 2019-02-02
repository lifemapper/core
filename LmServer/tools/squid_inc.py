"""This script adds SQUIDs to the tips of a tree
"""
import argparse

from LmCommon.common.lmconstants import DEFAULT_TREE_SCHEMA, PhyloTreeKeys
from LmCommon.trees.lmTree import LmTree

from LmServer.common.log import ScriptLogger
from LmServer.db.borgscribe import BorgScribe
from LmCommon.common.readyfile import readyFilename

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
    tree = LmTree.initFromFile(args.treeFile, DEFAULT_TREE_SCHEMA)
   
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
    readyFilename(args.outTreeFile)
    tree.writeTree(args.outTreeFile)
   
