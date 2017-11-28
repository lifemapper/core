"""
@summary: Add a tree and biogeographic hypotheses to a grid set
"""
import argparse
import os
import sys

from LmCommon.common.lmconstants import (JobStatus, MatrixType, PhyloTreeKeys,
                                         ProcessType)
from LmCommon.encoding.bioGeoContrasts import BioGeoEncoding
from LmServer.base.utilities import isCorrectUser
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.tree import Tree

# .............................................................................
def addToGridset(gridsetId, treeFilename=None, treeName=None, hypotheses=None, 
                 eventField=None):
   """
   @summary: Adds a tree and biogeographic hypotheses to a gridset 
   """
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   gs = scribe.getGridset(gridsetId=gridsetId)
   sg = gs.getShapegrid()
   
   # If hypotheses were provided
   if hypotheses:
      enc = BioGeoEncoding(sg.getDLocation())
      enc.addLayers(hypotheses, eventField=eventField)
      encMtx = enc.encodeHypotheses()
      
      # Insert matrix into db
      newMtx = LMMatrix(encMtx.data, headers=encMtx.getHeaders(), 
                        matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                        processType=ProcessType.ENCODE_HYPOTHESES,
                        userId=gs.getUserId(), gridset=gs)
      insertedMtx = scribe.findOrInsertMatrix(newMtx)
      insertedMtx.updateStatus(JobStatus.COMPLETE)
      scribe.updateObject(insertedMtx)
      # Write the encoded matrix to the new matrix dlocation
      with open(insertedMtx.getDLocation(), 'w') as outF:
         encMtx.save(outF)

   # If a tree was provided
   if treeFilename and treeName:
      t = Tree(treeName, dlocation=treeFilename, userId=gs.getUserId())

      # Add squids
      userId = gs.getUserId()
      
      squidDict = {}
      for label in t.getLabels():
         # TODO: Do we always need to do this?
         taxLabel = label.replace(' ', '_')
         sno = scribe.getTaxon(userId=userId, taxonName=taxLabel)
         if sno is not None:
            squidDict[label] = sno.squid

      t.annotateTree(PhyloTreeKeys.SQUID, squidDict)
   
      # Add node labels
      t.addNodeLabels()
      
      insertedTree = scribe.findOrInsertTree(t)
      insertedTree.clearDLocation()
      insertedTree.updateModtime()
      insertedTree.tree = t.tree
      scribe.updateObject(insertedTree)
      insertedTree.writeTree()

      # Need to add tree to grid set
      gs.tree = insertedTree
      gs.updateModtime()
      scribe.updateObject(gs)
   
   scribe.closeConnections()

# .............................................................................
if __name__ == '__main__':
   if not isCorrectUser():
      print("Run this script as 'lmwriter'")
      sys.exit(2)

   parser = argparse.ArgumentParser(
      description="Add a tree and hypotheses to a gridset")

   parser.add_argument('-t', '--tree_filename', type=str, 
                       help="File path to a json tree file to add")
   parser.add_argument('-tn', '--tree_name', type=str, 
                       help='The name of the new tree')
   parser.add_argument('-e', '--event_field', type=str, 
               help='The name of the event field in the hypotheses shapefiles')
   parser.add_argument('gridsetId', type=int, help='The gridset id to use')
   parser.add_argument('hypothesis', type=str, nargs='*', 
                       help='File path to a hypothesis shapefile')
   
   args = parser.parse_args()
   
   hypotheses = []
   for fn in args.hypothesis:
      if os.path.exists(fn):
         hypotheses.append(fn)
      else:
         print('{} does not exist'.format(fn))
   
   addToGridset(args.gridsetId, treeFilename=args.tree_filename, 
                treeName=args.tree_name, hypotheses=hypotheses, 
                eventField=args.event_field)
   
