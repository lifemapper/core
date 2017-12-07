"""
@summary: Add a tree and biogeographic hypotheses to a grid set


@todo: How to specify multiple hypotheses with different event fields?

"""
import argparse
import numpy as np
import os
import sys

from LmCommon.common.lmconstants import (JobStatus, MatrixType, PhyloTreeKeys,
                                         ProcessType)
from LmCommon.common.matrix import Matrix
from LmCommon.encoding.bioGeoContrasts import BioGeoEncoding
from LmServer.base.utilities import isCorrectUser
from LmServer.common.log import ConsoleLogger
from LmServer.db.borgscribe import BorgScribe
from LmServer.legion.lmmatrix import LMMatrix
from LmServer.legion.tree import Tree
from LmServer.legion.mtxcolumn import MatrixColumn

def addHypotheses(gs, bgMtx, layers=[]):
   """
   @summary: Example code for adding hypotheses to BioGeo matrix
   @param gs: A grid set you have created
   @param bgMtx: The bio geographic hypotheses matrix you created
   @param layers: A list of (layer object, event field) tuples.  Event field
                     may be None
   """
   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   sg = gs.getShapegrid()
   
   # Create the encoding data
   enc = None
   mtxCols = []
   
   for layer, eventField in layers:
      lyrEnc = BioGeoEncoding(sg.getDLocation())
      lyrEnc.addLayers(layer.getDLocation(), eventField)
      encMtx = lyrEnc.encodeHypotheses()
      
      # Add matrix columns for the newly encoded layers
      for col in encMtx.getColumnHeaders():
         # TODO: Fill in params and metadata
         # TODO: Create a better way to get the event field value
         efValue = col.split(' - ')[1]
         if eventField is not None:
            intParams = {MatrixColumn.INTERSECT_PARAM_VAL_NAME: eventField,
                         MatrixColumn.INTERSECT_PARAM_VAL_VALUE: efValue}
         metadata = {}
         mc = MatrixColumn(len(mtxCols), bgMtx.getId(), gs.getUserId(), layer=layer,
                           shapegrid=sg, intersectParams=intParams, 
                           metadata=metadata, postToSolr=False)
         mtxCols.append(mc)
      
      # Concatenate with the existing encoded matrix
      if enc is None:
         enc = encMtx
      else:
         enc = Matrix.concatenate([enc, encMtx], axis=1)
      
   # Update biogeo matrix with new data and headers
   bgMtx.data = enc.data
   bgMtx.setHeaders(enc.getHeaders())
   
   # Save matrix
   bgMtx.write()

   # Do any necessary database updates.  BG mtx mod time / status, mtx cols

# .................................
def addTree(gs, treeFilename, scribe):
   """
   @summary: This is an example method for adding a tree to a gridset
   @note: This code adds squids and internal node ids to the tree before writing
   @note: Taxon table must be filled in before running this
   """
   
   userId = gs.getUserId()
   
   treeName = os.path.splitext(os.path.basename(treeFilename))[0]
   t = Tree(treeName, dlocation=treeFilename, userId=userId)
   
   squidDict = {}
   for label in t.getLabels():
      # TODO: Do we always need to do this?
      taxLabel = label.replace(' ', '_')
      sno = scribe.getTaxon(userId=userId, taxonName=taxLabel)
      if sno is not None:
         squidDict[label] = sno.squid

   t.annotateTree(PhyloTreeKeys.SQUID, squidDict)

   print "Adding interior node labels to tree"
   # Add node labels
   t.addNodeLabels()
   
   print "Inserting tree"
   insertedTree = scribe.findOrInsertTree(t)
   insertedTree.clearDLocation()
   insertedTree.updateModtime()
   insertedTree.tree = t.tree
   scribe.updateObject(insertedTree)
   print "Write tree to final location"
   insertedTree.writeTree()

   # Need to add tree to grid set
   print "Add tree to grid set"
   gs.tree = insertedTree
   gs.updateModtime()
   scribe.updateObject(gs)


# .............................................................................
def addToGridset(gridsetId, treeFilename=None, hypotheses=None, 
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
      print "Encoding layers"
      encMtx = enc.encodeHypotheses()
      print "Layers encoded"
      
      # Check for an existing matrix
      mtx = scribe.getMatrix(userId=gs.getUserId(), gridsetId=gs.getId(),
                             mtxType=MatrixType.BIOGEO_HYPOTHESES)
      if mtx is None:
         # Insert matrix into db
         newMtx = LMMatrix(encMtx.data, headers=encMtx.getHeaders(), 
                        matrixType=MatrixType.BIOGEO_HYPOTHESES, 
                        processType=ProcessType.ENCODE_HYPOTHESES,
                        userId=gs.getUserId(), gridset=gs)
         mtx = scribe.findOrInsertMatrix(newMtx)
         mtx.data = encMtx.data
         mtx.updateStatus(JobStatus.COMPLETE)
         scribe.updateObject(mtx)
      else:
         mtx.data = np.append(mtx.data, encMtx.data, axis=1)
         colHeaders = mtx.getColumnHeaders()
         colHeaders.extend(encMtx.getColumnHeaders())
         mtx.setColumnHeaders(colHeaders)
      # Write the encoded matrix to the new matrix dlocation
      print "Saving encoded matrix to:", mtx.getDLocation()
      with open(mtx.getDLocation(), 'w') as outF:
         mtx.save(outF)
      print "Saved"

   # If a tree was provided
   if treeFilename:
      treeName = os.path.splitext(os.path.basename(treeFilename))[0]
      t = Tree(treeName, dlocation=treeFilename, userId=gs.getUserId())

      # Add squids
      userId = gs.getUserId()
      
      print "Adding squids to tree"
      squidDict = {}
      for label in t.getLabels():
         # TODO: Do we always need to do this?
         taxLabel = label.replace(' ', '_')
         sno = scribe.getTaxon(userId=userId, taxonName=taxLabel)
         if sno is not None:
            squidDict[label] = sno.squid

      t.annotateTree(PhyloTreeKeys.SQUID, squidDict)
   
      print "Adding interior node labels to tree"
      # Add node labels
      t.addNodeLabels()
      
      print "Inserting tree"
      insertedTree = scribe.findOrInsertTree(t)
      insertedTree.clearDLocation()
      insertedTree.updateModtime()
      insertedTree.tree = t.tree
      scribe.updateObject(insertedTree)
      print "Write tree to final location"
      insertedTree.writeTree()

      # Need to add tree to grid set
      print "Add tree to grid set"
      gs.tree = insertedTree
      gs.updateModtime()
      scribe.updateObject(gs)
   
   print "Done"
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
                hypotheses=hypotheses, eventField=args.event_field)
   
