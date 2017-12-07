"""
@summary: Add a tree and biogeographic hypotheses to a grid set


@todo: How to specify multiple hypotheses with different event fields?

"""
import argparse
import mx.DateTime
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
from LmServer.base.serviceobject2 import ServiceObject

# .................................
def encodeHypothesesToMatrix(scribe, usr, shapegrid, bgMtx, layers=[]):
   """
   @summary: Example code for encoding hypotheses to a BioGeo matrix
   @param bgMtx: The bio geographic hypotheses matrix you created
   @param layers: A list of (layer object, event field) tuples.  Event field
                     may be None
   """
   # Create the encoding data
   enc = None
   mtxCols = []
   
   for lyr in layers:
      lyrEnc = BioGeoEncoding(shapegrid.getDLocation())
      lyrEnc.addLayers(lyr.getDLocation(), lyr.valAttribute)
      encMtx = lyrEnc.encodeHypotheses()
      
      # Add matrix columns for the newly encoded layers
      for col in encMtx.getColumnHeaders():
         # TODO: Fill in params and metadata
         efValue = col.split(' - ')[1]
         if lyr.valAttribute is not None:
            intParams = {MatrixColumn.INTERSECT_PARAM_VAL_NAME: lyr.valAttribute,
                         MatrixColumn.INTERSECT_PARAM_VAL_VALUE: efValue}
         metadata = {
            ServiceObject.META_DESCRIPTION : 
         'Encoded Helmert contrasts using the Lifemapper bioGeoContrasts module',
            ServiceObject.META_TITLE : 
         'Biogeographic hypothesis column ({})'.format(col)}
         mc = MatrixColumn(len(mtxCols), bgMtx.getId(), usr, layer=lyr,
                           shapegrid=shapegrid, intersectParams=intParams, 
                           metadata=metadata, postToSolr=False,
                           status=JobStatus.COMPLETE, 
                           statusModTime=mx.DateTime.gmt().mjd)
         updatedMC = scribe.findOrInsertMatrixColumn(mc)
         mtxCols.append(updatedMC)
      
      # Concatenate with the existing encoded matrix
      if enc is None:
         enc = encMtx
      else:
         enc = Matrix.concatenate([enc, encMtx], axis=1)
      
   # Update biogeo matrix with new data and headers
   bgMtx.data = enc.data
   bgMtx.setHeaders(enc.getHeaders())
   
   # Save matrix and update record
   bgMtx.clearDLocation()
   bgMtx.setDLocation()
   bgMtx.write()
   bgMtx.updateStatus(JobStatus.COMPLETE, modTime=mx.DateTime.gmt().mjd)
   success = scribe.updateObject(bgMtx)
   return bgMtx

# .................................
def squidifyTree(scribe, usr, tree):
   """
   @summary: Annotate a tree with squids and node ids, then write to disk
   @note: Matching species must be present in the taxon table of the database
   """
   squidDict = {}
   for label in tree.getLabels():
      # TODO: Do we always need to do this?
      taxLabel = label.replace(' ', '_')
      sno = scribe.getTaxon(userId=usr, taxonName=taxLabel)
      if sno is not None:
         squidDict[label] = sno.squid

   tree.annotateTree(PhyloTreeKeys.SQUID, squidDict)

   print "Adding interior node labels to tree"
   # Add node labels
   tree.addNodeLabels()
   
   # Update tree properties
   tree.clearDLocation()
   tree.setDLocation()
   print "Write tree to final location"
   tree.writeTree()
   tree.updateModtime(mx.DateTime.gmt().mjd)
   success = scribe.updateObject(tree)
   return tree


# .............................................................................
if __name__ == '__main__':
   if not isCorrectUser():
      print("Run this script as 'lmwriter'")
      sys.exit(2)

   parser = argparse.ArgumentParser(
      description="Annotate a tree with squids and node ids")

   parser.add_argument('-u', '--user', type=str, 
                       help="User name")
   parser.add_argument('-t', '--tree_name', type=str, 
                       help="Tree name to annotate")
   
   args = parser.parse_args()
   usr = args.user
   treename = args.tree_name

   scribe = BorgScribe(ConsoleLogger())
   scribe.openConnections()
   
   baretree = Tree(treename, userId=args.user)
   
   tree = scribe.getTree(tree=baretree)
   
   decoratedtree = squidifyTree(scribe, usr, tree)
   
   scribe.closeConnections()
   
   
   
